# This your assignment report


## Part 1 - Batch data ingestion

1. The ingestion will be applied to files of data. Design a set of constraints for files that **mysimbdp** will support for ingestion.
Design a set of constraints for the tenant service profile w.r.t. ingestion (e.g., maximum number of files and amount of data).
Explain why you as a platform provider decide such constraints. Implement these constraints into simple configuration files
and provide examples (e.g., JSON or YAML).

We need such constraints for each tenant service profie to control the amount of data ingested to the platform from each tenant.
We'll need to scale down one tenant's throughput if we have too many tenants in the platform. In the current platform, each tenant's 
directory (e.g `code/tenants/gift_card`) contains `clientbatchingestapp.cfg` file which is the configurations file for the `clientingestapp`,
sample:

```
[INGEST]
file_num=2                        # Max Number of files in a batch
file_size_mb=100                  # File size limit in MB
file_extension=tsv                # File extension to include in batch

[CASSANDRA]                       # Cassandra related configs
host=localhost
username=k8ssandra-superuser
password=L8AqSx3E7kZofZ00Pash
keyspace=mysimbdp
```


2. Each tenant will put the tenant's data files to be ingested into a directory, **client-staging-input-directory** within **mysimbdp**.
Each tenant provides ingestion programs/pipelines, **clientbatchingestapp**, which will take the tenant's files as input, in
**client-staging-input-directory**, and ingest the files into **mysimbdp-coredms**. Any **clientbatchingestapp** must perform at
least one type of data wrangling. As a tenant, explain the design of **clientbatchingestapp** and provide one implementation.
Note that **clientbatchingestapp** follows the guideline of **mysimbdp** given in the next Point 3.

Each tenant will have its directory at `code/tenants/<TENANT_NAME>`, e.g `code/tenants/gift_card/`, the directory contains:

- `clientbatchingestapp.cfg`: configuration of the `clientbatchingestapp`
- `clientbatchingestapp.py`: `clientbatchingestapp` ingestion program
- `staging`: staging data directory

New coming tenants need to give only those 3 components.


3. As the **mysimbdp** provider, design and implement a component **mysimbdp-batchingestmanager** that invokes tenant's
**clientbatchingestapp** to perform the ingestion for available files in **client-staging-input-directory**. **mysimbdp** imposes the
model that **clientbatchingestapp** has to follow but **clientbatchingestapp** is, in principle, a blackbox to **mysimbdpbatchingestmanager**.
Explain how **mysimbdp-batchingestmanager** decides/schedules the execution of **clientbatchingestapp** for tenants. 


**mysimbdp-batchingestmanager** is located at `code/mysimbdp-batchingestmanager.py`, this program essentially registers tenants for batch ingestion.
Registration of new tenant happens as follow: create new directory under `code/tenants` with new tenant's name, e.g: `code/tenants/toys`. In that directory,
give tenant's configuration in `clientbatchingestapp.cfg`, its data in `staging` and data wrangling function under `clientbatchingestapp.app`. Then in the file
`code/mysimbdp-batchingestmanager.cfg`, add tenant's name into the list. After these 2 steps, the tenant is registered for batch ingestion. Under the hood,
create a `Tenant` object using the components defined in the tenant's directory. **mysimbdp-batchingestmanager** is then able to call the `Tenant` object's
`batch_ingest` method to start the batch ingestion, it does this one tenant at a time until all of registered tenants have finished ingestion.


`mysimbdp-batchingestmanager.cfg` content:
```
[DEFAULT]
tenants=[
    "gift_card",
    "toy"
  ]
```


4. Explain your design for the multi-tenancy model in mysimbdp: which parts of mysimbdp will be shared for all tenants,
which parts will be dedicated for individual tenants so that you as a platform provider can add and remove tenants based on
the principle of pay-per-use. Develop test programs (clientbatchingestapp), test data, and test constraints of files, and test
service profiles for tenants according your deployment. Show the performance of ingestion tests, including failures and
exceptions, for at least 2 different tenants in your test environment and constraints. What is the maximum amount of data
per second you can ingest in your tests?


General tenant structure: In this platform, I assume that the tenants will mostly share the same data, the data is reviews data,
each tenant will focus on a specific type of products, e.g gift cards, digital video games... Therefore, I decide the abstract the tenant
into a generic class at `code/tenant.py`. This class dictates how tenants are supposed to read and use their configurations, do logging, 
split staging data files into batches, read data and write data in batch. The main difference of tenants is in how they do data wrangling on
the staging data. This data wrangling is defined as a function on a Pandas Dataframe of staging data (`def process_batch(batch_df)`), located in the module 
`code/tenants/<TENANT>/clientbatchingingestapp.py` (e.g `code/tenants/gift_card/clientbatchingingestapp.py`). 

To unregister a tenant, simply remove its name from `code/mysimbdp-batchingestmanager.cfg`, or its directory.

Performance statistics from Grafana
![](/reports/images/performance.png)


5. Implement and provide logging features for capturing successful/failed ingestion as well as metrics about ingestion time,
data size, etc., for files which have been ingested into **mysimbdp**. Logging information must be stored in separate files,
databases or a monitoring system for analytics of ingestion. Show and explain simple statistical data extracted from logs for
individual tenants and for the whole platform with your tests.

Logging is implemented via the `logging` library of Python. Each tenant produces a `clientbatchngingestapp.log` file in the tenant directory
after the **mysimbdp-batchingestmanager** calls data ingestion for that tenant. This log outputs the number of batches, number of rows of data 
for each batch and time taken for ingesting of that batch.

Sample of tenant log:
```
2022-03-15 17:23:27,034 - gift_card - INFO - Start Batch Ingestion for: gift_card to Keyspace: mysimbdp
2022-03-15 17:23:27,043 - gift_card - INFO - Number of Batches: 1
2022-03-15 17:36:57,371 - gift_card - INFO - 	Batch 0: Ingested 148310 rows, took 810.3277261257172 seconds
```

Beside the tenants' logs, **mysimbdp-batchingestmanager** also produces logs available at `code/platform.log`, after execution of **mysimbdp-batchingestmanager**
has started.

Sample of platform log:
```
2022-03-15 17:23:26,711 - root - INFO - Tenants Present in the platform: ['gift_card', 'digital_video_games']
2022-03-15 17:23:26,711 - root - INFO - Tenants Registered in the platform: ['gift_card', 'digital_video_games']
2022-03-15 17:23:26,718 - cassandra.cluster - WARNING - Cluster.__init__ called with contact_points specified, but no load_balancing_policy. In the next major version, this will raise an error; please specify a load-balancing policy. (contact_points = ['localhost'], lbp = None)
2022-03-15 17:23:26,734 - cassandra.cluster - WARNING - Downgrading core protocol version from 66 to 65 for ::1:9042. To avoid this, it is best practice to explicitly set Cluster(protocol_version) to the version supported by your cluster. http://datastax.github.io/python-driver/api/cassandra/cluster.html#cassandra.cluster.Cluster.protocol_version
2022-03-15 17:23:26,752 - cassandra.cluster - WARNING - Downgrading core protocol version from 65 to 5 for ::1:9042. To avoid this, it is best practice to explicitly set Cluster(protocol_version) to the version supported by your cluster. http://datastax.github.io/python-driver/api/cassandra/cluster.html#cassandra.cluster.Cluster.protocol_version
2022-03-15 17:23:26,772 - cassandra.connection - ERROR - Closing connection <AsyncoreConnection(4552363984) ::1:9042> due to protocol error: Error from server: code=000a [Protocol error] message="Beta version of the protocol used (5/v5-beta), but USE_BETA flag is unset"
2022-03-15 17:23:26,773 - cassandra.cluster - WARNING - Downgrading core protocol version from 5 to 4 for ::1:9042. To avoid this, it is best practice to explicitly set Cluster(protocol_version) to the version supported by your cluster. http://datastax.github.io/python-driver/api/cassandra/cluster.html#cassandra.cluster.Cluster.protocol_version
2022-03-15 17:23:26,893 - cassandra.policies - INFO - Using datacenter 'dc1' for DCAwareRoundRobinPolicy (via host '::1:9042'); if incorrect, please specify a local_dc to the constructor, or limit contact points to local cluster nodes
2022-03-15 17:23:26,893 - cassandra.cluster - INFO - Cassandra host 127.0.0.1:9042 removed
2022-03-15 17:23:27,034 - gift_card - INFO - Start Batch Ingestion for: gift_card to Keyspace: mysimbdp
2022-03-15 17:23:27,043 - gift_card - INFO - Number of Batches: 1
2022-03-15 17:36:57,371 - gift_card - INFO - 	Batch 0: Ingested 148310 rows, took 810.3277261257172 seconds
2022-03-15 17:36:57,395 - cassandra.cluster - WARNING - Cluster.__init__ called with contact_points specified, but no load_balancing_policy. In the next major version, this will raise an error; please specify a load-balancing policy. (contact_points = ['localhost'], lbp = None)
2022-03-15 17:36:57,417 - cassandra.cluster - WARNING - Downgrading core protocol version from 66 to 65 for 127.0.0.1:9042. To avoid this, it is best practice to explicitly set Cluster(protocol_version) to the version supported by your cluster. http://datastax.github.io/python-driver/api/cassandra/cluster.html#cassandra.cluster.Cluster.protocol_version
2022-03-15 17:36:57,432 - cassandra.cluster - WARNING - Downgrading core protocol version from 65 to 5 for 127.0.0.1:9042. To avoid this, it is best practice to explicitly set Cluster(protocol_version) to the version supported by your cluster. http://datastax.github.io/python-driver/api/cassandra/cluster.html#cassandra.cluster.Cluster.protocol_version
2022-03-15 17:36:57,448 - cassandra.connection - ERROR - Closing connection <AsyncoreConnection(4606309280) 127.0.0.1:9042> due to protocol error: Error from server: code=000a [Protocol error] message="Beta version of the protocol used (5/v5-beta), but USE_BETA flag is unset"
2022-03-15 17:36:57,448 - cassandra.cluster - WARNING - Downgrading core protocol version from 5 to 4 for 127.0.0.1:9042. To avoid this, it is best practice to explicitly set Cluster(protocol_version) to the version supported by your cluster. http://datastax.github.io/python-driver/api/cassandra/cluster.html#cassandra.cluster.Cluster.protocol_version
2022-03-15 17:36:57,590 - cassandra.policies - INFO - Using datacenter 'dc1' for DCAwareRoundRobinPolicy (via host '127.0.0.1:9042'); if incorrect, please specify a local_dc to the constructor, or limit contact points to local cluster nodes
2022-03-15 17:36:57,590 - cassandra.cluster - INFO - Cassandra host ::1:9042 removed
2022-03-15 17:36:57,789 - digital_video_games - INFO - Start Batch Ingestion for: digital_video_games to Keyspace: mysimbdp
2022-03-15 17:36:57,797 - digital_video_games - INFO - Number of Batches: 1
```
