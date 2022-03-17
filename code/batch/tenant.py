import logging, time
from pathlib import Path

from tenant_utils import parse_ingest_config, parse_cassandra_config, get_cassandra_session, get_batches, read_batch_df, write_df_to_cassandra

class Tenant:

  def __init__(self, tenant_name) -> None:
    self.tenant_name = tenant_name
    self.__tenant_directory__ = f'./tenants/{tenant_name}'

    # Configure logger
    self.__logger__ = logging.getLogger(tenant_name)
    f_handler = logging.FileHandler(f'{self.__tenant_directory__}/clientbatchingestapp.log', mode="w")
    s_handler = logging.StreamHandler()

    f_handler.setLevel(logging.INFO)
    s_handler.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    f_handler.setFormatter(formatter)
    s_handler.setFormatter(formatter)
    
    self.__logger__.addHandler(f_handler)
    self.__logger__.addHandler(s_handler)

    tenant_config_path = Path(self.__tenant_directory__).joinpath("clientbatchingestapp.cfg")
    
    self.__tenant_staging_path__ = Path(self.__tenant_directory__).joinpath("staging")
    self.__ingest_config__ = parse_ingest_config(tenant_config_path)
    self.__cassandra_config__ = parse_cassandra_config(tenant_config_path)
    
    exec(f"from tenants.{self.tenant_name}.clientbatchingestapp import process_batch")
    exec("self.__process_batch_function__ = process_batch")


  def batch_ingest(self):
    session = get_cassandra_session(self.__cassandra_config__)
    self.__logger__.info(f"Start Batch Ingestion for Tenant: '{self.tenant_name}' to Keyspace: '{self.__cassandra_config__['keyspace']}'")
    session.execute(f"USE {self.__cassandra_config__['keyspace']}")

    batches = get_batches(self.__tenant_staging_path__, self.__ingest_config__)
    self.__logger__.info(f"Number of Batches: {len(batches)}")

    for i, batch in enumerate(batches):
      start = time.time()
      batch_df = read_batch_df(batch)

      processed_batch_df = self.__process_batch_function__(batch_df)

      write_df_to_cassandra(session, processed_batch_df)

      self.__logger__.info(f"\tBatch {i}: Ingested {processed_batch_df.shape[0]} rows, took {time.time() - start} seconds")
