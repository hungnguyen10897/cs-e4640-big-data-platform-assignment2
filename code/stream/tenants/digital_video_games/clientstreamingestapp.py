# Run from code/stream: python tenants/digital_video_games/clientstreamingestapp.py

import json,sys
from kafka import KafkaConsumer
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from tenants.shared import REVIEWS_DTYPES, INSERT_STMT, parse_cassandra_config, parse_kafka_config


TENANT = "digital_video_games"


def ingest_to_cassandra(record):
  row_values = list(map(lambda key: record[key], REVIEWS_DTYPES.keys()))
  session.execute(insert_statement, row_values)


if __name__ == "__main__":

  kafka_config = parse_kafka_config(f"./tenants/{TENANT}/clientstreamingestapp.cfg")

  consumer = KafkaConsumer(TENANT, 
    bootstrap_servers=[kafka_config["boostrap_server"]], 
    auto_offset_reset='earliest',
    consumer_timeout_ms=int(kafka_config["consumer_timeout_ms"]))

  cassandra_config = parse_cassandra_config(f"./tenants/{TENANT}/clientstreamingestapp.cfg")

  auth_provider = PlainTextAuthProvider(username=cassandra_config["username"], password=cassandra_config["password"])
  cluster = Cluster([cassandra_config["host"]], auth_provider=auth_provider)
  session = cluster.connect()

  session.execute(f"USE {cassandra_config['keyspace']}")

  insert_statement = session.prepare(INSERT_STMT)

  print(f"Start reading message from topic '{TENANT}'")
  msg_count = 0
  for message in consumer:
    record = json.loads(message.value)
    ingest_to_cassandra(record)
    msg_count+=1
    print(f"Read and ingest {msg_count} message(s)")