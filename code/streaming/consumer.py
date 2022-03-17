import json
import numpy as np
from kafka import KafkaConsumer
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster

REVIEWS_DTYPES = {
  "marketplace": object,
  "customer_id": object,
  "review_id": object,
  "product_id": object,
  "product_parent": object,
  "product_title": object,
  "product_category": object,
  "star_rating": np.int64,
  "helpful_votes": np.int64,
  "total_votes": np.int64,
  "vine": object,
  "verified_purchase": object,
  "review_headline": object,
  "review_body": object,
  "review_date": object,
}

REVIEWS_COLUMNS = list(REVIEWS_DTYPES.keys())

COLUMNS_PLACEHOLDERS = ', '.join(REVIEWS_COLUMNS)
VALUES_PLACEHOLDERS = ', '.join(len(REVIEWS_COLUMNS) * ["?"])
INSERT_STMT = f'INSERT INTO reviews ({COLUMNS_PLACEHOLDERS}) VALUES ({VALUES_PLACEHOLDERS});'

def ingest_to_kafka(record):
  print("Inserting row to mysimbdp")
  row_values = list(map(lambda key: record[key], REVIEWS_DTYPES.keys()))
  session.execute(insert_statement, row_values)


if __name__ == "__main__":
  consumer = KafkaConsumer('gift_card', 
    bootstrap_servers=["localhost:29092"], 
    auto_offset_reset='earliest',
    consumer_timeout_ms=100000)

  auth_provider = PlainTextAuthProvider(username="k8ssandra-superuser", password="L8AqSx3E7kZofZ00Pash")

  cluster = Cluster(["localhost"], auth_provider=auth_provider)
  session = cluster.connect()

  session.execute(f"USE mysimbdp")

  insert_statement = session.prepare(INSERT_STMT)

  print("Start reading message from gift_card")
  for message in consumer:
    record = json.loads(message.value)
    print(record)
    ingest_to_kafka(record)