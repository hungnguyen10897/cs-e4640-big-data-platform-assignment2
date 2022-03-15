from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
import pandas as pd
import numpy as np


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


def get_cassandra_session(cassandra_config):
  auth_provider = PlainTextAuthProvider(username=cassandra_config["username"], password=cassandra_config["password"])

  cluster = Cluster([cassandra_config["host"]], auth_provider=auth_provider)
  session = cluster.connect()

  return session


def read_batch_df(batch):
  batch_df = pd.DataFrame()
  for file_path in batch:
    file_df = pd.read_csv(file_path, sep='\t', header=0, dtype=REVIEWS_DTYPES)
    batch_df = pd.concat([batch_df, file_df], ignore_index=True)
  return batch_df


def write_df_to_cassandra(session, df):

  COLUMNS_PLACEHOLDERS = ', '.join(REVIEWS_COLUMNS)
  VALUES_PLACEHOLDERS = ', '.join(len(REVIEWS_COLUMNS) * ["?"])
  INSERT_STMT = f'INSERT INTO reviews ({COLUMNS_PLACEHOLDERS}) VALUES ({VALUES_PLACEHOLDERS});'
  insert_statement = session.prepare(INSERT_STMT)

  for row in df.iterrows():

    row_values = list(map(lambda key: row[1][key], REVIEWS_DTYPES.keys()))
    session.execute(insert_statement, row_values)

  