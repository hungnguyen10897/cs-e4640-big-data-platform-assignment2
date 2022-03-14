from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
import pandas as pd


REVIEWS_DTYPES = {
  "marketplace": "TEXT",
  "customer_id": "TEXT",
  "review_id": "TEXT",
  "product_id": "TEXT",
  "product_parent": "TEXT",
  "product_title": "TEXT",
  "product_category": "TEXT",
  "star_rating": "INT",
  "helpful_votes": "INT",
  "total_votes": "INT",
  "vine": "TEXT",
  "verified_purchase": "TEXT",
  "review_headline": "TEXT",
  "review_body": "TEXT",
  "review_date": "TEXT",
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
    file_df = pd.read_csv(file_path, sep='\t', header=0)
    batch_df = pd.concat([batch_df, file_df], ignore_index=True)
  return batch_df


def write_df_to_cassandra(session, df):

  COLUMNS_PLACEHOLDERS = ', '.join(REVIEWS_COLUMNS)
  VALUES_PLACEHOLDERS = ', '.join(len(REVIEWS_COLUMNS) * ["?"])
  INSERT_STMT = f'INSERT INTO reviews ({COLUMNS_PLACEHOLDERS}) VALUES ({VALUES_PLACEHOLDERS});'
  insert_statement = session.prepare(INSERT_STMT)

  for item in df:
    session.execute(insert_statement, (item[0],item[1],item[2],item[3],item[4],item[5],item[6], \
      item[7],item[8],item[9],item[10],item[11],item[12],item[13],item[14]))

  