import numpy as np
import configparser

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

def parse_cassandra_config(config_path):
  config = configparser.ConfigParser()
  config.read(config_path)
  return dict(config.items('CASSANDRA'))

def parse_kafka_config(config_path):
  config = configparser.ConfigParser()
  config.read(config_path)
  return dict(config.items('KAFKA'))


