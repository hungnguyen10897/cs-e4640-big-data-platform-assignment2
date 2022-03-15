# Run from code/: python tenants/gift_card/clientbatchingestapp.py

from tenants.shared.utils import parse_ingest_config, get_batches, parse_cassandra_config
from tenants.shared.io import get_cassandra_session, read_batch_df, write_df_to_cassandra
from pathlib import Path
import pandas as pd

def print_tenant():
  tenant_dir = Path(__file__).parent
  print(tenant_dir)


def process_batch(batch_df):
  # Filter non-purchase, non-helpful reviews
  processed_df = batch_df[(batch_df["helpful_votes"] != 0) & (batch_df["verified_purchase"] == 'Y')]

  return processed_df

if __name__ == "__main__":

  tenant_dir = Path(__file__).parent
  tenant_config_path = tenant_dir.joinpath("clientbatchingestapp.cfg")
  tenant_staging_path = tenant_dir.joinpath("staging")

  ingest_config = parse_ingest_config(tenant_config_path)
  cassandra_config = parse_cassandra_config(tenant_config_path)

  session = get_cassandra_session(cassandra_config)
  session.execute("USE mysimbdp")

  batches = get_batches(tenant_staging_path, ingest_config)

  for batch in batches:
    print(batch)
    batch_df = read_batch_df(batch)

    processed_batch_df = process_batch(batch_df)

    write_df_to_cassandra(session, processed_batch_df)

  session.close()
