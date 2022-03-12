# Run from code/tenants: python gift_card/clientbatchingestapp.py

from shared.utils import parse_ingest_config, get_batches
from pathlib import Path

if __name__ == "__main__":

  tenant_dir = Path(__file__).parent
  tenant_config_path = tenant_dir.joinpath("clientbatchingestapp.cfg")
  tenant_staging_path = tenant_dir.joinpath("staging")

  ingest_config = parse_ingest_config(tenant_config_path)
  batches = get_batches(tenant_staging_path, ingest_config)
  print(batches)