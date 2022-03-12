import configparser, os
from pathlib import Path


def parse_ingest_config(config_path):
  config = configparser.ConfigParser()
  config.read(config_path)
  return dict(config.items('INGEST'))


def get_batches(staging_path, ingest_config):
  batches = []
  for root, _, files  in os.walk(staging_path, topdown=True):
    
    batch = []
    for file in files:
      print(file)
      # Check file extension
      extension = file.split(".")[-1]
      if extension != ingest_config["file_extension"]:
        continue

      file_path = Path(root).joinpath(file)

      # Check file size
      file_size = file_path.stat().st_size
      print(f"SIZE: {file_size}")
      if file_size <= 1024*int(ingest_config["file_size_mb"]):
        batch.append(file_path)

      if len(batch) >= int(ingest_config["file_num"]):
        batches.append(batch)
        batch = []

    # Only scan root
    break

  # Final batch
  batches.append(batch)

  return batches
