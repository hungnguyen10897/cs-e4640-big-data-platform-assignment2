from operator import le
import os, configparser, json, logging
from tenant import Tenant


# Tenant's directories discovery
def scan_tenants():
  dirs = next(os.walk("./tenants", topdown=True))[1]
  tenants = list(filter(lambda x: x!="shared", dirs))
  return tenants


if __name__ == "__main__":

  logging.basicConfig(filename='platform.log', filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level="INFO")

  config = configparser.ConfigParser()
  config.read("./mysimbdp-batchingestmanager.cfg")
  
  available_tenants = scan_tenants()
  tenants_in_use = json.loads(config.get("DEFAULT", "tenants"))

  logging.info(f"Tenants Present in the platform: {available_tenants}")
  logging.info(f"Tenants Registered in the platform: {tenants_in_use}")
  
  for tenant in tenants_in_use:
    if tenant not in available_tenants:
      continue
    tenant_obj = Tenant(tenant)
    tenant_obj.batch_ingest()

