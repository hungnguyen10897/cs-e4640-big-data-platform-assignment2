import os

# Tenants discovery
def scan_tenants():
  dirs = next(os.walk("./tenants", topdown=True))[1]
  tenants = list(filter(lambda x: x!="shared", dirs))
  return tenants


if __name__ == "__main__":
  tenants = scan_tenants()
  for tenant in tenants:
    exec(f"from tenants.{tenant}.clientbatchingestapp import print_tenant")
    print_tenant()
