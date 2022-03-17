# This is a deployment/installation guide

Prerequisites:




Part 1 - Batch Ingest

```
minikube start \
-p mysimbdp \
--kubernetes-version=v1.20.7 \
--memory=5g \
--nodes=2 \
--cpus=4 \
--disk-size=5g
```

```
kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/master/deploy/local-path-storage.yaml
```

