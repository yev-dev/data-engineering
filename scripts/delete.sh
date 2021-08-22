#!/bin/bash

kubectl -n=data-engineering delete -f ../helm/spark-master-deployment.yaml
kubectl -n=data-engineering delete -f ../helm/spark-master-service.yaml
kubectl -n=data-engineering delete -f ../helm/spark-worker-deployment.yaml
kubectl -n=data-engineering delete -f ../helm/minikube-ingress.yaml
