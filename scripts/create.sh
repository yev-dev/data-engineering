#!/bin/bash

kubectl create -f ../helm/spark-master-deployment.yaml
kubectl create -f ../helm/spark-master-service.yaml

sleep 10

kubectl create -f ../helm/spark-worker-deployment.yaml
kubectl apply -f ../helm/minikube-ingress.yaml
