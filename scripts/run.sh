#!/bin/bash

minikube start --memory 8192 --cpus 4

sleep 10

minikube dashboard
