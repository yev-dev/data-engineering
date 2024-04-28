# Data Engineering Project

### Setup

If you are on Mac, make sure to enable Oracle America in Security & Privacy setting.

After https://matthewpalmer.net/kubernetes-app-developer/articles/guide-install-kubernetes-mac.html](Kubernetes) and Minikube installation. 

    brew install hyperkit
    brew link --overwrite hyperkit 
    minikube config set driver hyperkit

By default, the Minikube VM is configured to use 1GB of memory and 2 CPU cores. This is not sufficient for Spark jobs, so be sure to increase the memory in your Docker client (for HyperKit) or directly in VirtualBox. Then, when you start Minikube, pass the memory and CPU options to it.

Start the cluster:

```sh

$ minikube start --memory 8192 --cpus 4
$ minikube dashboard
$ kubectl cluster-info
```

Build the Docker image:

```sh
$ docker login
$ eval $(minikube docker-env)
$ docker build -t spark-hadoop:3.0.0 -f ./images/spark/Dockerfile ./images/spark
docker image ls spark-hadoop
```

Create the deployments and services:

```sh
$ kubectl create -f ./helm/namespace.yaml
$ kubectl create -f ./helm/spark-master-deployment.yaml
$ kubectl create -f ./helm/spark-master-service.yaml
$ kubectl create -f ./helm/spark-worker-deployment.yaml
$ minikube addons enable ingress
$ kubectl apply -f ./helm/minikube-ingress.yaml
```

```sh
kubectl -n=data-engineering delete -f ./helm/namespace.yaml 
kubectl -n=data-engineering delete -f ./helm/spark-master-deployment.yaml 
kubectl -n=data-engineering delete -f ./helm/spark-master-service.yaml 
kubectl -n=data-engineering delete -f ./helm/spark-worker-deployment.yaml 
kubectl -n=data-engineering delete -f ./helm/minikube-ingress.yaml
```

Verify that ingress is set

```sh
kubectl get -n=data-engineering ingress
```
Add an entry to /etc/hosts:

```sh
$ echo "$(minikube ip) spark-kubernetes" | sudo tee -a /etc/hosts
```

Test it out in the browser at [http://spark-kubernetes/](http://spark-kubernetes/).


```sh
$ kubectl -n=data-engineering  get namespaces
$ kubectl -n=data-engineering  get pods
$ kubectl -n=data-engineering  get deployment
$ kubectl -n=data-engineering  get replicaset
$ kubectl -n=data-engineering  get service
```

Alternatively, if you don't want to create ingress service, you can setup port forwarding

```sh
# Port forwarding for dashboard
$ kubectl -n=data-engineering port-forward service/spark-master 8080:8080
$ kubectl -n=data-engineering port-forward service/spark-master 7077:7077

# Kubernetes will allocate a random port number
$ kubectl port-forward service/spark-master :8080

# Change spark-master-58ffc45dc9-nlsvp to the name of the Pod
$ kubectl port-forward spark-master-58ffc45dc9-nlsvp 7077:7077
$ kubectl port-forward spark-master-58ffc45dc9-nlsvp 8080:8080 

# Change spark-master-58ffc45dc9-nlsvp to the name of the Pod to check all available ports
$ kubectl get pod spark-master-58ffc45dc9-nlsvp --template='{{(index (index .spec.containers 0).ports 0).containerPort}}{{"\n"}}'
```


To validate PySpark

```sh
kubectl -n=data-engineering get pods
kubectl -n=data-engineering describe pod <master-pod>
kubectl -n=data-engineering logs <master-pod>
kubectl -n=data-engineering exec <master-pod> -it -- pyspark
kubectl -n=data-engineering logs -f <master-pod>
```

```sh
words = 'the quick brown fox jumps over the\
        lazy dog the quick brown fox jumps over the lazy dog'
sc = SparkContext()
seq = words.split()
data = sc.parallelize(seq)
counts = data.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).collect()
dict(counts)
sc.stop()

```

```sh
spark-submit \                                      
--master k8s://https://192.168.99.110:7077 \
--deploy-mode cluster \
--name spark-pi \
--class org.apache.spark.examples.SparkPi \
--conf spark.executor.instances=2 \
--conf spark.kubernetes.container.image=spark-docker \
local:///opt/spark/examples/jars/spark-examples_2.11-2.3.0.jar
```

### Links

[Deployment of Standalone Spark Cluster on Kubernetes](https://kienmn97.medium.com/deployment-of-standalone-spark-cluster-on-kubernetes-ba15978658bf)
[Running spark-shell on Kubernetes](https://timothyzhang.medium.com/running-spark-shell-on-kubernetes-3181d4446622)


### DB accounts

-- create user pct with encrypted password 'password123';

psql postgres


CREATE ROLE pct WITH LOGIN PASSWORD 'password123';
ALTER ROLE pct CREATEDB;


CREATE ROLE market_data WITH LOGIN PASSWORD 'password123';
ALTER ROLE market_data CREATEDB;

\q
psql postgres -U market_data

\du => list of all roles


ALTER USER market_data set SEARCH_PATH = 'market_data';

ALTER USER pct set SEARCH_PATH = 'pct';


-- For the session
SET search_path TO market_data;
SET search_path TO pct;

-- To permanently change the schema
ALTER USER username SET search_path = schema_name;