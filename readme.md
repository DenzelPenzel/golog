# Distributed log system

- Single-writer and multiple-reader distributed service

### Log Models

- Record — the data stored in our log
- Store — the file we store records in
- Index — the file we store index entries in
- Segment — the abstraction that ties a store and an index together
- Log — the abstraction that ties all the segments together.

### Features

- define a gRPC service in protobuf
- compile gRPC protobufs into code
- build a gRPC server
- end-to-end testing

### Protobuff runtime

- google.golang.org/protobuf

### Compile protobuf

- make

### Generate certs

- make gencert

### Test app

Run the following snippet to start the server:

```
./run.sh
```

Open another tab in your terminal and run the following commands to add some records to your log:

```
$ curl -X POST localhost:4001 -d \
'{"record": {"value": "AA"}}'
$ curl -X POST localhost:4001 -d \
'{"record": {"value": "BB"}}'
$ curl -X POST localhost:4001 -d \
'{"record": {"value": "CC"}}'
```

Verify that you get the associated records back from the server:

```
curl -X GET localhost:4001 -d '{"offset": 0}'
curl -X GET localhost:4001 -d '{"offset": 1}'
curl -X GET localhost:4001 -d '{"offset": 2}'
```

### Run tests

```
make test
```

### Run specific test

```
cd specific folder
go test -v -debug=true
```

### Security

Encrypting the data sent between the client and server with SSL/TLS.
gRPS should communicate over TLS

### Security in distributed services can be broken down into three steps

- Encrypt data in-flight to protect against man-in-the-middle attacks
- Authenticate to identify clients
- Authorize to determine the permissions of the identified clients

### Cert

- CFSSL from CloudFlare4 for signing, verifying, and bundling TLS certificates

### Serf

Unlike service registry projects like ZooKeeper and Consul,

Serf doesn’t have a central-registry architectural style.
Each instance of your service in the cluster runs as a Serf node.

These nodes exchange messages with each other
You listen to Serf for messages about changes in the cluster
and then handle them accordingly

### Setup service discovery with Serf

- Create a Serf node on each server
- Configure each Serf node with an address to listen on and accept connections from other Serf nodes
- Configure each Serf node with addresses of other Serf nodes and join their cluster
- Handle Serf’s cluster discovery events, such as when a node joins or fails in the cluster

### Service Discovery

Discovery is important because the discovery events trigger other processes
in our service like replication and consensus.

### Pull-based replication

In pull-based replication, the consumer periodically polls the data source to
check if it has new data to consume.

In push-based replication, the data source pushes the data to its replicas.

### Raft

Leader maintains power by sending heartbeat requests to its followers.

Every Raft server has a term: a monotonically increasing integer that tells
other servers how authoritative and current this server is

Imagine you’ve built a job system with a database of jobs to run and a program
that queries the database every second to check if there’s a job to run and,
if so, runs the job. You want this system to be highly available and resilient
to failures, so you run multiple instances of the job runner. But you don’t
want all of the runners running simultaneously and duplicating the work.

So you use Raft to elect a leader; only the leader runs the jobs, and if the
leader fails, Raft elects a new leader that runs the jobs.

### Raft replication

The recommended number of servers in a Raft cluster is three and five.

A Raft cluster of three servers will tolerate a single server failure while a cluster
of five will tolerate two server failures. I recommend odd number cluster sizes
because Raft will handle (N–1)/2 failures, where N is the size of your cluster

### Route and Balance Requests with Pickers

In the gRPC architecture, gRPC resolves services and balances calls across them.
Clients can dynamically discover servers.
Pickers can handle the RPC balancing logic or build your own routing logic with them.

### Helm chart nginx

NGINX can be accessed through the following DNS name from within your cluster:
my-release-nginx.default.svc.cluster.local (port 80)

To access NGINX from outside the cluster, follow the steps below:

1. Get the NGINX URL by running these commands:

NOTE: It may take a few minutes for the LoadBalancer IP to be available.
Watch the status with: 'kubectl get svc --namespace default -w my-release-nginx'

```
export SERVICE_PORT=$(kubectl get --namespace default -o jsonpath="{.spec.ports[0].port}" services my-release-nginx)
export SERVICE_IP=$(kubectl get svc --namespace default my-release-nginx -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
echo "http://${SERVICE_IP}:${SERVICE_PORT}"
```

### Kubernetes locally

- use Kind tool to run a local Kubernetes cluster in Docker

### Kubernetes Services

A Service in Kubernetes exposes an application as a network service. You
define a Service with policies that specify what Pods the Service applies to
and how to access the Pods.

Four types of services specify how the Service exposes the Pods:

- ClusterIP exposes the Service on a load-balanced cluster-internal IP so
  the Service is reachable within the Kubernetes cluster only. This is the
  default Service type

- NodePort exposes the Service on each Node’s IP on a static port—even if
  the Node doesn’t have a Pod on it, Kubernetes sets up the routing so if
  you request a Node at the service’s port, it’ll direct the request to the
  proper place. You can request NodePort services outside the Kubernetes
  cluster

- LoadBalancer exposes the Service externally using a cloud provider’s load
  balancer. A LoadBalancer Service automatically creates ClusterIP and
  NodeIP services behind the scenes and manages the routes to these services

- ExternalName is a special Service that serves as a way to alias a DNS
  name

### Local Deployment

Build the image and load it into your Kind cluster by running:

- make build-docker
- kind create cluster
- kind load docker-image github.com/denisschmidt/golog:0.0.1

Install the Chart:

- helm install golog deploy/golog
- kubectl get pods
- kubectl port-forward pod/golog-0 8400 8400 (request a service running inside Kubernetes without a
  load balancer)

Connect to Docker image

- sudo docker exec –it <docker_name> /bin/bash

