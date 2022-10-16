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

# Service Discovery

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
