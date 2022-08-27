# Distributed log system


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

### Security
Encrypting the data sent between the client and server with SSL/TLS.
gRPS should communicate over TLS

### Security in distributed services can be broken down into three steps:
- Encrypt data in-flight to protect against man-in-the-middle attacks
- Authenticate to identify clients
- Authorize to determine the permissions of the identified clients

### Cert
- CFSSL from CloudFlare4 for signing, verifying, and bundling TLS certificates

