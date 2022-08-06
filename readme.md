# Distributed log system

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
