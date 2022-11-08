module github.com/denisschmidt/golog

require (
	github.com/casbin/casbin v1.9.1
	github.com/golang/protobuf v1.5.2
	github.com/gorilla/mux v1.8.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0
	github.com/hashicorp/raft v1.1.1
	github.com/hashicorp/raft-boltdb v0.0.0-20191021154308-4207f1bf0617
	github.com/hashicorp/serf v0.9.7
	github.com/soheilhy/cmux v0.1.4
	github.com/spf13/cobra v1.0.0
	github.com/spf13/viper v1.4.0
	github.com/stretchr/testify v1.8.0
	github.com/tysontate/gommap v0.0.0-20190103205956-899e1273fb5c
	go.opencensus.io v0.23.0
	go.uber.org/zap v1.17.0
	google.golang.org/genproto v0.0.0-20220519153652-3a47de7e79bd
	google.golang.org/grpc v1.46.2
	google.golang.org/grpc/examples v0.0.0-20221020162917-9127159caf5a // indirect
	google.golang.org/protobuf v1.28.0
	launchpad.net/gocheck v0.0.0-20140225173054-000000000087 // indirect
)

replace github.com/hashicorp/raft-boltdb => github.com/travisjeffery/raft-boltdb v0.0.0-20201002143322-bc94ee46437b

go 1.13
