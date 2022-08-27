package server

import (
	"context"
	"io/ioutil"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	api "github.com/denisschmidt/golog/api/v1"
	"github.com/denisschmidt/golog/internal/auth"
	"github.com/denisschmidt/golog/internal/config"
	"github.com/denisschmidt/golog/internal/log"
)

// defines our list of test cases and then runs a subtest for each case
func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		rootClient api.LogClient,
		nobodyClient api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log succeeeds": testProduceConsume,
		"produce/consume stream succeeds":                     testProduceConsumeStream,
		"consume past log boundary fails":                     testConsumePastBoundary,
		"unauthorized fails":                                  testUnauthorized,
	} {
		t.Run(scenario, func(t *testing.T) {
			rootClient, nobodyClient, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, rootClient, nobodyClient, config)
		})
	}
}

// it is a helper function to set up each test case
//
func setupTest(t *testing.T, fn func(*Config)) (
	rootClient api.LogClient,
	nobodyClient api.LogClient,
	cfg *Config,
	teardown func(),
) {
	t.Helper()

	// creating a listener on the local network address
	// the 0 port is useful for when we don’t care what port we use since 0 will automatically assign us a free port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	// START: multi_client
	newClient := func(crtPath, keyPath string) (
		*grpc.ClientConn,
		api.LogClient,
		[]grpc.DialOption,
	) {
		// configure our client’s TLS credentials to use our CA as the client’s Root CA (the CA it will use to verify the server
		clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CertFile: crtPath,
			KeyFile:  keyPath,
			CAFile:   config.CAFile,
			Server:   false,
		})
		require.NoError(t, err)

		tlsCreds := credentials.NewTLS(clientTLSConfig)

		opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
		conn, err := grpc.Dial(listener.Addr().String(), opts...)
		require.NoError(t, err)
		// init client client
		client := api.NewLogClient(conn)
		return conn, client, opts
	}

	var rootConn *grpc.ClientConn
	rootConn, rootClient, _ = newClient(
		config.RootClientCertFile,
		config.RootClientKeyFile,
	)

	var nobodyConn *grpc.ClientConn
	nobodyConn, nobodyClient, _ = newClient(
		config.NobodyClientCertFile,
		config.NobodyClientKeyFile,
	)

	// END: multi_client

	// hook up our server with its certificate and enable it to handle TLS connections
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile: config.ServerCertFile,
		KeyFile:  config.ServerKeyFile,
		CAFile:   config.CAFile,
		Server:   true,
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	clientLog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	// auth
	authorizer := auth.New(config.ACLModelFile, config.ACLPolicyFile)

	cfg = &Config{
		CommitLog:  clientLog,
		Authorizer: authorizer,
	}

	if fn != nil {
		fn(cfg)
	}

	// create server, pass TLS credentials to server
	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go func() {
		// start serving requests in a goroutine because Serve method is a blocking call
		// if we didn’t run it in a goroutine our tests further down would never run
		server.Serve(listener)
	}()

	return rootClient, nobodyClient, cfg, func() {
		server.Stop()
		rootConn.Close()
		nobodyConn.Close()
		listener.Close()
		clientLog.Remove()
	}
}

func testProduceConsume(t *testing.T, client, _ api.LogClient, config *Config) {
	ctx := context.Background()

	record := &api.Record{
		Value: []byte("hello world"),
	}

	// produce a record to the log
	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: record,
	})
	require.NoError(t, err)

	// consume it back, and then check that the record we sent is the same one we got back
	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset,
	})
	require.NoError(t, err)
	require.Equal(t, record.Value, consume.Record.Value)
	require.Equal(t, record.Offset, consume.Record.Offset)
}

// tests that our server responds with an api.ErrOffsetOutOfRange() error
// when a client tries to consume beyond the log’s boundaries.
func testConsumePastBoundary(t *testing.T, client, _ api.LogClient, config *Config) {
	ctx := context.Background()

	record := &api.Record{
		Value: []byte("hello world"),
	}

	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: record,
	})

	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset + 1,
	})

	if consume != nil {
		t.Fatal("consume not nil")
	}

	actual := grpc.Code(err)
	expect := grpc.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())

	if expect != actual {
		t.Fatalf("got err: %v, expect: %v", actual, expect)
	}
}

// it is the streaming counterpart to testProduceConsume()
// testing that we can produce and consume through streams
func testProduceConsumeStream(t *testing.T, client, _ api.LogClient, config *Config) {
	ctx := context.Background()

	records := []*api.Record{{
		Value:  []byte("first message"),
		Offset: 0,
	}, {
		Value:  []byte("second message"),
		Offset: 1,
	}}

	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)

		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{
				Record: record,
			})
			require.NoError(t, err)
			res, err := stream.Recv()
			require.NoError(t, err)

			if res.Offset != uint64(offset) {
				t.Fatalf("got offset: %d, want: %d", res.Offset, offset)
			}
		}
	}

	{
		stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
		require.NoError(t, err)

		for i, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, res.Record, &api.Record{
				Value:  record.Value,
				Offset: uint64(i),
			})
		}

	}

}

func testUnauthorized(t *testing.T, _, client api.LogClient, config *Config) {
	ctx := context.Background()
	record := &api.Record{
		Value: []byte("hello world"),
	}

	produce, err := client.Produce(
		ctx,
		&api.ProduceRequest{
			Record: record,
		},
	)

	if produce != nil {
		t.Fatalf("produce response should be nil")
	}

	currentCode, expectedCode := status.Code(err), codes.PermissionDenied

	if currentCode != expectedCode {
		t.Fatalf("current code: %d, expected: %d", currentCode, expectedCode)
	}

	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: 0,
	})

	if consume != nil {
		t.Fatalf("consume response should be nil")
	}

	currentCode, expectedCode = status.Code(err), codes.PermissionDenied

	if currentCode != expectedCode {
		t.Fatalf("got code: %d, want: %d", currentCode, expectedCode)
	}
}
