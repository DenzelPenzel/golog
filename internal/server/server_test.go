package server

import (
	"io/ioutil"
	"net"
	"testing"

	api "github.com/denisschmidt/golog/api/v1"
	"github.com/denisschmidt/golog/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// defines our list of test cases and then runs a subtest for each case
func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log succeeeds": testProduceConsume,
		"produce/consume stream succeeds":                     testProduceConsumeStream,
		"consume past log boundary fails":                     testConsumePastBoundary,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, config)
		})
	}
}

// it is a helper function to set up each test case
//
func setupTest(t *testing.T, fn func(*Config)) (client api.LogClient, cfg *Config, teardown func()) {
	t.Helper()

	// creating a listener on the local network address
	// the 0 port is useful for when we don’t care what port we use since 0 will automatically assign us a free port
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	clientOptions := []grpc.DialOption{grpc.WithInsecure()}

	cc, err := grpc.Dial(listener.Addr().String(), clientOptions...)
	require.NoError(t, err)

	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)

	clientLog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	cfg = &Config{
		CommitLog: clientLog,
	}

	if fn != nil {
		fn(cfg)
	}

	// create server
	server, err := NewGRPCServer(cfg)
	require.NoError(t, err)

	go func() {
		// start serving requests in a goroutine because Serve method is a blocking call
		// if we didn’t run it in a goroutine our tests further down would never run
		server.Serve(listener)
	}()

	client = api.NewLogClient(cc)

	return client, cfg, func() {
		server.Stop()
		cc.Close()
		listener.Close()
		clientLog.Remove()
	}
}

func testProduceConsume() {

}
