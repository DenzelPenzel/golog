package agent_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/denisschmidt/golog/internal/loadbalance"
	"io/ioutil"
	"os"
	"testing"
	"time"

	api "github.com/denisschmidt/golog/api/v1"
	"github.com/denisschmidt/golog/internal/agent"
	"github.com/denisschmidt/golog/internal/config"
	"github.com/denisschmidt/golog/internal/dynamicport"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

/*
Test replication mechanism between nodes
Sets up a three-node cluster

Replication works asynchronously across servers(have a latency)
the logs produced to one server won’t be immediately available on the replica servers
*/
func TestAgent(t *testing.T) {
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		Server:        true,
		ServerAddress: "127.0.0.1",
	})

	require.NoError(t, err)

	// configuration of the certificate that’s served between servers
	// so they can connect with and replicate each other
	peerTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		Server:        false,
		ServerAddress: "127.0.0.1",
	})

	require.NoError(t, err)

	var agents []*agent.Agent

	for i := 0; i < 3; i++ {
		// assign two ports:
		// 1) gRPC log connection
		// 2) Serf service discovery connection
		ports := dynamicport.Get(2)

		bindAddr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
		rpcPort := ports[1]

		dataDir, err := ioutil.TempDir("", "agent-test-log")
		require.NoError(t, err)

		var startJoinAddrs []string

		if i != 0 {
			startJoinAddrs = append(startJoinAddrs, agents[0].Config.BindAddr)
		}

		agent, err := agent.New(agent.Config{
			NodeName:        fmt.Sprintf("%d", i),
			Bootstrap:       i == 0,
			StartJoinAddrs:  startJoinAddrs,
			BindAddr:        bindAddr,
			RPCPort:         rpcPort,
			DataDir:         dataDir,
			ACLModelFile:    config.ACLModelFile,
			ACLPolicyFile:   config.ACLPolicyFile,
			ServerTLSConfig: serverTLSConfig,
			PeerTLSConfig:   peerTLSConfig,
		})
		require.NoError(t, err)

		agents = append(agents, agent)
	}

	// runs after the test to verify that the agents successfully shut down and to delete the test data
	defer func() {
		for _, agent := range agents {
			err := agent.Shutdown()
			require.NoError(t, err)
			require.NoError(t,
				os.RemoveAll(agent.Config.DataDir),
			)
		}
	}()

	time.Sleep(3 * time.Second)

	// create leaderNode
	leaderClient := client(t, agents[0], peerTLSConfig)
	produceResponse, err := leaderClient.Produce(
		context.Background(),
		&api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("hello"),
			},
		},
	)
	require.NoError(t, err)

	// eventually consistency issue, because we read records only from the followers
	// need to wait until replication has finished
	time.Sleep(3 * time.Second)

	consumeResponse, err := leaderClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset,
		},
	)
	require.NoError(t, err)
	require.Equal(t, consumeResponse.Record.Value, []byte("hello"))

	time.Sleep(3 * time.Second)

	// create followerNode
	followerClient := client(t, agents[1], peerTLSConfig)

	consumeResponse, err = followerClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset,
		},
	)
	require.NoError(t, err)
	require.Equal(t, consumeResponse.Record.Value, []byte("hello"))

	// check that Raft has replicated the record we produced to the leader
	consumeResponse, err = leaderClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset + 1,
		},
	)
	require.Nil(t, consumeResponse)
	require.Error(t, err)
	got := grpc.Code(err)
	want := grpc.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	require.Equal(t, got, want)

}

// set up client
func client(t *testing.T, agent *agent.Agent, tlsConfig *tls.Config) api.LogClient {
	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}

	rpcAddr, err := agent.Config.RPCAddr()
	require.NoError(t, err)

	// now gRPS knows to use our resolver
	scheme := fmt.Sprintf("%s:///%s", loadbalance.Name, rpcAddr)
	conn, err := grpc.Dial(scheme, opts...)
	require.NoError(t, err)

	client := api.NewLogClient(conn)
	return client
}
