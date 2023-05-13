package agent

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/soheilhy/cmux"
	"io"
	"net"
	"sync"
	"time"

	"github.com/denisschmidt/golog/internal/auth"
	"github.com/denisschmidt/golog/internal/discovery"
	"github.com/denisschmidt/golog/internal/log"
	"github.com/denisschmidt/golog/internal/server"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

/*
Agent

Manages the different components and processes that make up the service

An Agent runs on every service instance, setting up and connecting all the
different components. The struct references each component (log, server,
membership, replicator) that the Agent manages.
*/
type Agent struct {
	Config

	mux        cmux.CMux
	log        *log.DistributedLog
	server     *grpc.Server
	membership *discovery.Membership

	replicator *log.Replicator

	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

type Config struct {
	ServerTLSConfig *tls.Config
	PeerTLSConfig   *tls.Config
	// DataDir stores the log and raft data
	DataDir string
	// BindAddr is the address serf runs on
	BindAddr string
	// RPCPort is the port for client (and Raft) connections
	RPCPort int
	// Raft server id
	NodeName       string
	StartJoinAddrs []string

	ACLModelFile  string
	ACLPolicyFile string

	// Bootstrap should be set to true when starting the first node of the cluster.
	Bootstrap bool
}

func (c Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

// New - creates an Agent and runs a set of methods to set up and run the agent’s components
func New(config Config) (*Agent, error) {
	a := &Agent{
		Config:    config,
		shutdowns: make(chan struct{}),
	}

	setup := []func() error{
		a.setupLogger,
		a.setupMux,
		a.setupLog,
		a.setupServer,
		a.setupMembership,
	}

	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}

	go a.serve()

	return a, nil
}

// creates a listener on RPC address that accept both Raft and gRPC connections
// then creates the mux with the listener
// mux will accept connections on that listener and match connections based on your configured rules
func (a *Agent) setupMux() error {
	addr, err := net.ResolveTCPAddr("tcp", a.Config.BindAddr)
	if err != nil {
		return err
	}
	rpcAddr := fmt.Sprintf(
		"%s:%d",
		addr.IP.String(),
		a.Config.RPCPort,
	)
	ln, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}
	a.mux = cmux.New(ln)
	return nil
}

func (a *Agent) setupLogger() error {
	logger, err := zap.NewDevelopment()
	if err != nil {
		return err
	}
	zap.ReplaceGlobals(logger)
	return nil
}

// configure the rule to match Raft and create the distributed log
// identify Raft connections by reading one byte and checking that the byte
// matches the byte we set up our outgoing Raft connections to write in Stream Layer
func (a *Agent) setupLog() error {
	raftLn := a.mux.Match(func(reader io.Reader) bool {
		b := make([]byte, 1)
		if _, err := reader.Read(b); err != nil {
			return false
		}
		return bytes.Compare(b, []byte{byte(log.RaftRPC)}) == 0
	})

	logConfig := log.Config{}
	logConfig.Raft.StreamLayer = log.NewStreamLayer(
		raftLn,
		a.Config.ServerTLSConfig,
		a.Config.PeerTLSConfig,
	)

	rpcAddr, err := a.Config.RPCAddr()
	if err != nil {
		return err
	}
	logConfig.Raft.BindAddr = rpcAddr
	logConfig.Raft.LocalID = raft.ServerID(a.Config.NodeName)
	logConfig.Raft.Bootstrap = a.Config.Bootstrap

	a.log, err = log.NewDistributedLog(
		a.Config.DataDir,
		logConfig,
	)
	if err != nil {
		return err
	}
	if a.Config.Bootstrap {
		return a.log.WaitForLeader(3 * time.Second)
	}
	return nil
}

func (a *Agent) setupServer() error {
	authorizer := auth.New(
		a.Config.ACLModelFile,
		a.Config.ACLPolicyFile,
	)

	serverConfig := &server.Config{
		CommitLog:   a.log,
		Authorizer:  authorizer,
		GetServerer: a.log,
	}

	var opts []grpc.ServerOption

	if a.Config.ServerTLSConfig != nil {
		creds := credentials.NewTLS(a.Config.ServerTLSConfig)
		opts = append(opts, grpc.Creds(creds))
	}

	var err error
	a.server, err = server.NewGRPCServer(serverConfig, opts...)
	if err != nil {
		return err
	}

	// matches any connections
	grpcLn := a.mux.Match(cmux.Any())
	go func() {
		// tell our gRPC server to serve on the multiplexed listener.
		if err := a.server.Serve(grpcLn); err != nil {
			_ = a.Shutdown()
		}
	}()

	return err
}

/*
Sets up a Replicator with the gRPC dial options, needed to connect to other servers and a client
so the replicator can connect to other servers consume their data,
and produce a copy of the data to the local server
*/
func (a *Agent) setupMembership() error {
	rpcAddr, err := a.Config.RPCAddr()
	if err != nil {
		return err
	}
	a.membership, err = discovery.New(a.log, discovery.Config{
		NodeName: a.Config.NodeName,
		BindAddr: a.Config.BindAddr,
		Tags: map[string]string{
			"rpc_addr": rpcAddr,
		},
		StartJoinAddrs: a.Config.StartJoinAddrs,
	})
	return err
}

/*
Shutdown
  - Leaving the membership so that other servers will see that this server
    has left the cluster and so that this server doesn’t receive discovery events anymore;

- Closing the replicator so it doesn’t continue to replicate;

  - Gracefully stopping the server, which stops the server from accepting
    new connections and blocks until all the pending RPCs have finished; and

- Closing the log
*/
func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()
	if a.shutdown {
		return nil
	}
	a.shutdown = true
	close(a.shutdowns)

	shutdown := []func() error{
		a.membership.Leave,
		// a.replicator.Close,
		func() error {
			a.server.GracefulStop()
			return nil
		},
		a.log.Close,
	}

	for _, fn := range shutdown {
		if err := fn(); err != nil {
			return err
		}
	}

	return nil
}

func (a *Agent) serve() error {
	if err := a.mux.Serve(); err != nil {
		_ = a.Shutdown()
		return err
	}
	return nil
}
