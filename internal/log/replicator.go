package log

import (
	"context"
	"sync"

	api "github.com/denisschmidt/golog/api/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// The replicator connects to other servers with the gRPC client
type Replicator struct {
	DialOptions []grpc.DialOption
	LocalServer api.LogClient
	logger      *zap.Logger
	mu          sync.Mutex
	servers     map[string]chan struct{} // server address to a channel
	closed      bool
	close       chan struct{}
}

// Adds the given server address to the list of servers to replicate
// and launch the add goroutine to run the actual replication logic
func (r *Replicator) Join(name, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if r.closed {
		return nil
	}

	if _, ok := r.servers[name]; ok {
		// already replicating so skip
		return nil
	}

	r.servers[name] = make(chan struct{})

	// run replication goroutine
	go r.replicate(addr, r.servers[name])

	return nil
}

func (r *Replicator) replicate(addr string, leave chan struct{}) {
	cc, err := grpc.Dial(addr, r.DialOptions...)

	if err != nil {
		r.logError(err, "failed to dial", addr)
		return
	}

	defer cc.Close()

	// create a client
	client := api.NewLogClient(cc)

	// open up a stream to consume all logs on the server
	ctx := context.Background()
	stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{
		Offset: 0,
	})

	if err != nil {
		r.logError(err, "failed to consume", addr)
		return
	}

	records := make(chan *api.Record)

	go func() {
		for {
			recv, err := stream.Recv()
			if err != nil {
				r.logError(err, "failed to receive", addr)
				return
			}
			records <- recv.Record
		}
	}()

	/*
		The loop consumes the logs from the discovered server in a stream and then
		produces to the local server to save a copy.

		We replicate messages from the
		other server until that server fails or leaves the cluster and the replicator
		closes the channel for that server, which breaks the loop and ends the replicate()
		goroutine. The replicator closes the channel when Serf receives an event
		saying that the other server left the cluster, and then this server calls the
		Leave() method that we’re about to add
	*/
	for {
		select {
		case <-r.close:
			return

		case <-leave:
			return

		case record := <-records:
			_, err := r.LocalServer.Produce(ctx, &api.ProduceRequest{
				Record: record,
			})
			if err != nil {
				r.logError(err, "failed to produce", addr)
				return
			}
		}
	}
}

// Remove server from the cluster and close the server's channel
func (r *Replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if _, ok := r.servers[name]; !ok {
		return nil
	}
	close(r.servers[name])
	delete(r.servers, name)
	return nil
}

func (r *Replicator) init() {
	if r.logger == nil {
		r.logger = zap.L().Named("replicator")
	}

	if r.servers == nil {
		r.servers = make(map[string]chan struct{})
	}

	if r.close == nil {
		r.close = make(chan struct{})
	}
}

func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if r.closed {
		return nil
	}

	r.closed = true
	// close channel
	close(r.close)
	return nil
}

func (r *Replicator) logError(err error, msg, addr string) {
	r.logger.Error(
		msg,
		zap.String("addr", addr),
		zap.Error(err),
	)
}
