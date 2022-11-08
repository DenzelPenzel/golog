package loadbalance

import (
	"context"
	"fmt"
	api "github.com/denisschmidt/golog/api/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
	"sync"
)

type Resolver struct {
	mu sync.Mutex
	// user's client connection
	clientConn    resolver.ClientConn
	resolverConn  *grpc.ClientConn
	serviceConfig *serviceconfig.ParseResult
	logger        *zap.Logger
}

const Name = "golog"

var _ resolver.Builder = (*Resolver)(nil)
var _ resolver.Resolver = (*Resolver)(nil)

/*
Build - receives the data needed to build a resolver that can discover the
servers (like the target address) and the client connection the resolver will
update with the servers it discovers.

Sets up a client connection to
our server so the resolver can call the GetServers() API.
*/
func (r *Resolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r.logger = zap.L().Named("resolver")
	r.clientConn = cc

	var dialOpts []grpc.DialOption

	if opts.DialCreds != nil {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(opts.DialCreds))
	}

	r.serviceConfig = r.clientConn.ParseServiceConfig(
		fmt.Sprintf(`{"loadBalancingConfig":[{"%s":{}}]}`, Name),
	)

	var err error
	r.resolverConn, err = grpc.Dial(target.Endpoint, dialOpts...)
	if err != nil {
		return nil, err
	}

	r.ResolveNow(resolver.ResolveNowOptions{})

	return r, nil
}

/*
Scheme - returns the resolver’s scheme identifier. When you call grpc.Dial,
gRPC parses out the scheme from the target address you gave it and tries
to find a resolver that matches, defaulting to its DNS resolver. For our
resolver, you’ll format the target address like this: golog://your-serviceaddress.
*/
func (r *Resolver) Scheme() string {
	return Name
}

/*
ResolveNow - resolve the target, discover the servers, and update the client
connection with the servers.

Services can specify how clients should balance their calls to the service by
updating the state with a service config.
*/
func (r *Resolver) ResolveNow(resolver.ResolveNowOptions) {
	// protect access across goroutines
	r.mu.Lock()
	defer r.mu.Unlock()
	client := api.NewLogClient(r.resolverConn)
	// get cluster and then set on cc attributes
	ctx := context.Background()
	// get list of servers
	servers, err := client.GetServers(ctx, &api.GetServersRequest{})
	if err != nil {
		r.logger.Error(
			"failed to resolve server",
			zap.Error(err),
		)
		return
	}

	var addrs []resolver.Address
	for _, server := range servers.Servers {
		addrs = append(addrs, resolver.Address{
			// the address of the server to connect to
			Addr: server.RpcAddr,
			// Map containing any data that’s useful
			// for the load balancer. We’ll tell the picker what server is the leader and
			// what servers are followers with this field
			Attributes: attributes.New("is_leader", server.IsLeader), //
		})
	}

	// update the client connection
	r.clientConn.UpdateState(resolver.State{
		Addresses:     addrs,
		ServiceConfig: r.serviceConfig,
	})
}

// Close - close connection to our server created in Build()
func (r *Resolver) Close() {
	if err := r.resolverConn.Close(); err != nil {
		r.logger.Error(
			"failed to close conn",
			zap.Error(err),
		)
	}
}

func init() {
	// register resolver
	resolver.Register(&Resolver{})
}
