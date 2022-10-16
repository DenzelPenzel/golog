package loadbalance

import (
	"fmt"
	"go.uber.org/zap"
	"google.golang.org/grpc"
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

/*
Build - receives the data needed to build a resolver that can discover the
servers (like the target address) and the client connection the resolver will
update with the servers it discovers.

Sets up a client connection to
our server so the resolver can call the GetServers() API.
*/
func (r Resolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r.logger = zap.L().Named("resolver")
	r.clientConn = cc
	var dialOpts []grpc.DialOption
	if opts.DialCreds != nil {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(opts.DialCreds))
	}
	r.serviceConfig = r.clientConn.ParseServiceConfig(fmt.Sprintf(`{"loadBalancingConfig":[{"%s":{}}]}`, Name))

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
func (r Resolver) Scheme() string {
	return Name
}

func (r Resolver) ResolveNow(options resolver.ResolveNowOptions) {
	//TODO implement me
	panic("implement me")
}

func (r Resolver) Close() {
	//TODO implement me
	panic("implement me")
}

func init() {
	resolver.Register(&Resolver{})
}

var _ resolver.Builder = (*Resolver)(nil)
var _ resolver.Resolver = (*Resolver)(nil)
