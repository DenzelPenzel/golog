package loadbalance

/*
In the gRPC architecture, pickers handle the RPC balancing logic.
Pickers pick a server from the servers discovered by the resolver to handle each RPC.

Pickers can route RPCs based on information
about the RPC, client, and server, so their utility goes beyond balancing to
any kind of request-routing logic.

*/

import (
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"strings"
	"sync"
	"sync/atomic"
)

var _ base.PickerBuilder = (*Picker)(nil)
var _ balancer.Picker = (*Picker)(nil)

type Picker struct {
	mu        sync.RWMutex
	leader    balancer.SubConn
	followers []balancer.SubConn
	current   uint64
}

/*
Build - gRPC passes a map of
subconnections with information about those subconnections to Build()
to set up the picker—behind the scenes, gRPC connected to the addresses that our
resolver.go discovered.

Picker will route "consume" RPCs to follower servers
and "produce" RPCs to the leader server.
*/
func (p *Picker) Build(buildInfo base.PickerBuildInfo) balancer.Picker {
	p.mu.Lock()
	defer p.mu.Unlock()

	var followers []balancer.SubConn

	// buildInfo.ReadySCs -> map[balancer.SubConn]base.SubConnInfo
	for conn, info := range buildInfo.ReadySCs {
		isLeader := info.Address.Attributes.Value("is_leader").(bool)
		if isLeader {
			// save the ref on the leaver server
			p.leader = conn
			continue
		}
		// append the conn to followers
		followers = append(followers, conn)
	}
	p.followers = followers
	return p
}

/*
Pick
grpc gives Pick() a balancer.PickInfo
containing the RPC’s name and context to help the picker know what subconnection to pick.

Though pickers handle routing the calls, which we’d traditionally consider
handling the balancing, gRPC has a balancer type that takes input from gRPC,
manages subconnections, and collects and aggregates connectivity states.

gRPC provides a base balancer; you probably don’t need to write your own.
*/
func (p *Picker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var result balancer.PickResult

	if strings.Contains(info.FullMethodName, "Produce") || len(p.followers) == 0 {
		// make request to the leader for write request
		result.SubConn = p.leader
	} else if strings.Contains(info.FullMethodName, "Consume") {
		// make request to the follower for read request (round-robin algo)
		result.SubConn = p.nextFollower()
	}

	if result.SubConn == nil {
		return result, balancer.ErrNoSubConnAvailable
	}

	return result, nil
}

func (p *Picker) nextFollower() balancer.SubConn {
	cur := atomic.AddUint64(&p.current, uint64(1))
	len := uint64(len(p.followers))
	idx := int(cur % len)
	return p.followers[idx]
}

func init() {
	balancer.Register(
		base.NewBalancerBuilder(Name, &Picker{}, base.Config{}),
	)
}
