package loadbalance_test

import (
	"fmt"
	"github.com/denisschmidt/golog/internal/loadbalance"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/resolver"
	"testing"
)

func TestPickerNoSubConnAvailable(t *testing.T) {
	picker := &loadbalance.Picker{}

	for _, method := range []string{"/log.vX.Log/Produce", "/log.vX.Log/Consume"} {
		info := balancer.PickInfo{
			FullMethodName: method,
		}
		result, err := picker.Pick(info)
		// there are no available subconnections from resolver
		require.Equal(t, balancer.ErrNoSubConnAvailable, err)
		require.Nil(t, result.SubConn)
	}
}

func TestPickerProducesToLeader(t *testing.T) {
	picker, subConns := setupTest()
	info := balancer.PickInfo{
		FullMethodName: "/log.vX.Log/Produce",
	}

	for i := 0; i < 5; i++ {
		gotPick, err := picker.Pick(info)
		require.NoError(t, err)
		require.Equal(t, subConns[0], gotPick.SubConn)
	}

}

func TestPickerConsumesFromFollowers(t *testing.T) {
	picker, subConns := setupTest()
	info := balancer.PickInfo{
		FullMethodName: "/log.vX.Log/Consume",
	}
	for i := 1; i <= 5; i++ {
		pick, err := picker.Pick(info)
		require.NoError(t, err)
		fmt.Println(pick, subConns)
		require.Equal(t, subConns[i%2+1], pick.SubConn)
	}
}

func setupTest() (*loadbalance.Picker, []*subConn) {
	var subConns []*subConn

	buildInfo := base.PickerBuildInfo{
		ReadySCs: make(map[balancer.SubConn]base.SubConnInfo),
	}

	for i := 0; i < 3; i++ {
		sc := &subConn{}
		addr := resolver.Address{
			ServerName: fmt.Sprintf("test server %d", i),
			Attributes: attributes.New("is_leader", i == 0),
		}
		sc.UpdateAddresses([]resolver.Address{addr})
		buildInfo.ReadySCs[sc] = base.SubConnInfo{Address: addr}
		subConns = append(subConns, sc)
	}

	picker := &loadbalance.Picker{}
	picker.Build(buildInfo)
	return picker, subConns
}

type subConn struct {
	addrs []resolver.Address
}

func (s *subConn) Connect() {}

func (s *subConn) UpdateAddresses(addrs []resolver.Address) {
	s.addrs = addrs
}
