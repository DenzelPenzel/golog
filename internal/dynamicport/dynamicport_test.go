package dynamicport

import (
	"net"
	"testing"
)

func TestGet(t *testing.T) {
	n := 3
	ports, err := GetWithErr(n)

	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if len(ports) != n {
		t.Fatal("expected to get 3 ports")
	}

	for _, port := range ports {
		ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: port})

		if err != nil {
			t.Fatal("expected port to be free")
		}

		ln.Close()
	}
}
