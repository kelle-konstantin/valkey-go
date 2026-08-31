package valkey

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

func TestPickLowestLatencyReplica(t *testing.T) {
	l1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen l1: %v", err)
	}
	defer l1.Close()

	l2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen l2: %v", err)
	}
	defer l2.Close()

	l3, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen l3: %v", err)
	}
	defer l3.Close()

	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	serveMock := func(l net.Listener, delay time.Duration) {
		defer wg.Done()
		for {
			conn, err := l.Accept()
			if err != nil {
				select {
				case <-stopCh:
					return
				default:
					return
				}
			}
			go func(c net.Conn) {
				defer c.Close()
				var buf [32]byte
				_, _ = c.Read(buf[:])
				if delay > 0 {
					time.Sleep(delay)
				}
				_, _ = c.Write([]byte("+PONG\r\n"))
			}(conn)
		}
	}

	wg.Add(3)
	go serveMock(l1, 60*time.Millisecond)
	go serveMock(l2, 0) // fast node (simulates local node)
	go serveMock(l3, 30*time.Millisecond)

	defer func() {
		close(stopCh)
		l1.Close()
		l2.Close()
		l3.Close()
		wg.Wait()
	}()

	host1, port1, _ := net.SplitHostPort(l1.Addr().String())
	host2, port2, _ := net.SplitHostPort(l2.Addr().String())
	host3, port3, _ := net.SplitHostPort(l3.Addr().String())

	eligible := []map[string]string{
		{"ip": host1, "port": port1},
		{"ip": host2, "port": port2},
		{"ip": host3, "port": port3},
	}

	client := &sentinelClient{
		mOpt: &ClientOption{
			RouteByLatency: true,
			Dialer: net.Dialer{
				Timeout: 200 * time.Millisecond,
			},
		},
	}

	for i := 0; i < 5; i++ {
		best, err := client.pickLowestLatencyReplica(eligible)
		if err != nil {
			t.Fatalf("iteration %d: pickLowestLatencyReplica failed: %v", i, err)
		}

		expectedAddr := fmt.Sprintf("%s:%s", host2, port2)
		if best != expectedAddr {
			t.Fatalf("iteration %d: expected lowest latency node %s, got %s", i, expectedAddr, best)
		}
	}
}

func TestPickLowestLatencyReplica_WithUnreachableNodes(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer l.Close()

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			var buf [32]byte
			_, _ = conn.Read(buf[:])
			_, _ = conn.Write([]byte("+PONG\r\n"))
			_ = conn.Close()
		}
	}()

	validHost, validPort, _ := net.SplitHostPort(l.Addr().String())

	eligible := []map[string]string{
		{"ip": "127.0.0.1", "port": "59991"}, // unreachable
		{"ip": validHost, "port": validPort},   // reachable
		{"ip": "127.0.0.1", "port": "59992"}, // unreachable
	}

	client := &sentinelClient{
		mOpt: &ClientOption{
			RouteByLatency: true,
			Dialer: net.Dialer{
				Timeout: 100 * time.Millisecond,
			},
		},
	}

	best, err := client.pickLowestLatencyReplica(eligible)
	if err != nil {
		t.Fatalf("pickLowestLatencyReplica failed with unreachable nodes: %v", err)
	}

	expected := fmt.Sprintf("%s:%s", validHost, validPort)
	if best != expected {
		t.Fatalf("expected reachable node %s, got %s", expected, best)
	}
}

func TestSentinelAutoEnableSendToReplicas(t *testing.T) {
	opt := &ClientOption{
		InitAddress:    []string{"127.0.0.1:26379"},
		Sentinel:       SentinelOption{MasterSet: "mymaster"},
		RouteByLatency: true,
	}

	if opt.RouteByLatency && opt.SendToReplicas == nil && !opt.ReplicaOnly {
		opt.SendToReplicas = func(cmd Completed) bool { return true }
	}

	if opt.SendToReplicas == nil {
		t.Fatal("expected SendToReplicas to be auto-configured")
	}

	if !opt.SendToReplicas(Completed{}) {
		t.Fatal("expected SendToReplicas to return true for commands")
	}
}
