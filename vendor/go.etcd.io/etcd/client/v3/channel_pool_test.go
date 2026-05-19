// Copyright 2026 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package clientv3

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/peer"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func TestChannelPoolCreatesConnectionPerEndpoint(t *testing.T) {
	const endpointCount = 3
	servers := make([]*trackedKVServer, 0, endpointCount)
	endpoints := make([]string, 0, endpointCount)
	for i := 0; i < endpointCount; i++ {
		s := newTrackedKVServer(t)
		servers = append(servers, s)
		endpoints = append(endpoints, "http://"+s.addr())
	}

	c, err := New(Config{
		Endpoints:   endpoints,
		DialTimeout: 30 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithBlock(),
		},
		ChannelKeys: []string{"bulk"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	requireChannelConnections(t, servers, 6)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := 0; i < endpointCount; i++ {
		if _, err := c.Get(ctx, fmt.Sprintf("default-%d", i)); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Get(WithChannelKey(ctx, "bulk"), fmt.Sprintf("bulk-%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	for _, s := range servers {
		defaultPeer, ok := s.peerAddr("default")
		if !ok {
			t.Fatalf("server %s did not receive default request", s.addr())
		}
		bulkPeer, ok := s.peerAddr("bulk")
		if !ok {
			t.Fatalf("server %s did not receive bulk request", s.addr())
		}
		if defaultPeer == bulkPeer {
			t.Fatalf("expected default and bulk requests to use different connections, got %q", defaultPeer)
		}
	}
}

func requireChannelConnections(t *testing.T, servers []*trackedKVServer, totalConnections uint64) {
	t.Helper()

	deadline := time.After(30 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var total uint64
		allReady := true
		for _, s := range servers {
			accepted := s.accepted()
			total += accepted
			if accepted < 2 {
				allReady = false
			}
		}
		if allReady && total >= totalConnections {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for at least %d channel connections, got %d", totalConnections, total)
		case <-ticker.C:
		}
	}
}

type trackedKVServer struct {
	etcdserverpb.UnimplementedKVServer

	listener *trackedListener
	server   *grpc.Server
	donec    chan error
	mu       sync.Mutex
	peers    map[string]string
}

func newTrackedKVServer(t *testing.T) *trackedKVServer {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	trackedLn := &trackedListener{Listener: ln}

	s := &trackedKVServer{
		listener: trackedLn,
		server:   grpc.NewServer(),
		donec:    make(chan error, 1),
		peers:    make(map[string]string),
	}
	healthpb.RegisterHealthServer(s.server, health.NewServer())
	etcdserverpb.RegisterKVServer(s.server, s)
	go func() {
		s.donec <- s.server.Serve(trackedLn)
	}()
	t.Cleanup(func() {
		s.server.Stop()
		<-s.donec
	})
	return s
}

func (s *trackedKVServer) Range(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	p, ok := peer.FromContext(ctx)
	if ok {
		keyPrefix, _, _ := strings.Cut(string(req.Key), "-")
		if keyPrefix == "default" || keyPrefix == "bulk" {
			s.mu.Lock()
			s.peers[keyPrefix] = p.Addr.String()
			s.mu.Unlock()
		}
	}
	return &etcdserverpb.RangeResponse{Header: &etcdserverpb.ResponseHeader{Revision: 1}}, nil
}

func (s *trackedKVServer) addr() string {
	return s.listener.Addr().String()
}

func (s *trackedKVServer) accepted() uint64 {
	return s.listener.accepted.Load()
}

func (s *trackedKVServer) peerAddr(keyPrefix string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	peer, ok := s.peers[keyPrefix]
	return peer, ok
}

type trackedListener struct {
	net.Listener
	accepted atomic.Uint64
}

func (l *trackedListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil {
		l.accepted.Add(1)
	}
	return conn, err
}
