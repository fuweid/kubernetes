/*
Copyright 2022 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package factory

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/kubernetes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/peer"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/etcd3/testserver"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
)

type mockKV struct {
	get func(ctx context.Context) (*clientv3.GetResponse, error)
}

func TestEtcdChannelKeysForStorage(t *testing.T) {
	if got := etcdChannelKeysForStorage(); got != nil {
		t.Fatalf("expected no storage channel keys by default, got %v", got)
	}

	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.SeparateCacheDelegateListEtcdChannel, true)
	got := etcdChannelKeysForStorage()
	if len(got) != 1 || got[0] != storage.DelegateListStorageChannelKey {
		t.Fatalf("expected delegate-list storage channel key, got %v", got)
	}
}

func TestNewETCD3ClientWithDelegateListChannelCreatesDedicatedConnection(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.SeparateCacheDelegateListEtcdChannel, true)

	s := newTrackedFactoryKVServer(t)
	client, err := newETCD3ClientWithChannelKeys(storagebackend.TransportConfig{
		ServerList: []string{"http://" + s.addr()},
	}, etcdChannelKeysForStorage()...)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	requireFactoryChannelConnections(t, s, 2)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := client.Client.KV.Get(ctx, "default-key"); err != nil {
		t.Fatalf("default Get failed: %v", err)
	}
	if _, err := client.Client.KV.Get(clientv3.WithChannelKey(ctx, storage.DelegateListStorageChannelKey), "delegate-list-key"); err != nil {
		t.Fatalf("delegate-list Get failed: %v", err)
	}

	defaultPeer, ok := s.peerAddr("default")
	if !ok {
		t.Fatalf("server %s did not receive default request", s.addr())
	}
	delegateListPeer, ok := s.peerAddr("delegate-list")
	if !ok {
		t.Fatalf("server %s did not receive delegate-list request", s.addr())
	}
	if defaultPeer == delegateListPeer {
		t.Fatalf("expected default and delegate-list requests to use different connections, got %q", defaultPeer)
	}
}

func (mkv mockKV) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	return nil, nil
}
func (mkv mockKV) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return mkv.get(ctx)
}
func (mockKV) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	return nil, nil
}
func (mockKV) Compact(ctx context.Context, rev int64, opts ...clientv3.CompactOption) (*clientv3.CompactResponse, error) {
	return nil, nil
}
func (mockKV) Do(ctx context.Context, op clientv3.Op) (clientv3.OpResponse, error) {
	return clientv3.OpResponse{}, nil
}
func (mockKV) Txn(ctx context.Context) clientv3.Txn {
	return nil
}

func TestCreateHealthcheck(t *testing.T) {
	etcdConfig := testserver.NewTestConfig(t)
	client := testserver.RunEtcd(t, etcdConfig)
	newETCD3ClientFn := newETCD3Client
	defer func() {
		newETCD3Client = newETCD3ClientFn
	}()
	tests := []struct {
		name         string
		cfg          storagebackend.Config
		want         error
		responseTime time.Duration
	}{
		{
			name: "ok if response time lower than default timeout",
			cfg: storagebackend.Config{
				Type:      storagebackend.StorageTypeETCD3,
				Transport: storagebackend.TransportConfig{},
			},
			responseTime: 1 * time.Second,
			want:         nil,
		},
		{
			name: "ok if response time lower than custom timeout",
			cfg: storagebackend.Config{
				Type:               storagebackend.StorageTypeETCD3,
				Transport:          storagebackend.TransportConfig{},
				HealthcheckTimeout: 5 * time.Second,
			},
			responseTime: 3 * time.Second,
			want:         nil,
		},
		{
			name: "timeouts if response time higher than default timeout",
			cfg: storagebackend.Config{
				Type:      storagebackend.StorageTypeETCD3,
				Transport: storagebackend.TransportConfig{},
			},
			responseTime: 3 * time.Second,
			want:         context.DeadlineExceeded,
		},
		{
			name: "timeouts if response time higher than custom timeout",
			cfg: storagebackend.Config{
				Type:               storagebackend.StorageTypeETCD3,
				Transport:          storagebackend.TransportConfig{},
				HealthcheckTimeout: 3 * time.Second,
			},
			responseTime: 5 * time.Second,
			want:         context.DeadlineExceeded,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ready := make(chan struct{})
			tc.cfg.Transport.ServerList = client.Endpoints()
			newETCD3Client = func(c storagebackend.TransportConfig) (*kubernetes.Client, error) {
				defer close(ready)
				dummyKV := mockKV{
					get: func(ctx context.Context) (*clientv3.GetResponse, error) {
						select {
						case <-ctx.Done():
							return nil, ctx.Err()
						case <-time.After(tc.responseTime):
							return nil, nil
						}
					},
				}
				client.KV = dummyKV
				return client, nil
			}
			stop := make(chan struct{})
			defer close(stop)

			healthcheck, err := CreateHealthCheck(tc.cfg, stop)
			if err != nil {
				t.Fatal(err)
			}
			// Wait for healthcheck to establish connection
			<-ready
			got := healthcheck()

			if !errors.Is(got, tc.want) {
				t.Errorf("healthcheck() missmatch want %v got %v", tc.want, got)
			}
		})
	}
}

func TestCreateReadycheck(t *testing.T) {
	etcdConfig := testserver.NewTestConfig(t)
	client := testserver.RunEtcd(t, etcdConfig)
	newETCD3ClientFn := newETCD3Client
	defer func() {
		newETCD3Client = newETCD3ClientFn
	}()
	tests := []struct {
		name         string
		cfg          storagebackend.Config
		want         error
		responseTime time.Duration
	}{
		{
			name: "ok if response time lower than default timeout",
			cfg: storagebackend.Config{
				Type:      storagebackend.StorageTypeETCD3,
				Transport: storagebackend.TransportConfig{},
			},
			responseTime: 1 * time.Second,
			want:         nil,
		},
		{
			name: "ok if response time lower than custom timeout",
			cfg: storagebackend.Config{
				Type:              storagebackend.StorageTypeETCD3,
				Transport:         storagebackend.TransportConfig{},
				ReadycheckTimeout: 5 * time.Second,
			},
			responseTime: 3 * time.Second,
			want:         nil,
		},
		{
			name: "timeouts if response time higher than default timeout",
			cfg: storagebackend.Config{
				Type:      storagebackend.StorageTypeETCD3,
				Transport: storagebackend.TransportConfig{},
			},
			responseTime: 3 * time.Second,
			want:         context.DeadlineExceeded,
		},
		{
			name: "timeouts if response time higher than custom timeout",
			cfg: storagebackend.Config{
				Type:              storagebackend.StorageTypeETCD3,
				Transport:         storagebackend.TransportConfig{},
				ReadycheckTimeout: 3 * time.Second,
			},
			responseTime: 5 * time.Second,
			want:         context.DeadlineExceeded,
		},
		{
			name: "timeouts if response time higher than default timeout with custom healthcheck timeout",
			cfg: storagebackend.Config{
				Type:               storagebackend.StorageTypeETCD3,
				Transport:          storagebackend.TransportConfig{},
				HealthcheckTimeout: 10 * time.Second,
			},
			responseTime: 3 * time.Second,
			want:         context.DeadlineExceeded,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ready := make(chan struct{})
			tc.cfg.Transport.ServerList = client.Endpoints()
			newETCD3Client = func(c storagebackend.TransportConfig) (*kubernetes.Client, error) {
				defer close(ready)
				dummyKV := mockKV{
					get: func(ctx context.Context) (*clientv3.GetResponse, error) {
						select {
						case <-ctx.Done():
							return nil, ctx.Err()
						case <-time.After(tc.responseTime):
							return nil, nil
						}
					},
				}
				client.KV = dummyKV
				return client, nil
			}
			stop := make(chan struct{})
			defer close(stop)

			healthcheck, err := CreateReadyCheck(tc.cfg, stop)
			if err != nil {
				t.Fatal(err)
			}
			// Wait for healthcheck to establish connection
			<-ready

			got := healthcheck()

			if !errors.Is(got, tc.want) {
				t.Errorf("healthcheck() missmatch want %v got %v", tc.want, got)
			}
		})
	}
}

func TestRateLimitHealthcheck(t *testing.T) {
	etcdConfig := testserver.NewTestConfig(t)
	client := testserver.RunEtcd(t, etcdConfig)
	newETCD3ClientFn := newETCD3Client
	defer func() {
		newETCD3Client = newETCD3ClientFn
	}()

	cfg := storagebackend.Config{
		Type:               storagebackend.StorageTypeETCD3,
		Transport:          storagebackend.TransportConfig{},
		HealthcheckTimeout: 5 * time.Second,
	}
	cfg.Transport.ServerList = client.Endpoints()
	tests := []struct {
		name string
		want error
	}{
		{
			name: "etcd ok",
		},
		{
			name: "etcd down",
			want: errors.New("etcd down"),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {

			ready := make(chan struct{})

			var counter uint64
			newETCD3Client = func(c storagebackend.TransportConfig) (*kubernetes.Client, error) {
				defer close(ready)
				dummyKV := mockKV{
					get: func(ctx context.Context) (*clientv3.GetResponse, error) {
						atomic.AddUint64(&counter, 1)
						select {
						case <-ctx.Done():
							return nil, ctx.Err()
						default:
							return nil, tc.want
						}
					},
				}
				client.KV = dummyKV
				return client, nil
			}

			stop := make(chan struct{})
			defer close(stop)
			healthcheck, err := CreateHealthCheck(cfg, stop)
			if err != nil {
				t.Fatal(err)
			}
			// Wait for healthcheck to establish connection
			<-ready
			// run a first request to obtain the state
			err = healthcheck()
			if !errors.Is(err, tc.want) {
				t.Errorf("healthcheck() mismatch want %v got %v", tc.want, err)
			}

			// run multiple request in parallel, they should have the same state that the first one
			var wg sync.WaitGroup
			for i := 0; i < 100; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					err := healthcheck()
					if !errors.Is(err, tc.want) {
						t.Errorf("healthcheck() mismatch want %v got %v", tc.want, err)
					}

				}()
			}

			// check the counter once the requests have finished
			wg.Wait()
			if counter != 1 {
				t.Errorf("healthcheck() called etcd %d times, expected only one call", counter)
			}

			// wait until the rate limit allows new connections
			time.Sleep(cfg.HealthcheckTimeout / 2)

			// a new run on request should increment the counter only once
			// run multiple request in parallel, they should have the same state that the first one
			for i := 0; i < 100; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					err := healthcheck()
					if !errors.Is(err, tc.want) {
						t.Errorf("healthcheck() mismatch want %v got %v", tc.want, err)
					}

				}()
			}
			wg.Wait()

			if counter != 2 {
				t.Errorf("healthcheck() called etcd %d times, expected only two calls", counter)
			}
		})
	}

}

func TestTimeTravelHealthcheck(t *testing.T) {
	etcdConfig := testserver.NewTestConfig(t)
	client := testserver.RunEtcd(t, etcdConfig)
	newETCD3ClientFn := newETCD3Client
	defer func() {
		newETCD3Client = newETCD3ClientFn
	}()

	cfg := storagebackend.Config{
		Type:               storagebackend.StorageTypeETCD3,
		Transport:          storagebackend.TransportConfig{},
		HealthcheckTimeout: 5 * time.Second,
	}
	cfg.Transport.ServerList = client.Endpoints()

	ready := make(chan struct{})
	signal := make(chan struct{})

	var counter uint64
	newETCD3Client = func(c storagebackend.TransportConfig) (*kubernetes.Client, error) {
		defer close(ready)
		dummyKV := mockKV{
			get: func(ctx context.Context) (*clientv3.GetResponse, error) {
				atomic.AddUint64(&counter, 1)
				val := atomic.LoadUint64(&counter)
				// the first request wait for a custom timeout to trigger an error.
				// We don't use the context timeout because we want to check that
				// the cached answer is not overridden, and since the rate limit is
				// based on cfg.HealthcheckTimeout / 2, the timeout will race with
				// the race limiter to server the new request from the cache or allow
				// it to go through
				if val == 1 {
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-time.After((2 * cfg.HealthcheckTimeout) / 3):
						return nil, fmt.Errorf("etcd down")
					}
				}
				// subsequent requests will always work
				return nil, nil
			},
		}
		client.KV = dummyKV
		return client, nil
	}

	stop := make(chan struct{})
	defer close(stop)
	healthcheck, err := CreateHealthCheck(cfg, stop)
	if err != nil {
		t.Fatal(err)
	}
	// Wait for healthcheck to establish connection
	<-ready
	// run a first request that fails after 2 seconds
	go func() {
		err := healthcheck()
		if !strings.Contains(err.Error(), "etcd down") {
			t.Errorf("healthcheck() mismatch want %v got %v", fmt.Errorf("etcd down"), err)
		}
		close(signal)
	}()

	// wait until the rate limit allows new connections
	time.Sleep(cfg.HealthcheckTimeout / 2)

	select {
	case <-signal:
		t.Errorf("first request should not return yet")
	default:
	}

	// a new run on request should succeed and increment the counter
	err = healthcheck()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	c := atomic.LoadUint64(&counter)
	if c != 2 {
		t.Errorf("healthcheck() called etcd %d times, expected only two calls", c)
	}

	// cached request should be success and not be overridden by the late error
	<-signal
	err = healthcheck()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	c = atomic.LoadUint64(&counter)
	if c != 2 {
		t.Errorf("healthcheck() called etcd %d times, expected only two calls", c)
	}

}

func requireFactoryChannelConnections(t *testing.T, s *trackedFactoryKVServer, totalConnections uint64) {
	t.Helper()

	deadline := time.After(30 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if accepted := s.accepted(); accepted >= totalConnections {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for at least %d channel connections, got %d", totalConnections, s.accepted())
		case <-ticker.C:
		}
	}
}

type trackedFactoryKVServer struct {
	etcdserverpb.UnimplementedKVServer

	listener *trackedFactoryListener
	server   *grpc.Server
	donec    chan error
	mu       sync.Mutex
	peers    map[string]string
}

func newTrackedFactoryKVServer(t *testing.T) *trackedFactoryKVServer {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	trackedLn := &trackedFactoryListener{Listener: ln}

	s := &trackedFactoryKVServer{
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

func (s *trackedFactoryKVServer) Range(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	if p, ok := peer.FromContext(ctx); ok {
		keyPrefix := ""
		switch key := string(req.Key); {
		case strings.HasPrefix(key, "default-"):
			keyPrefix = "default"
		case strings.HasPrefix(key, "delegate-list-"):
			keyPrefix = "delegate-list"
		}
		if keyPrefix != "" {
			s.mu.Lock()
			s.peers[keyPrefix] = p.Addr.String()
			s.mu.Unlock()
		}
	}
	return &etcdserverpb.RangeResponse{Header: &etcdserverpb.ResponseHeader{Revision: 1}}, nil
}

func (s *trackedFactoryKVServer) addr() string {
	return s.listener.Addr().String()
}

func (s *trackedFactoryKVServer) accepted() uint64 {
	return s.listener.accepted.Load()
}

func (s *trackedFactoryKVServer) peerAddr(keyPrefix string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	peer, ok := s.peers[keyPrefix]
	return peer, ok
}

type trackedFactoryListener struct {
	net.Listener
	accepted atomic.Uint64
}

func (l *trackedFactoryListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil {
		l.accepted.Add(1)
	}
	return conn, err
}
