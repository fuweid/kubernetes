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
	"errors"
	"sync"
	"testing"

	"google.golang.org/grpc"
)

func TestWithChannelKey(t *testing.T) {
	if key := ChannelKeyFromContext(context.Background()); key != "" {
		t.Fatalf("expected no channel key, got %q", key)
	}
	if key := ChannelKeyFromContext(WithChannelKey(context.Background(), "")); key != "" {
		t.Fatalf("expected empty channel key, got %q", key)
	}
	if key := ChannelKeyFromContext(WithChannelKey(context.Background(), "bulk")); key != "bulk" {
		t.Fatalf("expected channel key %q, got %q", "bulk", key)
	}
}

func TestReservedChannelKeysInConfig(t *testing.T) {
	for _, key := range []string{"", "default"} {
		_, err := New(Config{Endpoints: []string{"http://127.0.0.1:1"}, ChannelKeys: []string{key}})
		if !errors.Is(err, ErrReservedChannelKey) {
			t.Fatalf("expected %v for key %q, got %v", ErrReservedChannelKey, key, err)
		}
	}
}

func TestConnectionForContextSelectsChannelKey(t *testing.T) {
	defaultConn := &grpc.ClientConn{}
	bulkConn := &grpc.ClientConn{}
	c := &Client{
		ctx:  context.Background(),
		epMu: new(sync.RWMutex),
		channels: map[string]channel{
			defaultChannelKey: {conn: defaultConn},
			"bulk":            {conn: bulkConn},
		},
	}

	got, err := c.connectionForContext(context.Background())
	if err != nil {
		t.Fatalf("unexpected default connection error: %v", err)
	}
	if got != defaultConn {
		t.Fatalf("expected default connection %p, got %p", defaultConn, got)
	}

	got, err = c.connectionForContext(WithChannelKey(context.Background(), "bulk"))
	if err != nil {
		t.Fatalf("unexpected keyed connection error: %v", err)
	}
	if got != bulkConn {
		t.Fatalf("expected bulk connection %p, got %p", bulkConn, got)
	}

	_, err = c.connectionForContext(WithChannelKey(context.Background(), "missing"))
	if !errors.Is(err, ErrChannelKeyNotFound) {
		t.Fatalf("expected %v, got %v", ErrChannelKeyNotFound, err)
	}
}
