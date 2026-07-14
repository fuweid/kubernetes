/*
Copyright 2024 The Kubernetes Authors.

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

package webhook

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/apis/apiserver"
	"k8s.io/apiserver/pkg/server/egressselector"
	"k8s.io/client-go/rest"
	certutil "k8s.io/client-go/util/cert"
	"k8s.io/component-base/tracing"
	netutils "k8s.io/utils/net"
)

func TestWebhookClientConfig(t *testing.T) {
	cm, _ := NewClientManager([]schema.GroupVersion{})
	authInfoResolver, err := NewDefaultAuthenticationInfoResolver("")
	if err != nil {
		t.Fatal(err)
	}
	cm.SetAuthenticationInfoResolver(authInfoResolver)
	cm.SetServiceResolver(NewDefaultServiceResolver())

	tests := []struct {
		name             string
		url              string
		expectAllowHTTP2 bool
	}{
		{
			name:             "force http1",
			url:              "https://webhook.example.com",
			expectAllowHTTP2: false,
		},
		{
			name:             "allow http2 for localhost",
			url:              "https://localhost",
			expectAllowHTTP2: true,
		},
		{
			name:             "allow http2 for 127.0.0.1",
			url:              "https://127.0.0.1",
			expectAllowHTTP2: true,
		},
		{
			name:             "allow http2 for [::1]:0",
			url:              "https://[::1]",
			expectAllowHTTP2: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {

			cc := ClientConfig{
				URL: tc.url,
			}
			cfg, err := cm.hookClientConfig(cc)
			if err != nil {
				t.Fatal(err)
			}
			if tc.expectAllowHTTP2 && !allowHTTP2(cfg.NextProtos) {
				t.Errorf("expected allow http/2, got: %v", cfg.NextProtos)
			}
		})
	}
}

func allowHTTP2(nextProtos []string) bool {
	if len(nextProtos) == 0 {
		// the transport expressed no NextProto preference, allow
		return true
	}
	for _, p := range nextProtos {
		if p == http2.NextProtoTLS {
			// the transport explicitly allowed http/2
			return true
		}
	}
	// the transport explicitly set NextProtos and excluded http/2
	return false
}

type fakeAuthInfoResolver struct{}

func (f *fakeAuthInfoResolver) ClientConfigFor(server string) (*rest.Config, error) {
	return &rest.Config{
		TLSClientConfig: rest.TLSClientConfig{
			ServerName: "example.com",
		},
	}, nil
}

func (f *fakeAuthInfoResolver) ClientConfigForService(serviceName, namespace string, port int) (*rest.Config, error) {
	return &rest.Config{
		TLSClientConfig: rest.TLSClientConfig{
			ServerName: "example.com",
		},
	}, nil
}

// fakeDynamicServiceResolver returns the next endpoint in the list for each request.
type fakeDynamicServiceResolver struct {
	endpoints []*url.URL
	counter   int32
}

func (f *fakeDynamicServiceResolver) ResolveEndpoint(namespace, name string, port int32) (*url.URL, error) {
	val := atomic.AddInt32(&f.counter, 1) - 1
	if val >= int32(len(f.endpoints)) {
		val = int32(len(f.endpoints)) - 1
	}
	return f.endpoints[val], nil
}

func TestWebhookClientHTTPConnectTimeout(t *testing.T) {
	var webhookHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("webhook response"))
	})

	timeout := 2 * time.Second
	waitCh := make(chan struct{})
	var wrapperProxyHandler httpHandlerWrapper = func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer close(waitCh)

			t.Logf("HTTP CONNECT proxy received request: %s %s", r.Method, r.URL)
			t.Log("Waiting for client cancellation...")

			select {
			case <-r.Context().Done():
				t.Logf("HTTP CONNECT proxy received client cancellation signal: %v", r.Context().Err())
			case <-time.After(timeout * 5):
				t.Fatal("proxy handler did not receive client cancellation after timeout")
			}
		})
	}

	proxy := newWebhookBasedonHTTPConnectProxy(t, webhookHandler, wrapperProxyHandler)
	es := newHTTPConnectEgressSelector(t, proxy.url, proxy.tlsConfig)

	cm, err := NewClientManager([]schema.GroupVersion{})
	if err != nil {
		t.Fatalf("failed to create webhook client manager: %v", err)
	}
	cm.SetAuthenticationInfoResolver(&fakeAuthInfoResolver{})
	cm.SetServiceResolver(NewDefaultServiceResolver())
	cm.SetAuthenticationInfoResolverWrapper(NewDefaultAuthenticationInfoResolverWrapper(
		nil,
		es,
		&rest.Config{},
		tracing.NewNoopTracerProvider(),
	))

	client, err := cm.HookClient(ClientConfig{
		Name:     "test-webhook",
		CABundle: proxy.caBundle,
		Service: &ClientConfigService{
			Name:      "webhook",
			Namespace: "default",
			Path:      "/mutate",
			Port:      443,
		},
	})
	if err != nil {
		t.Fatalf("failed to create webhook REST client: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	_, err = client.Post().Body([]byte("{}")).DoRaw(ctx)
	cancel()
	if err == nil {
		t.Fatalf("expected webhook request through HTTP CONNECT proxy to timeout, but it succeeded")
	}
	<-waitCh // wait for the proxy handler to finish
}

func TestWebhookClientHTTPConnect(t *testing.T) {
	var webhookCalls int32
	var webhookHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&webhookCalls, 1)

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("webhook response"))
	})

	var connectCalls int32
	var wrapperProxyHandler httpHandlerWrapper = func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&connectCalls, 1)
			h.ServeHTTP(w, r)
		})
	}

	proxy := newWebhookBasedonHTTPConnectProxy(t, webhookHandler, wrapperProxyHandler)
	es := newHTTPConnectEgressSelector(t, proxy.url, proxy.tlsConfig)

	cm, err := NewClientManager([]schema.GroupVersion{})
	if err != nil {
		t.Fatalf("failed to create webhook client manager: %v", err)
	}
	cm.SetAuthenticationInfoResolver(&fakeAuthInfoResolver{})
	cm.SetServiceResolver(NewDefaultServiceResolver())
	cm.SetAuthenticationInfoResolverWrapper(NewDefaultAuthenticationInfoResolverWrapper(
		nil,
		es,
		&rest.Config{},
		tracing.NewNoopTracerProvider(),
	))

	client, err := cm.HookClient(ClientConfig{
		Name:     "test-webhook",
		CABundle: proxy.caBundle,
		Service: &ClientConfigService{
			Name:      "webhook",
			Namespace: "default",
			Path:      "/mutate",
			Port:      443,
		},
	})
	if err != nil {
		t.Fatalf("failed to create webhook REST client: %v", err)
	}

	n := 4
	for i := range n {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		_, err := client.Post().Body([]byte("{}")).DoRaw(ctx)
		cancel()
		if err != nil {
			t.Fatalf("webhook request %d through HTTP CONNECT proxy failed: %v", i+1, err)
		}
	}

	if calls := atomic.LoadInt32(&webhookCalls); calls != int32(n) {
		t.Fatalf("expected webhook backend to receive %d requests, got %d", n, calls)
	}
	if calls := atomic.LoadInt32(&connectCalls); calls != int32(1) {
		t.Fatalf("expected HTTP CONNECT proxy to receive 1 CONNECT request, got %d", calls)
	}
}

type httpConnectProxy struct {
	url       string
	tlsConfig *apiserver.TLSConfig
	caBundle  []byte
}

type httpHandlerWrapper func(http.Handler) http.Handler

// newWebhookBasedonHTTPConnectProxy returns a new HTTP CONNECT proxy that
// forwards requests to the given webhookHandler.
func newWebhookBasedonHTTPConnectProxy(t *testing.T, webhookHandler http.Handler, wrapperProxyHandler httpHandlerWrapper) httpConnectProxy {
	t.Helper()

	certPEM, keyPEM, err := certutil.GenerateSelfSignedCertKey(
		"example.com",
		[]net.IP{netutils.ParseIPSloppy("127.0.0.1")},
		nil,
	)
	if err != nil {
		t.Fatalf("failed to generate proxy serving certificate: %v", err)
	}

	tempDir := t.TempDir()

	certPath := filepath.Join(tempDir, "proxy.crt")
	if err := os.WriteFile(certPath, certPEM, 0600); err != nil {
		t.Fatalf("failed to write proxy cert file: %v", err)
	}

	keyPath := filepath.Join(tempDir, "proxy.key")
	if err := os.WriteFile(keyPath, keyPEM, 0600); err != nil {
		t.Fatalf("failed to write proxy key file: %v", err)
	}

	proxyCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("failed to load proxy serving certificate: %v", err)
	}

	backendListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on webhook backend address: %v", err)
	}
	backendServer := &http.Server{Handler: webhookHandler}
	go func() {
		tlsListener := tls.NewListener(backendListener, &tls.Config{
			Certificates: []tls.Certificate{proxyCert},
		})
		_ = backendServer.Serve(tlsListener)
	}()

	proxyListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on proxy address: %v", err)
	}

	handler := newHTTPConnectProxyHandler(t, backendListener.Addr().String())
	if wrapperProxyHandler != nil {
		handler = wrapperProxyHandler(handler)
	}

	proxyServer := &http.Server{Handler: handler}
	go func() {
		tlsListener := tls.NewListener(proxyListener, &tls.Config{
			Certificates: []tls.Certificate{proxyCert},
		})
		_ = proxyServer.Serve(tlsListener)
	}()

	t.Cleanup(func() {
		_ = proxyServer.Close()
		_ = backendServer.Close()
	})

	return httpConnectProxy{
		url: "https://" + proxyListener.Addr().String(),
		tlsConfig: &apiserver.TLSConfig{
			CABundle:   certPath,
			ClientCert: certPath,
			ClientKey:  keyPath,
		},
		caBundle: certPEM,
	}
}

// newHTTPConnectProxyHandler returns an HTTP handler that implements a simple
// HTTP CONNECT proxy.
func newHTTPConnectProxyHandler(t *testing.T, backendAddress string) http.Handler {
	t.Helper()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			http.Error(w, "CONNECT required", http.StatusMethodNotAllowed)
			return
		}

		backendConn, err := net.Dial("tcp", backendAddress)
		if err != nil {
			http.Error(w, "failed to connect webhook backend", http.StatusBadGateway)
			return
		}
		defer func() {
			_ = backendConn.Close()
		}()

		hijacker, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "response writer does not support hijacking", http.StatusInternalServerError)
			return
		}

		clientConn, _, err := hijacker.Hijack()
		if err != nil {
			t.Logf("failed to hijack HTTP CONNECT client connection: %v", err)
			return
		}
		defer func() {
			_ = clientConn.Close()
		}()

		if _, err := clientConn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n")); err != nil {
			t.Logf("failed to write HTTP CONNECT 200 response: %v", err)
			return
		}

		proxyConnections(clientConn, backendConn)
	})
}

func proxyConnections(clientConn, backendConn net.Conn) {
	var wg sync.WaitGroup
	var closeOnce sync.Once

	closeConns := func() {
		_ = clientConn.Close()
		_ = backendConn.Close()
	}

	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(clientConn, backendConn)
		closeOnce.Do(closeConns)
	}()
	go func() {
		defer wg.Done()
		_, _ = io.Copy(backendConn, clientConn)
		closeOnce.Do(closeConns)
	}()
	wg.Wait()
}

func newHTTPConnectEgressSelector(t *testing.T, proxyURL string, proxyTLSConfig *apiserver.TLSConfig) *egressselector.EgressSelector {
	es, err := egressselector.NewEgressSelector(&apiserver.EgressSelectorConfiguration{
		EgressSelections: []apiserver.EgressSelection{
			{
				Name: "cluster",
				Connection: apiserver.Connection{
					ProxyProtocol: apiserver.ProtocolHTTPConnect,
					Transport: &apiserver.Transport{
						TCP: &apiserver.TCPTransport{
							URL:       proxyURL,
							TLSConfig: proxyTLSConfig,
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to create egress selector: %v", err)
	}
	return es
}
