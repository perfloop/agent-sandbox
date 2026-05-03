// Copyright 2026 The Kubernetes Authors.
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

package sandbox

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// TestInClusterStrategy_URL pins the perfloop fork's in-cluster URL
// shape: http://{sandbox}.{namespace}.svc.cluster.local:{port}. The
// strategy resolves the sandbox name from the connector at Connect
// time (the SDK's Open flow calls SetIdentity before Connect), so
// this test sets the identity directly on a freshly built connector
// and asserts the resulting URL.
func TestInClusterStrategy_URL(t *testing.T) {
	conn := newConnector(connectorConfig{
		Strategy:   &DirectStrategy{URL: "http://placeholder"},
		Namespace:  "perfloop-sandbox",
		ServerPort: 8888,
	})
	conn.SetIdentity("session-abc123")

	strategy := &inClusterStrategy{
		namespace:  "perfloop-sandbox",
		serverPort: 8888,
		scheme:     "http",
		connector:  conn,
	}
	url, err := strategy.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	want := "http://session-abc123.perfloop-sandbox.svc.cluster.local:8888"
	if url != want {
		t.Errorf("URL = %q, want %q", url, want)
	}
}

// TestInClusterStrategy_RequiresIdentity pins that Connect refuses to
// build a URL before SetIdentity has resolved the sandbox name. A
// regression that swapped the order would silently produce a URL
// like "http://.{ns}.svc.cluster.local:8888" that DNS would refuse;
// failing fast at the strategy is friendlier than a downstream
// resolve error.
func TestInClusterStrategy_RequiresIdentity(t *testing.T) {
	conn := newConnector(connectorConfig{
		Strategy:   &DirectStrategy{URL: "http://placeholder"},
		Namespace:  "perfloop-sandbox",
		ServerPort: 8888,
	})
	// Note: no SetIdentity call.

	strategy := &inClusterStrategy{
		namespace:  "perfloop-sandbox",
		serverPort: 8888,
		scheme:     "http",
		connector:  conn,
	}
	if _, err := strategy.Connect(context.Background()); err == nil {
		t.Fatal("Connect with no identity returned nil error; want failure")
	}
}

// TestInClusterStrategy_SuppressesRouterHeaders pins the second half
// of PR #489's contract: when the in-cluster strategy is active the
// connector must not set X-Sandbox-ID, X-Sandbox-Namespace, or
// X-Sandbox-Port on outgoing requests. Those are router-dispatch
// metadata; the runtime server ignores them, and sending them risks
// confusion when traffic later flows through a router that does
// trust them. X-Request-ID stays for trace correlation.
func TestInClusterStrategy_SuppressesRouterHeaders(t *testing.T) {
	var captured http.Header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	conn := newConnector(connectorConfig{
		Strategy:          &DirectStrategy{URL: "http://placeholder"},
		Namespace:         "perfloop-sandbox",
		ServerPort:        8888,
		RequestTimeout:    5 * time.Second,
		PerAttemptTimeout: 5 * time.Second,
	})
	// Override the strategy AFTER newConnector wired up the placeholder
	// so the type-assertion in SendRequest sees *inClusterStrategy.
	conn.strategy = &inClusterStrategy{
		namespace:  "perfloop-sandbox",
		serverPort: 8888,
		scheme:     "http",
		connector:  conn,
	}
	conn.SetIdentity("session-abc123")
	conn.baseURL = server.URL

	resp, err := conn.SendRequest(context.Background(), http.MethodGet, "execute", nil, "", 0)
	if err != nil {
		t.Fatalf("SendRequest: %v", err)
	}
	resp.Body.Close()

	for _, suppressed := range []string{"X-Sandbox-Id", "X-Sandbox-Namespace", "X-Sandbox-Port"} {
		if v := captured.Get(suppressed); v != "" {
			t.Errorf("header %q must be suppressed for in-cluster mode, got %q", suppressed, v)
		}
	}
	if captured.Get("X-Request-Id") == "" {
		t.Error("X-Request-Id must still be set; trace correlation depends on it")
	}
}

// TestInClusterStrategy_NewSelectsStrategy pins that Options.
// InClusterDirect actually wires the new strategy. Without this the
// option could land but a code path could still pick tunnel mode by
// accident.
func TestInClusterStrategy_NewSelectsStrategy(t *testing.T) {
	opts := Options{
		TemplateName:    "test-template",
		Namespace:       "perfloop-sandbox",
		ServerPort:      8888,
		InClusterDirect: true,
		APIURL:          "http://placeholder", // higher-precedence value should be ignored
		Quiet:           true,
		K8sHelper:       &K8sHelper{},
	}
	sb, err := New(context.Background(), opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, ok := sb.connector.strategy.(*inClusterStrategy); !ok {
		t.Errorf("strategy = %T, want *inClusterStrategy", sb.connector.strategy)
	}

	// Sanity-check the URL the strategy would produce so a regression
	// in the percent-formatting (e.g., a stray %s) shows up here.
	sb.connector.SetIdentity("session-xyz")
	got, err := sb.connector.strategy.Connect(context.Background())
	if err != nil {
		t.Fatalf("strategy.Connect: %v", err)
	}
	if !strings.HasPrefix(got, "http://session-xyz.perfloop-sandbox.svc.cluster.local:") {
		if u, perr := url.Parse(got); perr == nil {
			t.Logf("(parsed: %+v)", u)
		}
		t.Errorf("URL prefix unexpected: %q", got)
	}
}
