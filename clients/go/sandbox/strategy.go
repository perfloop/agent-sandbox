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
	"fmt"
)

// ConnectionStrategy defines how the SDK discovers the sandbox-router URL.
type ConnectionStrategy interface {
	Connect(ctx context.Context) (baseURL string, err error)
	Close() error
}

// DirectStrategy connects using a pre-configured URL, bypassing all discovery.
type DirectStrategy struct {
	URL string
}

func (s *DirectStrategy) Connect(_ context.Context) (string, error) { return s.URL, nil }
func (s *DirectStrategy) Close() error                              { return nil }

// inClusterStrategy connects directly to a sandbox pod via cluster DNS,
// bypassing the sandbox-router entirely. Mirrors the Python SDK's
// SandboxInClusterConnectionConfig (kubernetes-sigs/agent-sandbox PR
// #489) and is the natural connectivity for in-cluster callers — a
// controller pod, a smoke Job, anything that resolves cluster DNS.
//
// The URL pattern is http://{sandbox-name}.{namespace}.svc.cluster.local:{port}.
// Sandbox name is read from the connector at Connect time because the
// SDK's Open flow resolves it from the SandboxClaim status before
// calling Connect; the strategy itself owns no naming logic.
//
// Connector header suppression: when this strategy is active, the
// connector skips X-Sandbox-ID / X-Sandbox-Namespace / X-Sandbox-Port
// because requests go straight to the pod. Those headers are router-
// dispatch metadata; the runtime server does not consult them.
//
// Perfloop fork addition. Tracked separately from the upstream
// kubernetes-sigs/agent-sandbox#622 nested-paths fix so the two
// patches can be reviewed independently.
type inClusterStrategy struct {
	namespace  string
	serverPort int
	scheme     string
	// connector is set after construction so Connect can read the
	// sandbox identity SetIdentity placed there. Mirrors how
	// tunnelStrategy receives its connector reference.
	connector *connector
}

func (s *inClusterStrategy) Connect(_ context.Context) (string, error) {
	if s.connector == nil {
		return "", fmt.Errorf("sandbox: in-cluster strategy missing connector reference")
	}
	s.connector.mu.Lock()
	sandboxID := s.connector.sandboxID
	s.connector.mu.Unlock()
	if sandboxID == "" {
		return "", fmt.Errorf("sandbox: in-cluster strategy requires resolved sandbox name (Open must run before Connect)")
	}
	scheme := s.scheme
	if scheme == "" {
		scheme = "http"
	}
	return fmt.Sprintf("%s://%s.%s.svc.cluster.local:%d", scheme, sandboxID, s.namespace, s.serverPort), nil
}

func (s *inClusterStrategy) Close() error { return nil }
