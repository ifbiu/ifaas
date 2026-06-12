/*
Copyright 2026.

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

package scaledown

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
)

// HTTPProber issues a GET against pods/proxy and parses the response body as
// the workload's "ok to scale to zero" flag.
//
// Wire format:
//
//	GET /api/v1/namespaces/<ns>/pods/<pod>:<port>/proxy/<path>
//
// The response body is decoded in two compatible shapes (in priority order):
//
//  1. JSON object per ckbackup-scaledownz-design.md §"`/scaledownz` 协议":
//     `{"allowScaleDown": <bool>, "inFlight": <int>}` — `.allowScaleDown`
//     drives the result. This is the canonical contract for new workloads.
//  2. Plain text, trimmed and lowercased, in {"true","1","yes","y","ok"} —
//     accepted as a legacy / stub fallback so existing simple endpoints keep
//     working without a redeploy.
//
// Anything else (non-2xx, parse failure on neither shape, transport error,
// timeout, unreachable) → Result.Err is set when transport-level and Allowed
// stays false otherwise.
type HTTPProber struct {
	rc rest.Interface
}

// NewHTTPProber returns a Prober that reuses the operator's rest.Config so it
// participates in normal apiserver auth, rate limiting, and TLS settings.
func NewHTTPProber(cfg *rest.Config) (*HTTPProber, error) {
	c, err := corev1client.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("scaledown: build core client: %w", err)
	}
	return &HTTPProber{rc: c.RESTClient()}, nil
}

// Probe implements Prober. The supplied timeout, when > 0, is enforced via a
// child context; the original ctx is honoured otherwise.
func (p *HTTPProber) Probe(ctx context.Context, namespace, podName string, port int32, path string, timeout time.Duration) Result {
	res := Result{PodName: podName}
	if port <= 0 {
		res.Err = ErrInvalidPort
		return res
	}
	suffix := strings.TrimPrefix(path, "/")

	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	started := time.Now()
	body, err := p.rc.Get().
		Namespace(namespace).
		Resource("pods").
		Name(podName + ":" + strconv.Itoa(int(port))).
		SubResource("proxy").
		Suffix(suffix).
		DoRaw(ctx)
	res.Latency = time.Since(started)
	if err != nil {
		res.Err = err
		return res
	}
	res.Allowed = parseAllowScaleDown(body)
	return res
}

// parseAllowScaleDown extracts the boolean verdict from the /scaledownz
// response body. JSON object form takes priority; plain-text form is kept
// as a backwards-compatible fallback for stubs and minimal endpoints.
func parseAllowScaleDown(b []byte) bool {
	s := strings.TrimSpace(string(b))
	if s == "" {
		return false
	}
	if s[0] == '{' {
		var resp struct {
			AllowScaleDown *bool `json:"allowScaleDown"`
		}
		if err := json.Unmarshal([]byte(s), &resp); err == nil && resp.AllowScaleDown != nil {
			return *resp.AllowScaleDown
		}
		return false
	}
	switch strings.ToLower(s) {
	case "true", "1", "yes", "y", "ok":
		return true
	}
	return false
}
