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
// A 2xx response with body in {true,1,yes,y,ok} (case-insensitive, trimmed)
// → Allowed=true. Anything else (non-2xx, parse failure, transport error,
// timeout, unreachable) → Result.Err is set and Allowed stays false.
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
	res.Allowed = parseBool(body)
	return res
}

func parseBool(b []byte) bool {
	switch strings.ToLower(strings.TrimSpace(string(b))) {
	case "true", "1", "yes", "y", "ok":
		return true
	}
	return false
}
