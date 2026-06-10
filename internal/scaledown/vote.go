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

// Package scaledown holds the pure decision rules for the /scaledownz guard
// described in docs/knative-autopilot-design.md §8.5 and impl-plan §S6.
//
// The package has zero dependencies on controller-runtime, client-go beyond
// rest, or kubebuilder types: it is reused by both the per-CR guard pass in
// the reconciler (S6) and the namespace-level flusher (S7), and is exercised
// by table-driven unit tests.
package scaledown

import (
	"context"
	"errors"
	"time"
)

// Result captures the outcome of probing a single pod.
//
// Allowed is meaningful only when Err == nil. The pure rules below treat
// Err != nil as "unable to confirm scale-to-zero is safe" and therefore as
// a vote against scaling down.
type Result struct {
	PodName string
	Allowed bool
	Err     error
	Latency time.Duration
}

// Outcome aggregates a slice of Results into the action the caller should take.
type Outcome string

const (
	// OutcomeAllowZero: every probed pod returned Allowed=true with no error.
	// Operator may hold min-scale=0.
	OutcomeAllowZero Outcome = "AllowZero"

	// OutcomeBlock: at least one pod refused, errored, or timed out.
	// Operator must pin min-scale=1 until the next round flips it back.
	OutcomeBlock Outcome = "Block"

	// OutcomeNoPods: there were no pods to probe (e.g. KSvc has already
	// scaled to zero, or the latest revision has not produced any pods yet).
	// The caller should skip the patch and not treat the round as a failure.
	OutcomeNoPods Outcome = "NoPods"
)

// Vote is the pure decision rule. Order of arguments does not matter.
//
//   - len(results) == 0           → OutcomeNoPods
//   - any Err != nil              → OutcomeBlock
//   - any Allowed == false        → OutcomeBlock
//   - all Allowed == true, no err → OutcomeAllowZero
func Vote(results []Result) Outcome {
	if len(results) == 0 {
		return OutcomeNoPods
	}
	for _, r := range results {
		if r.Err != nil || !r.Allowed {
			return OutcomeBlock
		}
	}
	return OutcomeAllowZero
}

// Tally classifies results into ok / refused / errored buckets so callers can
// emit metrics or build human-readable status messages without re-walking the
// slice multiple times.
func Tally(results []Result) (ok, refused, errored int) {
	for _, r := range results {
		switch {
		case r.Err != nil:
			errored++
		case r.Allowed:
			ok++
		default:
			refused++
		}
	}
	return
}

// HasErrors reports whether any single probe failed at the transport layer.
// It is equivalent to `errored > 0` from Tally and is exposed as a helper for
// the consecutive-error counter that drives the Degraded condition.
func HasErrors(results []Result) bool {
	for _, r := range results {
		if r.Err != nil {
			return true
		}
	}
	return false
}

// Prober is the runtime side of the guard. It is exposed as an interface so
// the reconciler can inject a fake in unit/envtests, while production wires
// up an HTTPProber against pods/proxy.
//
// Implementations must:
//   - honour the supplied context (timeouts, cancellation)
//   - never panic on transport failures; surface them as Result.Err
//   - leave Allowed at its zero value when Err is non-nil
type Prober interface {
	Probe(ctx context.Context, namespace, podName string, port int32, path string, timeout time.Duration) Result
}

// ErrInvalidPort is returned by HTTPProber when the caller did not pass a
// non-zero port. It is exposed so tests can match against it explicitly.
var ErrInvalidPort = errors.New("scaledown: probe port must be > 0")
