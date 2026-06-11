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

// Package flusher implements the namespace-level coalescing queue described
// in docs/knative-autopilot-impl-plan.md §S7.
//
// Goals:
//   - Hide the per-pod /scaledownz vote rate from the apiserver: when the
//     guard re-evaluates a single KSvc many times within its debounce window,
//     only the latest decision should turn into a real PATCH.
//   - Treat min-scale=1 as the SLA-critical fast lane: enqueueing it triggers
//     an immediate flush instead of waiting for the debounce window, because
//     a delayed warm-up multiplies cold-start cost on user requests.
//   - Cap concurrent in-flight PATCHes per namespace so a noisy namespace
//     cannot exhaust the operator's apiserver budget.
//   - Keep the controller decoupled: this package depends on neither
//     controller-runtime nor the v1alpha1 API, only on a pluggable Patcher
//     interface and a FailureSink callback. The reconciler-side adapter
//     lives in internal/controller/ifaas/.
//
// Non-goals (deferred to S10):
//   - Prometheus metrics emission.
//   - Writing Conditions / k8s Events on flush failure. The Manager only
//     surfaces failures through FailureSink so the writer of Conditions
//     stays a single component.
package flusher

import (
	"context"
	"errors"
	"time"
)

// Decision is the unit of work the guard hands to the Manager. Two decisions
// for the same (Namespace, KSvcName) collapse into one: the latest one wins.
type Decision struct {
	// Namespace is the apiserver namespace owning both the KSvc and the
	// originating KnativeAdoption.
	Namespace string

	// KSvcName is the Knative Service whose autoscaling.min-scale annotation
	// the guard wants to patch. The Manager treats this as the coalescing
	// key; same name within the debounce window collapses to one PATCH.
	KSvcName string

	// AdoptionName lets the failure sink resolve the KnativeAdoption that
	// originated the decision so it can roll up Conditions / Events without
	// the Manager having to import the API package.
	AdoptionName string

	// DesiredMinScale is the value the guard wants to land on the KSvc.
	// Only 0 and 1 are meaningful in M1; anything else is forwarded to the
	// Patcher unchanged.
	DesiredMinScale int32

	// Reason is a short opaque tag (e.g. "scaledownz=true", "probe-error")
	// the failure sink can fold into Events / log lines.
	Reason string
}

// Key returns the coalescing key. Exposed so adapters and tests can index
// internal maps the same way the Manager does.
func (d Decision) Key() Key {
	return Key{Namespace: d.Namespace, KSvcName: d.KSvcName}
}

// Key is the (namespace, ksvc) tuple the Manager coalesces on.
type Key struct {
	Namespace string
	KSvcName  string
}

// Patcher is the apiserver-facing side of the Manager. Implementations must
// be safe for concurrent use; the Manager calls Patch from up to maxInFlight
// goroutines per namespace.
//
// Patcher is expected to be **idempotent and skip-aware**: when the live KSvc
// already has the desired min-scale, the implementation should return
// (skipped=true, nil) instead of issuing a no-op PATCH so the failure
// counters do not advance. The Manager treats `skipped=true, err=nil` as
// success-without-write.
type Patcher interface {
	Patch(ctx context.Context, d Decision) (skipped bool, err error)
}

// FailureSink is invoked once a decision has crossed the configured
// consecutive-failure threshold. The callback is dispatched serially per
// (namespace, ksvc) so the receiver does not need its own mutex; it must
// not block for long because it is invoked on the lane's worker goroutine.
//
// `attempts` is the number of consecutive failures observed (== threshold
// at the moment of escalation, then keeps growing if failures continue).
type FailureSink func(ctx context.Context, d Decision, attempts int, lastErr error)

// Defaults applied when Config leaves a knob unset.
const (
	DefaultDebounceWindow      = 2 * time.Second
	DefaultMaxInFlight         = 5
	DefaultMaxFailures         = 5
	DefaultRetryInitialBackoff = 100 * time.Millisecond
	DefaultRetryMaxBackoff     = 2 * time.Second
)

// Config configures a Manager. Zero-value fields fall back to the Default*
// constants above; that is the supported way to construct a production
// Manager without spelling out every knob.
type Config struct {
	// DebounceWindow is the slow-lane (min-scale=0) coalescing window.
	// All decisions for the same key landing inside this window collapse
	// into one PATCH carrying the latest DesiredMinScale.
	DebounceWindow time.Duration

	// MaxInFlight is the cap on concurrent Patcher invocations per
	// namespace. Set to 0 to use DefaultMaxInFlight; set to a negative
	// value to disable concurrency capping (useful in tests).
	MaxInFlight int

	// MaxFailures is the consecutive-failure count that triggers
	// FailureSink. The counter resets on the first success or skip.
	MaxFailures int

	// RetryInitialBackoff / RetryMaxBackoff bound the per-key exponential
	// backoff applied between retries. Backoff is doubled on each failure
	// up to RetryMaxBackoff.
	RetryInitialBackoff time.Duration
	RetryMaxBackoff     time.Duration

	// Clock is the time source used for all timers. Leave nil to use the
	// real wall clock; tests inject a fake clock to drive timers
	// deterministically.
	Clock Clock

	// Observer receives queue-depth / inflight / flush-result transitions
	// for metrics emission. Leave nil to disable observation; the Manager
	// substitutes a nopObserver so internal call sites stay
	// nil-check-free.
	Observer Observer
}

// withDefaults returns a copy of c with zero-valued fields populated by the
// Default* constants. Negative MaxInFlight is preserved (it disables the
// per-namespace concurrency cap entirely).
func (c Config) withDefaults() Config {
	if c.DebounceWindow <= 0 {
		c.DebounceWindow = DefaultDebounceWindow
	}
	if c.MaxInFlight == 0 {
		c.MaxInFlight = DefaultMaxInFlight
	}
	if c.MaxFailures <= 0 {
		c.MaxFailures = DefaultMaxFailures
	}
	if c.RetryInitialBackoff <= 0 {
		c.RetryInitialBackoff = DefaultRetryInitialBackoff
	}
	if c.RetryMaxBackoff <= 0 {
		c.RetryMaxBackoff = DefaultRetryMaxBackoff
	}
	if c.Clock == nil {
		c.Clock = realClock{}
	}
	if c.Observer == nil {
		c.Observer = nopObserver{}
	}
	return c
}

// ErrManagerStopped is returned by Enqueue once the Manager's run loop has
// been cancelled. Callers should treat it as a normal shutdown signal, not
// a flush failure.
var ErrManagerStopped = errors.New("flusher: manager stopped")
