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

// Package metrics centralises every Prometheus collector emitted by the
// operator.
//
// Why a dedicated package:
//   - One place to read off the entire metric surface — name, label set,
//     help text — without hunting through reconcilers.
//   - Tests anywhere in the tree can call `metrics.AdoptedTotal.WithLabelValues(...)`
//     without taking a circular dependency on the controller package.
//   - The flusher gets a thin Observer interface (see flusher_observer.go)
//     so its package stays metrics-free.
//
// Where the metrics are exposed:
//   - All collectors are registered against the controller-runtime global
//     `metrics.Registry`, which is what the manager's :8443 /metrics
//     endpoint already serves. That keeps a single endpoint, single auth
//     story, single TLS chain.
//   - Names are auto-prefixed with `autopilot_` via WrapRegistererWithPrefix
//     to match the inventory in docs/knative-autopilot-design.md §9 and
//     impl-plan §S10.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// metricPrefix is prepended to every collector defined in this package.
// The wrapped registerer guarantees no other code path can land an
// unprefixed `autopilot_*` collector by accident.
const metricPrefix = "autopilot_"

// registerer is the wrapped controller-runtime registry. Every collector
// declared below registers through it so the on-the-wire metric name has
// the `autopilot_` prefix without each Opts.Name carrying it explicitly.
var registerer = prometheus.WrapRegistererWithPrefix(metricPrefix, ctrlmetrics.Registry)

// Result label values for autopilot_guard_flush_total. Kept in one place
// so the flusher observer and tests cannot diverge.
const (
	GuardFlushResultSuccess = "success"
	GuardFlushResultSkipped = "skipped"
	GuardFlushResultFailed  = "failed"
)

// Reason label values for autopilot_translation_errors_total. The empty
// string is reserved for "no translator error" which we never emit.
const (
	TranslationReasonMultiContainer  = "multi-container"
	TranslationReasonHostNetwork     = "host-network"
	TranslationReasonHostPort        = "host-port"
	TranslationReasonInvalidProbe    = "invalid-probe"
	TranslationReasonUnsupportedKind = "unsupported-kind"
	TranslationReasonOther           = "other"
)

// Reason label values for autopilot_scaledownz_probe_errors_total.
const (
	ProbeErrorReasonListPods     = "list-pods"
	ProbeErrorReasonProberFault  = "prober-fault"
	ProbeErrorReasonInvalidPort  = "invalid-port"
	ProbeErrorReasonProbeTimeout = "probe-timeout"
)

var (
	// AdoptedTotal counts successful adoption transitions per namespace.
	// Incremented exactly once on each Adopted=False → Adopted=True
	// transition observed by the reconciler — reconciles that find the
	// CR already adopted do not bump the counter.
	AdoptedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "adopted_total",
		Help: "Number of successful KnativeAdoption transitions to Adopted=True.",
	}, []string{"namespace"})

	// TranslationErrorsTotal counts refusals from the translator. The
	// `reason` label is the structured cause (multi-container, host-network,
	// …) — see the TranslationReason* constants for the full enum.
	TranslationErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "translation_errors_total",
		Help: "Number of translator refusals, partitioned by reason.",
	}, []string{"reason"})

	// RevisionAgeSeconds is the wall-clock age of the active adoption,
	// computed as `now - creationTimestamp` and re-stamped on every
	// successful reconcile. M1 reports adoption age in lieu of revision
	// age (the two coincide on KSvc creation).
	RevisionAgeSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "revision_age_seconds",
		Help: "Wall-clock age of the active KnativeAdoption in seconds.",
	}, []string{"namespace", "name"})

	// ScaleDownBlockedTotal counts guard rounds that voted Block. Useful
	// to spot workloads that perpetually report `/scaledownz=false` and
	// therefore never quiesce.
	ScaleDownBlockedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "scaledown_blocked_total",
		Help: "Number of /scaledownz guard rounds whose aggregate vote was Block.",
	}, []string{"namespace", "adoption"})

	// ScaleDownProbeErrorsTotal counts transport-layer probe errors. The
	// `reason` label is one of the ProbeErrorReason* constants.
	ScaleDownProbeErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "scaledownz_probe_errors_total",
		Help: "Number of /scaledownz probe attempts that failed at the transport layer.",
	}, []string{"reason"})

	// ScaleDownProbeLatencySeconds tracks per-pod probe latency. Buckets
	// span 5 ms .. 5 s; anything slower is almost certainly the prober's
	// own context timeout and gets bucketed in +Inf.
	ScaleDownProbeLatencySeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "scaledownz_probe_latency_seconds",
		Help:    "End-to-end /scaledownz probe latency observed by the guard.",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 11),
	})

	// GuardFlushTotal counts every Decision the namespace flusher
	// resolves. `result` is one of GuardFlushResult* (success / skipped /
	// failed). Failures here are the per-attempt failures, not the
	// terminal threshold-cross handed to FailureSink.
	GuardFlushTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "guard_flush_total",
		Help: "Number of namespace-flusher decisions resolved, by result.",
	}, []string{"namespace", "result"})

	// GuardQueueDepth is the number of debounced Decisions waiting in
	// each per-namespace lane. A persistent non-zero value means the
	// flusher is serialising under contention.
	GuardQueueDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "guard_queue_depth",
		Help: "Pending decisions in each namespace flusher lane.",
	}, []string{"namespace"})

	// GuardPatchInflight is the number of Decisions currently being
	// patched against the apiserver per namespace. Bounded by the
	// flusher's per-namespace concurrency; sustained saturation is a
	// signal the apiserver is the bottleneck.
	GuardPatchInflight = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "guard_patch_inflight",
		Help: "Number of decisions currently being patched per namespace.",
	}, []string{"namespace"})
)

// allCollectors is the single source of truth for what gets registered.
// Tests pull it through `Collectors()` to assert the full surface without
// duplicating the list.
var allCollectors = []prometheus.Collector{
	AdoptedTotal,
	TranslationErrorsTotal,
	RevisionAgeSeconds,
	ScaleDownBlockedTotal,
	ScaleDownProbeErrorsTotal,
	ScaleDownProbeLatencySeconds,
	GuardFlushTotal,
	GuardQueueDepth,
	GuardPatchInflight,
}

// Collectors returns the live collector slice. Read-only by convention —
// callers must not mutate the underlying array.
func Collectors() []prometheus.Collector { return allCollectors }

func init() {
	registerer.MustRegister(allCollectors...)
}
