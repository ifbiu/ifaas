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

package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// expectedFamilies is the on-the-wire metric name (with the autopilot_ prefix
// added by the wrapped registerer) for every collector this package owns.
// The list is intentionally hand-maintained so adding a new collector to
// allCollectors without also adding its prefixed name here is caught here.
var expectedFamilies = []string{
	"autopilot_adopted_total",
	"autopilot_translation_errors_total",
	"autopilot_revision_age_seconds",
	"autopilot_scaledown_blocked_total",
	"autopilot_scaledownz_probe_errors_total",
	"autopilot_scaledownz_probe_latency_seconds",
	"autopilot_guard_flush_total",
	"autopilot_guard_queue_depth",
	"autopilot_guard_patch_inflight",
}

// TestCollectorsExposeAutopilotPrefix asserts every metric this package
// registers lands on the controller-runtime registry with the `autopilot_`
// prefix expected by docs/knative-autopilot-design.md §9. The prefix is
// added by WrapRegistererWithPrefix at registration time, so the only
// reliable way to observe it is to Gather() the underlying registry.
//
// Label-vec children are eagerly created so the metric family appears in
// the Gather output even when no production code path has yet incremented
// it.
func TestCollectorsExposeAutopilotPrefix(t *testing.T) {
	t.Parallel()
	const ns = "metrics-test-prefix"
	AdoptedTotal.WithLabelValues(ns)
	TranslationErrorsTotal.WithLabelValues(TranslationReasonOther)
	RevisionAgeSeconds.WithLabelValues(ns, "x")
	ScaleDownBlockedTotal.WithLabelValues(ns, "x")
	ScaleDownProbeErrorsTotal.WithLabelValues(ProbeErrorReasonProberFault)
	GuardFlushTotal.WithLabelValues(ns, GuardFlushResultSuccess)
	GuardQueueDepth.WithLabelValues(ns)
	GuardPatchInflight.WithLabelValues(ns)

	mfs, err := ctrlmetrics.Registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	got := make(map[string]bool, len(mfs))
	for _, mf := range mfs {
		got[mf.GetName()] = true
	}
	for _, name := range expectedFamilies {
		if !got[name] {
			t.Fatalf("metric family %q not exposed; got prefixed families: %v", name, prefixedFamilyNames(mfs))
		}
		if !strings.HasPrefix(name, metricPrefix) {
			t.Fatalf("expected family %q itself missing the %q prefix", name, metricPrefix)
		}
	}
}

// TestCollectorsRegisterIdempotently re-registers each collector through the
// wrapped registerer and asserts the underlying registry refuses the second
// registration. This catches accidental double-registration in init().
func TestCollectorsRegisterIdempotently(t *testing.T) {
	t.Parallel()
	for _, c := range Collectors() {
		err := registerer.Register(c)
		if err == nil {
			t.Fatalf("collector %T re-registered without error; want failure", c)
		}
	}
}

// TestFlusherObserverFlushResult walks the three Result enum values through
// the observer and asserts each lands as a distinct counter sample on
// GuardFlushTotal.
func TestFlusherObserverFlushResult(t *testing.T) {
	t.Parallel()
	obs := NewFlusherObserver()
	results := []string{GuardFlushResultSuccess, GuardFlushResultSkipped, GuardFlushResultFailed}
	const ns = "metrics-test-ns-flush"
	for _, r := range results {
		obs.FlushResult(ns, r)
	}
	for _, r := range results {
		got := readCounter(t, GuardFlushTotal.WithLabelValues(ns, r))
		if got != 1 {
			t.Fatalf("FlushResult(%s) sample = %v; want 1", r, got)
		}
	}
}

// TestFlusherObserverGauges asserts QueueDepth / PatchInflight write through
// to their gauges. Different namespaces are used so the test is repeatable
// against the package-global collectors.
func TestFlusherObserverGauges(t *testing.T) {
	t.Parallel()
	obs := NewFlusherObserver()

	obs.QueueDepth("metrics-test-ns-q", 7)
	if got := readGauge(t, GuardQueueDepth.WithLabelValues("metrics-test-ns-q")); got != 7 {
		t.Fatalf("QueueDepth gauge = %v; want 7", got)
	}

	obs.PatchInflight("metrics-test-ns-p", 3)
	if got := readGauge(t, GuardPatchInflight.WithLabelValues("metrics-test-ns-p")); got != 3 {
		t.Fatalf("PatchInflight gauge = %v; want 3", got)
	}

	obs.QueueDepth("metrics-test-ns-q", 0)
	if got := readGauge(t, GuardQueueDepth.WithLabelValues("metrics-test-ns-q")); got != 0 {
		t.Fatalf("QueueDepth gauge after reset = %v; want 0", got)
	}
}

// readCounter returns the current Counter sample value.
func readCounter(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatalf("counter write: %v", err)
	}
	return m.Counter.GetValue()
}

// readGauge returns the current Gauge sample value.
func readGauge(t *testing.T, g prometheus.Gauge) float64 {
	t.Helper()
	var m dto.Metric
	if err := g.Write(&m); err != nil {
		t.Fatalf("gauge write: %v", err)
	}
	return m.Gauge.GetValue()
}

// prefixedFamilyNames extracts the names of metric families that already
// carry the autopilot_ prefix; used purely to keep failure messages short.
func prefixedFamilyNames(mfs []*dto.MetricFamily) []string {
	out := make([]string, 0, len(mfs))
	for _, mf := range mfs {
		if strings.HasPrefix(mf.GetName(), metricPrefix) {
			out = append(out, mf.GetName())
		}
	}
	return out
}
