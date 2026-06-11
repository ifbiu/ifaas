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
	"github.com/ifbiu/ifaas/internal/flusher"
)

// flusherObserver bridges flusher.Observer to the Prometheus collectors in
// this package. It is intentionally stateless — the Manager owns the live
// counters; the observer just mirrors transitions onto Prometheus.
type flusherObserver struct{}

// NewFlusherObserver returns the singleton flusher.Observer that updates
// guard_queue_depth, guard_patch_inflight, and guard_flush_total. The
// returned value is safe for concurrent use because every call ultimately
// hits one of the package-level CounterVec/GaugeVec instances, which are
// goroutine-safe by construction.
func NewFlusherObserver() flusher.Observer { return flusherObserver{} }

// QueueDepth records the current per-namespace lane depth.
func (flusherObserver) QueueDepth(namespace string, depth int) {
	GuardQueueDepth.WithLabelValues(namespace).Set(float64(depth))
}

// PatchInflight records the current per-namespace inflight patch count.
func (flusherObserver) PatchInflight(namespace string, inflight int) {
	GuardPatchInflight.WithLabelValues(namespace).Set(float64(inflight))
}

// FlushResult bumps the guard_flush_total counter for a single Decision.
// `result` must be one of the GuardFlushResult* constants in metrics.go;
// any other value is accepted by Prometheus but breaks dashboards.
func (flusherObserver) FlushResult(namespace, result string) {
	GuardFlushTotal.WithLabelValues(namespace, result).Inc()
}
