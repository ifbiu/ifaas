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

package flusher

// Observer is the metrics-side hook every Manager fires on lane state
// transitions. It is intentionally narrow:
//
//   - QueueDepth     — current number of debounced decisions waiting in
//     a namespace lane. Reported on every enqueue and
//     after every drain.
//   - PatchInflight  — current count of in-flight Patcher invocations
//     for a namespace. Bracketed around each Patch call.
//   - FlushResult    — terminal outcome of a single Decision's Patch
//     attempt: ResultSuccess / ResultSkipped / ResultFailed.
//
// Implementations must be safe for concurrent use; the Manager calls into
// the Observer from worker goroutines without any external serialisation.
//
// The flusher package deliberately does not depend on prometheus/client_golang
// — internal/metrics injects a concrete implementation at wire time. Tests
// that don't care about metrics leave Config.Observer nil and let
// withDefaults swap in nopObserver.
type Observer interface {
	QueueDepth(namespace string, depth int)
	PatchInflight(namespace string, inflight int)
	FlushResult(namespace, result string)
}

// Result label values reported through Observer.FlushResult. Kept as
// constants so the flusher and any Observer implementation cannot
// disagree on the wire format.
const (
	ResultSuccess = "success"
	ResultSkipped = "skipped"
	ResultFailed  = "failed"
)

// nopObserver is the default Observer used when Config.Observer is nil. It
// avoids nil-checks at every call site at the cost of three method calls
// per transition; production paths inject a real Observer so this only ever
// runs in tests.
type nopObserver struct{}

func (nopObserver) QueueDepth(string, int)     {}
func (nopObserver) PatchInflight(string, int)  {}
func (nopObserver) FlushResult(string, string) {}
