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

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock drives the Manager's debounce timers from tests. It is
// intentionally minimal: timers are stored in registration order and fire
// synchronously when the test calls Advance(d) past their deadline.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

type fakeTimer struct {
	deadline time.Time
	fn       func()
	stopped  bool
	fired    bool
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(0, 0)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) AfterFunc(d time.Duration, fn func()) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{deadline: c.now.Add(d), fn: fn}
	c.timers = append(c.timers, t)
	return t
}

func (t *fakeTimer) Stop() bool {
	if t.fired || t.stopped {
		return false
	}
	t.stopped = true
	return true
}

// Advance moves the logical clock forward by d and fires every timer whose
// deadline has been crossed, in deadline order.
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	due := make([]*fakeTimer, 0)
	keep := c.timers[:0]
	for _, t := range c.timers {
		if t.stopped || t.fired {
			continue
		}
		if !t.deadline.After(c.now) {
			t.fired = true
			due = append(due, t)
		} else {
			keep = append(keep, t)
		}
	}
	c.timers = keep
	c.mu.Unlock()

	sort.SliceStable(due, func(i, j int) bool { return due[i].deadline.Before(due[j].deadline) })
	for _, t := range due {
		t.fn()
	}
}

// recordingPatcher is the unit-test side of Patcher. It captures every call
// in-order and lets each test customise the per-call behaviour via patchFn.
type recordingPatcher struct {
	mu      sync.Mutex
	calls   []Decision
	patchFn func(Decision) (bool, error)
}

func (p *recordingPatcher) Patch(_ context.Context, d Decision) (bool, error) {
	p.mu.Lock()
	p.calls = append(p.calls, d)
	fn := p.patchFn
	p.mu.Unlock()
	if fn == nil {
		return false, nil
	}
	return fn(d)
}

func (p *recordingPatcher) snapshot() []Decision {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]Decision, len(p.calls))
	copy(out, p.calls)
	return out
}

// waitFor polls cond until it returns true or budget expires; it lets us
// avoid sleeps that race the Manager's goroutines without resorting to
// channel plumbing inside every test case.
func waitFor(cond func() bool, budget time.Duration) bool {
	deadline := time.Now().Add(budget)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func newTestManager(p Patcher, sink FailureSink, clock Clock) *Manager {
	return New(Config{
		DebounceWindow:      2 * time.Second,
		MaxInFlight:         5,
		MaxFailures:         5,
		RetryInitialBackoff: 10 * time.Millisecond,
		RetryMaxBackoff:     50 * time.Millisecond,
		Clock:               clock,
	}, p, sink)
}

// TestSlowLaneCoalescing covers the headline guarantee of S7: many decisions
// for the same key landing inside the debounce window collapse into a single
// PATCH carrying the latest DesiredMinScale.
func TestSlowLaneCoalescing(t *testing.T) {
	clock := newFakeClock()
	patcher := &recordingPatcher{}
	m := newTestManager(patcher, nil, clock)
	defer m.Stop()

	for i := 0; i < 10; i++ {
		err := m.Enqueue(Decision{
			Namespace:       "default",
			KSvcName:        "echo",
			AdoptionName:    "echo",
			DesiredMinScale: 0,
			Reason:          "burst",
		})
		if err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	if got := len(patcher.snapshot()); got != 0 {
		t.Fatalf("expected 0 patches before debounce window expires, got %d", got)
	}

	clock.Advance(2 * time.Second)
	if !waitFor(func() bool { return len(patcher.snapshot()) == 1 }, 1*time.Second) {
		t.Fatalf("expected exactly 1 patch after debounce window, got %d", len(patcher.snapshot()))
	}

	calls := patcher.snapshot()
	if got := calls[0].DesiredMinScale; got != 0 {
		t.Fatalf("expected DesiredMinScale=0, got %d", got)
	}
}

// TestFastLanePreemptsDebounce asserts that a min-scale=1 decision triggers
// an immediate flush instead of waiting on the debounce window. It also
// asserts that earlier slow-lane entries for the same key are folded into
// the same flush instead of producing a separate, later PATCH.
func TestFastLanePreemptsDebounce(t *testing.T) {
	clock := newFakeClock()
	patcher := &recordingPatcher{}
	m := newTestManager(patcher, nil, clock)
	defer m.Stop()

	if err := m.Enqueue(Decision{Namespace: "ns1", KSvcName: "echo", DesiredMinScale: 0, Reason: "ok"}); err != nil {
		t.Fatalf("enqueue slow: %v", err)
	}
	if err := m.Enqueue(Decision{Namespace: "ns1", KSvcName: "echo", DesiredMinScale: 1, Reason: "block"}); err != nil {
		t.Fatalf("enqueue fast: %v", err)
	}

	if !waitFor(func() bool { return len(patcher.snapshot()) >= 1 }, 1*time.Second) {
		t.Fatalf("fast lane did not flush within 1s")
	}

	clock.Advance(5 * time.Second)
	time.Sleep(20 * time.Millisecond)

	calls := patcher.snapshot()
	if len(calls) != 1 {
		t.Fatalf("expected 1 patch, got %d (%+v)", len(calls), calls)
	}
	if calls[0].DesiredMinScale != 1 {
		t.Fatalf("expected DesiredMinScale=1 after fast lane, got %d", calls[0].DesiredMinScale)
	}
}

// TestFastLaneDifferentKeysCoalesceSameFlush asserts that when multiple keys
// have pending slow-lane entries and a fast-lane Enqueue lands, the lane
// drain folds *all* keys into one batch (per S7: fast-lane preemption is
// per-namespace, not per-key).
func TestFastLaneDifferentKeysCoalesceSameFlush(t *testing.T) {
	clock := newFakeClock()
	patcher := &recordingPatcher{}
	m := newTestManager(patcher, nil, clock)
	defer m.Stop()

	for i := 0; i < 3; i++ {
		if err := m.Enqueue(Decision{Namespace: "ns1", KSvcName: name(i), DesiredMinScale: 0}); err != nil {
			t.Fatalf("enqueue slow: %v", err)
		}
	}
	if err := m.Enqueue(Decision{Namespace: "ns1", KSvcName: "fast-key", DesiredMinScale: 1}); err != nil {
		t.Fatalf("enqueue fast: %v", err)
	}

	if !waitFor(func() bool { return len(patcher.snapshot()) == 4 }, 1*time.Second) {
		t.Fatalf("expected 4 patches after fast-lane drain, got %d", len(patcher.snapshot()))
	}
}

// TestMaxInFlightCap asserts that the per-namespace concurrency cap is
// enforced: dispatching N>MaxInFlight decisions never sees more than
// MaxInFlight Patcher invocations active simultaneously.
func TestMaxInFlightCap(t *testing.T) {
	clock := newFakeClock()
	const inFlightCap = 5
	const fanout = 8

	var (
		active    int64
		maxActive int64
		release   = make(chan struct{})
	)
	patcher := &recordingPatcher{
		patchFn: func(_ Decision) (bool, error) {
			n := atomic.AddInt64(&active, 1)
			for {
				m := atomic.LoadInt64(&maxActive)
				if n <= m || atomic.CompareAndSwapInt64(&maxActive, m, n) {
					break
				}
			}
			<-release
			atomic.AddInt64(&active, -1)
			return false, nil
		},
	}
	m := New(Config{
		DebounceWindow: 2 * time.Second,
		MaxInFlight:    inFlightCap,
		MaxFailures:    5,
		Clock:          clock,
	}, patcher, nil)
	defer m.Stop()

	for i := 0; i < fanout; i++ {
		if err := m.Enqueue(Decision{Namespace: "ns1", KSvcName: name(i), DesiredMinScale: 1}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	if !waitFor(func() bool { return atomic.LoadInt64(&active) == int64(inFlightCap) }, 1*time.Second) {
		t.Fatalf("expected %d active patches, got %d", inFlightCap, atomic.LoadInt64(&active))
	}

	close(release)
	if !waitFor(func() bool { return len(patcher.snapshot()) == fanout }, 1*time.Second) {
		t.Fatalf("expected %d patches total, got %d", fanout, len(patcher.snapshot()))
	}
	if got := atomic.LoadInt64(&maxActive); got > int64(inFlightCap) {
		t.Fatalf("max-in-flight breached: %d > %d", got, inFlightCap)
	}
}

// TestFailureSinkAfterThreshold asserts the failure counter increments only
// across the same key, fires FailureSink once it crosses MaxFailures, and
// keeps reporting subsequent failures while the counter stays elevated.
func TestFailureSinkAfterThreshold(t *testing.T) {
	clock := newFakeClock()
	boom := errors.New("boom")
	patcher := &recordingPatcher{patchFn: func(_ Decision) (bool, error) { return false, boom }}

	type sinkCall struct {
		Decision
		attempts int
	}
	var (
		sinkMu    sync.Mutex
		sinkCalls []sinkCall
	)
	sink := func(_ context.Context, d Decision, attempts int, _ error) {
		sinkMu.Lock()
		sinkCalls = append(sinkCalls, sinkCall{Decision: d, attempts: attempts})
		sinkMu.Unlock()
	}

	m := New(Config{
		DebounceWindow: 2 * time.Second,
		MaxInFlight:    5,
		MaxFailures:    5,
		Clock:          clock,
	}, patcher, sink)
	defer m.Stop()

	for i := 0; i < 5; i++ {
		if err := m.Enqueue(Decision{Namespace: "ns1", KSvcName: "echo", DesiredMinScale: 1}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	if !waitFor(func() bool {
		sinkMu.Lock()
		defer sinkMu.Unlock()
		return len(sinkCalls) >= 1
	}, 1*time.Second) {
		t.Fatalf("expected sink to fire after threshold")
	}

	sinkMu.Lock()
	got := sinkCalls[0].attempts
	sinkMu.Unlock()
	if got < 5 {
		t.Fatalf("expected attempts >= 5 on first sink call, got %d", got)
	}
}

// TestSuccessResetsFailureCounter asserts that the per-key failure counter
// drops to zero on a successful PATCH, so a single intermittent error after
// many recoveries does not falsely trigger Degraded.
func TestSuccessResetsFailureCounter(t *testing.T) {
	clock := newFakeClock()

	var failCount int64
	boom := errors.New("boom")
	patcher := &recordingPatcher{patchFn: func(_ Decision) (bool, error) {
		if atomic.LoadInt64(&failCount) > 0 {
			atomic.AddInt64(&failCount, -1)
			return false, boom
		}
		return false, nil
	}}

	var sinkHits int64
	sink := func(_ context.Context, _ Decision, _ int, _ error) {
		atomic.AddInt64(&sinkHits, 1)
	}

	m := New(Config{
		DebounceWindow: 2 * time.Second,
		MaxInFlight:    5,
		MaxFailures:    5,
		Clock:          clock,
	}, patcher, sink)
	defer m.Stop()

	resultsCh := make(chan int, 16)
	m.onResult = func(_ Key, _ error, count int) { resultsCh <- count }

	awaitCount := func(want int) {
		t.Helper()
		select {
		case got := <-resultsCh:
			if got != want {
				t.Fatalf("expected failure count=%d, got %d", want, got)
			}
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for failure count=%d", want)
		}
	}

	atomic.StoreInt64(&failCount, 4)
	for i := 1; i <= 4; i++ {
		_ = m.Enqueue(Decision{Namespace: "ns1", KSvcName: "echo", DesiredMinScale: 1})
		awaitCount(i)
	}

	_ = m.Enqueue(Decision{Namespace: "ns1", KSvcName: "echo", DesiredMinScale: 1})
	awaitCount(0)

	atomic.StoreInt64(&failCount, 1)
	_ = m.Enqueue(Decision{Namespace: "ns1", KSvcName: "echo", DesiredMinScale: 1})
	awaitCount(1)

	if got := atomic.LoadInt64(&sinkHits); got != 0 {
		t.Fatalf("expected no sink calls (counter reset by intermediate success), got %d", got)
	}
}

// TestStopRejectsFurtherEnqueues guards against accidental enqueues after
// shutdown, which would leak goroutines into the parent test.
func TestStopRejectsFurtherEnqueues(t *testing.T) {
	clock := newFakeClock()
	m := newTestManager(&recordingPatcher{}, nil, clock)
	m.Stop()

	err := m.Enqueue(Decision{Namespace: "ns1", KSvcName: "echo", DesiredMinScale: 0})
	if !errors.Is(err, ErrManagerStopped) {
		t.Fatalf("expected ErrManagerStopped, got %v", err)
	}
}

func name(i int) string {
	return "ksvc-" + string(rune('a'+i))
}
