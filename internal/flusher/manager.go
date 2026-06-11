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
	"sync"
)

// Manager owns the per-namespace coalescing queues. A single Manager is
// expected to live for the lifetime of the operator process; callers
// construct it once via New, drive it with Enqueue, and tear it down with
// Stop on shutdown.
//
// Concurrency model:
//   - One nsLane per namespace, lazily created on first Enqueue. Each lane
//     holds its own debounce timer, pending map and per-key failure counter
//     under lane.mu.
//   - PATCHes execute on goroutines spawned by the lane (one per dispatched
//     decision); the lane's `sem` channel caps concurrent in-flight PATCHes
//     to Config.MaxInFlight.
//   - Stop cancels the root context, releases all timers, and waits on
//     m.wg so the caller can rely on "no Patcher invocations after Stop
//     returns".
type Manager struct {
	cfg     Config
	patcher Patcher
	sink    FailureSink

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu      sync.Mutex
	lanes   map[string]*nsLane
	stopped bool

	// onResult is a test-only hook invoked from runOne *after* the per-key
	// failure counter has been updated. Production code leaves it nil; the
	// extra nil-check is the only runtime cost. The hook receives the key,
	// the Patch error (nil on success), and the post-update consecutive
	// failure count for that key (0 on success).
	onResult func(k Key, err error, count int)
}

// New builds a Manager bound to the given Patcher / FailureSink. Patcher is
// required (a nil Patcher panics on the first Enqueue); FailureSink may be
// nil, in which case failures past the threshold are silently dropped.
//
// The returned Manager is inert until something calls Enqueue; there is no
// background goroutine to start.
func New(cfg Config, p Patcher, sink FailureSink) *Manager {
	cfg = cfg.withDefaults()
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{
		cfg:     cfg,
		patcher: p,
		sink:    sink,
		ctx:     ctx,
		cancel:  cancel,
		lanes:   make(map[string]*nsLane),
	}
}

// Stop shuts the Manager down: it cancels in-flight PATCH contexts, stops
// every pending debounce timer and waits for already-dispatched workers to
// drain. After Stop returns, further Enqueue calls fail with
// ErrManagerStopped.
func (m *Manager) Stop() {
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return
	}
	m.stopped = true
	lanes := make([]*nsLane, 0, len(m.lanes))
	for _, l := range m.lanes {
		lanes = append(lanes, l)
	}
	m.mu.Unlock()

	for _, l := range lanes {
		l.stopTimer()
	}
	m.cancel()
	m.wg.Wait()
}

// Enqueue submits a decision. Same-key decisions inside the debounce window
// collapse to a single PATCH carrying the latest DesiredMinScale; a decision
// whose DesiredMinScale==1 short-circuits the debounce timer and dispatches
// the lane immediately.
//
// Enqueue is safe for concurrent callers.
func (m *Manager) Enqueue(d Decision) error {
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return ErrManagerStopped
	}
	lane := m.lanes[d.Namespace]
	if lane == nil {
		lane = m.newLane(d.Namespace)
		m.lanes[d.Namespace] = lane
	}
	m.mu.Unlock()

	lane.enqueue(d)
	return nil
}

func (m *Manager) newLane(namespace string) *nsLane {
	cap := m.cfg.MaxInFlight
	var sem chan struct{}
	if cap > 0 {
		sem = make(chan struct{}, cap)
	}
	return &nsLane{
		mgr:       m,
		namespace: namespace,
		pending:   make(map[Key]Decision),
		failures:  make(map[Key]int),
		sem:       sem,
	}
}

// nsLane holds the per-namespace state of the Manager. All fields except the
// channel-based `sem` are protected by `mu`. `namespace` is set at
// construction and never mutated.
type nsLane struct {
	mgr       *Manager
	namespace string

	mu       sync.Mutex
	pending  map[Key]Decision
	failures map[Key]int
	timer    Timer
	inflight int

	sem chan struct{}
}

// enqueue installs/overwrites the pending decision for d.Key and either
// arms the debounce timer (slow lane) or dispatches immediately (fast lane).
//
// Coalescing rule: a fast-lane decision (min-scale=1) cannot be overridden
// by a later slow-lane decision before it has flushed; any pending entry is
// flushed verbatim with the latest written value.
func (l *nsLane) enqueue(d Decision) {
	l.mu.Lock()
	l.pending[d.Key()] = d
	if d.DesiredMinScale == 1 {
		// Fast lane: cancel the slow-lane timer, swap out the entire
		// pending map and dispatch every entry under the same flush.
		// Folding fast + slow into one drain keeps the invariant that
		// the lane is empty after a flush.
		batch := l.drainLocked()
		l.mu.Unlock()
		l.mgr.cfg.Observer.QueueDepth(l.namespace, 0)
		l.dispatch(batch)
		return
	}

	// Slow lane: arm the timer if it is not already running. If it is,
	// the existing one will already fire within the same window we just
	// wrote into.
	if l.timer == nil {
		l.timer = l.mgr.cfg.Clock.AfterFunc(l.mgr.cfg.DebounceWindow, l.flushSlow)
	}
	depth := len(l.pending)
	l.mu.Unlock()
	l.mgr.cfg.Observer.QueueDepth(l.namespace, depth)
}

// drainLocked snapshots and clears the pending map and stops the debounce
// timer. The caller must hold l.mu.
func (l *nsLane) drainLocked() []Decision {
	if l.timer != nil {
		l.timer.Stop()
		l.timer = nil
	}
	if len(l.pending) == 0 {
		return nil
	}
	batch := make([]Decision, 0, len(l.pending))
	for _, d := range l.pending {
		batch = append(batch, d)
	}
	l.pending = make(map[Key]Decision)
	return batch
}

// flushSlow is invoked from the debounce timer goroutine when the slow-lane
// window expires.
func (l *nsLane) flushSlow() {
	l.mu.Lock()
	batch := l.drainLocked()
	l.mu.Unlock()
	l.mgr.cfg.Observer.QueueDepth(l.namespace, 0)
	l.dispatch(batch)
}

// stopTimer is called from Manager.Stop. It cancels the timer but does not
// drain the pending map: any decisions still queued at shutdown are simply
// dropped because the operator is going away.
func (l *nsLane) stopTimer() {
	l.mu.Lock()
	if l.timer != nil {
		l.timer.Stop()
		l.timer = nil
	}
	l.mu.Unlock()
}

// dispatch fans out batch onto worker goroutines, each capped by the lane's
// in-flight semaphore.
func (l *nsLane) dispatch(batch []Decision) {
	for _, d := range batch {
		d := d
		l.mgr.wg.Add(1)
		go l.runOne(d)
	}
}

func (l *nsLane) runOne(d Decision) {
	defer l.mgr.wg.Done()

	if l.sem != nil {
		select {
		case l.sem <- struct{}{}:
		case <-l.mgr.ctx.Done():
			return
		}
		defer func() { <-l.sem }()
	}

	l.mu.Lock()
	l.inflight++
	curIn := l.inflight
	l.mu.Unlock()
	l.mgr.cfg.Observer.PatchInflight(l.namespace, curIn)

	skipped, err := l.mgr.patcher.Patch(l.mgr.ctx, d)

	l.mu.Lock()
	l.inflight--
	curOut := l.inflight
	switch {
	case err == nil:
		// Success or apiserver-state-already-equal: reset the per-key
		// failure counter so a future intermittent error does not
		// instantly cross the threshold.
		delete(l.failures, d.Key())
		l.mu.Unlock()
		l.mgr.cfg.Observer.PatchInflight(l.namespace, curOut)
		if skipped {
			l.mgr.cfg.Observer.FlushResult(l.namespace, ResultSkipped)
		} else {
			l.mgr.cfg.Observer.FlushResult(l.namespace, ResultSuccess)
		}
		if l.mgr.onResult != nil {
			l.mgr.onResult(d.Key(), nil, 0)
		}
	default:
		l.failures[d.Key()]++
		count := l.failures[d.Key()]
		threshold := l.mgr.cfg.MaxFailures
		l.mu.Unlock()
		l.mgr.cfg.Observer.PatchInflight(l.namespace, curOut)
		l.mgr.cfg.Observer.FlushResult(l.namespace, ResultFailed)
		if l.mgr.onResult != nil {
			l.mgr.onResult(d.Key(), err, count)
		}
		if count >= threshold && l.mgr.sink != nil {
			l.mgr.sink(l.mgr.ctx, d, count, err)
		}
	}
}
