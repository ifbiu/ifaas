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

import "time"

// Clock is the minimal time abstraction the Manager needs. It exists so the
// debounce / backoff timers can be driven deterministically from tests
// instead of waiting on wall time.
//
// Implementations must be safe for concurrent use.
type Clock interface {
	// Now returns the current logical time.
	Now() time.Time

	// AfterFunc schedules f to run after d elapses on this clock and
	// returns a Timer handle that can be Stop()'d before firing. The
	// returned handle's semantics mirror time.Timer.
	AfterFunc(d time.Duration, f func()) Timer
}

// Timer is the subset of time.Timer used by the Manager.
type Timer interface {
	// Stop prevents the underlying callback from firing. It returns true
	// if the call stops the timer, false if the timer has already expired
	// or been stopped.
	Stop() bool
}

// realClock is the production wall-clock implementation. It is intentionally
// stateless and a value receiver so the zero value is usable.
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) AfterFunc(d time.Duration, f func()) Timer {
	return realTimer{t: time.AfterFunc(d, f)}
}

type realTimer struct {
	t *time.Timer
}

func (r realTimer) Stop() bool { return r.t.Stop() }
