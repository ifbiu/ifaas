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
	"errors"
	"testing"
)

func TestVote(t *testing.T) {
	boom := errors.New("boom")
	cases := []struct {
		name string
		in   []Result
		want Outcome
	}{
		{name: "empty", in: nil, want: OutcomeNoPods},
		{name: "single-true", in: []Result{{Allowed: true}}, want: OutcomeAllowZero},
		{name: "all-true", in: []Result{{Allowed: true}, {Allowed: true}}, want: OutcomeAllowZero},
		{name: "one-false", in: []Result{{Allowed: true}, {Allowed: false}}, want: OutcomeBlock},
		{name: "all-false", in: []Result{{Allowed: false}, {Allowed: false}}, want: OutcomeBlock},
		{name: "one-error", in: []Result{{Allowed: true}, {Err: boom}}, want: OutcomeBlock},
		{name: "all-error", in: []Result{{Err: boom}, {Err: boom}}, want: OutcomeBlock},
		{name: "false+error", in: []Result{{Allowed: false}, {Err: boom}}, want: OutcomeBlock},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Vote(tc.in); got != tc.want {
				t.Errorf("Vote() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestTally(t *testing.T) {
	boom := errors.New("boom")
	in := []Result{
		{Allowed: true},
		{Allowed: false},
		{Err: boom},
		{Allowed: true},
	}
	ok, refused, errored := Tally(in)
	if ok != 2 || refused != 1 || errored != 1 {
		t.Errorf("Tally got (%d,%d,%d); want (2,1,1)", ok, refused, errored)
	}
}

func TestHasErrors(t *testing.T) {
	boom := errors.New("boom")
	if HasErrors([]Result{{Allowed: true}, {Allowed: false}}) {
		t.Errorf("HasErrors should be false when only refusals are present")
	}
	if !HasErrors([]Result{{Allowed: true}, {Err: boom}}) {
		t.Errorf("HasErrors should be true when any result carries an error")
	}
	if HasErrors(nil) {
		t.Errorf("HasErrors on empty slice should be false")
	}
}
