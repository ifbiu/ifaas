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

import "testing"

// TestParseAllowScaleDown locks the wire-format compatibility matrix between
// the operator's HTTPProber and the per-workload /scaledownz endpoints. The
// canonical JSON contract is documented in
// docs/ckbackup-scaledownz-design.md §"`/scaledownz` 协议".
func TestParseAllowScaleDown(t *testing.T) {
	cases := []struct {
		name string
		body string
		want bool
	}{
		// Canonical JSON contract.
		{"json_allow", `{"allowScaleDown":true,"inFlight":0}`, true},
		{"json_block_inflight", `{"allowScaleDown":false,"inFlight":3}`, false},
		{"json_block_only_field", `{"allowScaleDown":false}`, false},
		{"json_pretty_with_whitespace", "  \n{\n  \"allowScaleDown\": true,\n  \"inFlight\": 0\n}\n", true},
		{"json_extra_fields_ignored", `{"allowScaleDown":true,"inFlight":0,"extra":"v"}`, true},
		{"json_missing_field_defaults_false", `{"inFlight":0}`, false},
		{"json_wrong_type_defaults_false", `{"allowScaleDown":"true"}`, false},
		{"json_malformed_object_defaults_false", `{"allowScaleDown":tru`, false},

		// Legacy plain-text fallback. Kept for stubs that pre-date the JSON
		// contract; case-insensitive and whitespace-tolerant.
		{"plain_true", "true", true},
		{"plain_TRUE", "TRUE", true},
		{"plain_one", "1", true},
		{"plain_yes", "yes", true},
		{"plain_y", "y", true},
		{"plain_ok", "ok", true},
		{"plain_with_whitespace", "  true\n", true},
		{"plain_false", "false", false},
		{"plain_zero", "0", false},
		{"plain_garbage", "maybe", false},

		// Pathological inputs.
		{"empty", "", false},
		{"whitespace_only", "   \n\t", false},
		{"json_array_not_supported", `[true]`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseAllowScaleDown([]byte(tc.body))
			if got != tc.want {
				t.Fatalf("parseAllowScaleDown(%q) = %v, want %v", tc.body, got, tc.want)
			}
		})
	}
}