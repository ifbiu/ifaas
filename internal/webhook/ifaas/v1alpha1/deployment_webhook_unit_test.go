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

package v1alpha1

import (
	"encoding/json"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestReplicasOwnedByExternalManager pins the four ManagedFields shapes
// the gate has to reason about, without booting an apiserver:
//
//   - ifaas owns alone        → admit (steady state)
//   - external owns alone     → ban   (real scale-up intent)
//   - co-ownership            → ban   (Update path produces shared
//                                      ownership; even one external
//                                      claimant flips the gate)
//   - nobody owns spec.replicas → admit (transient — let the next
//                                        reconcile re-stamp the field)
//
// envtest exercises the SSA half end-to-end (deployment_webhook_test.go);
// these unit cases nail down the entry-by-entry decoding so a future
// refactor of entryOwnsSpecReplicas can't silently change semantics.
func TestReplicasOwnedByExternalManager(t *testing.T) {
	specReplicas := map[string]any{
		"f:spec": map[string]any{"f:replicas": map[string]any{}},
	}
	specTemplate := map[string]any{
		"f:spec": map[string]any{"f:template": map[string]any{}},
	}
	metadataLabels := map[string]any{
		"f:metadata": map[string]any{"f:labels": map[string]any{}},
	}

	type owner struct {
		manager string
		fields  map[string]any
	}
	cases := []struct {
		name   string
		owners []owner
		want   bool
	}{
		{
			name:   "ifaas-autopilot owns spec.replicas alone",
			owners: []owner{{manager: fieldOwnerAutopilot, fields: specReplicas}},
			want:   false,
		},
		{
			name:   "ifaas-watcher owns spec.replicas alone",
			owners: []owner{{manager: fieldOwnerWatcher, fields: specReplicas}},
			want:   false,
		},
		{
			name:   "external manager owns spec.replicas alone",
			owners: []owner{{manager: "argocd-controller", fields: specReplicas}},
			want:   true,
		},
		{
			name: "ifaas + external co-own spec.replicas",
			owners: []owner{
				{manager: fieldOwnerAutopilot, fields: specReplicas},
				{manager: "kubectl-edit", fields: specReplicas},
			},
			want: true,
		},
		{
			name: "external manager claims unrelated fields only",
			owners: []owner{
				{manager: fieldOwnerAutopilot, fields: specReplicas},
				{manager: "argocd-controller", fields: specTemplate},
			},
			want: false,
		},
		{
			name: "ifaas-autopilot owns metadata only, replicas unowned",
			owners: []owner{
				{manager: fieldOwnerAutopilot, fields: metadataLabels},
			},
			want: false,
		},
		{
			name:   "no managedFields entries at all",
			owners: nil,
			want:   false,
		},
		{
			name: "entry with empty FieldsV1 is ignored",
			owners: []owner{
				{manager: "argocd-controller", fields: nil},
				{manager: fieldOwnerAutopilot, fields: specReplicas},
			},
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := &appsv1.Deployment{}
			for _, o := range tc.owners {
				entry := metav1.ManagedFieldsEntry{Manager: o.manager}
				if o.fields != nil {
					raw, err := json.Marshal(o.fields)
					if err != nil {
						t.Fatalf("marshal fields: %v", err)
					}
					entry.FieldsV1 = &metav1.FieldsV1{Raw: raw}
				}
				d.ManagedFields = append(d.ManagedFields, entry)
			}
			if got := replicasOwnedByExternalManager(d); got != tc.want {
				t.Fatalf("replicasOwnedByExternalManager() = %v, want %v", got, tc.want)
			}
		})
	}
}