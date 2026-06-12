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
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	appsv1 "k8s.io/api/apps/v1"
	authnv1 "k8s.io/api/authentication/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// TestDeploymentScaleValidatorHandle pins the decision tree of the
// /scale subresource validator without booting an apiserver. The scale
// path cannot reuse the typed CustomValidator harness used by
// deployment_webhook_test.go (Scale is a different Go type), so this
// table-driven unit test is the canonical regression net for the gate
// that closes the kubectl-scale bypass discovered in P7 case B.
//
// Each case sets up a parent Deployment in a fake client.Reader and
// drives Handle with a synthetic admission.Request whose Object /
// OldObject Raw fields encode autoscaling/v1.Scale. The fake reader is
// a stand-in for mgr.GetAPIReader(); we never need the cache layer.
func TestDeploymentScaleValidatorHandle(t *testing.T) {
	const (
		ns           = "default"
		name         = "scale-target"
		ctrlUser     = "system:serviceaccount:ifaas-system:ifaas-controller-manager"
		humanUser    = "alice@example.com"
		notFoundName = "ghost"
	)

	scheme := runtime.NewScheme()
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}

	deployment := func(labels map[string]string) *appsv1.Deployment {
		return &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: ns,
				Labels:    labels,
			},
		}
	}
	autopilotLabels := map[string]string{labelEnabled: labelEnabledValue}

	scaleRaw := func(replicas int32) []byte {
		raw, err := json.Marshal(&autoscalingv1.Scale{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec:       autoscalingv1.ScaleSpec{Replicas: replicas},
		})
		if err != nil {
			t.Fatalf("marshal Scale: %v", err)
		}
		return raw
	}

	type want struct {
		allowed bool
		// when allowed=false the gate must surface the canonical error
		// users will see; we substring-match to avoid binding the test
		// to exact wording.
		denyContains string
		// when the request body is malformed the gate replies with an
		// HTTP error code rather than an admission verdict; we assert
		// the code so a future refactor cannot silently swallow it.
		errorCode int32
	}
	cases := []struct {
		name        string
		parent      *appsv1.Deployment // nil → reader returns NotFound
		username    string
		reqName     string
		oldReplicas int32
		newReplicas int32
		oldRaw      []byte // override for malformed-old paths
		newRaw      []byte // override for malformed-new paths
		want        want
	}{
		{
			name:        "controller SA bypasses gate even on 0->3",
			parent:      deployment(autopilotLabels),
			username:    ctrlUser,
			reqName:     name,
			oldReplicas: 0, newReplicas: 3,
			want: want{allowed: true},
		},
		{
			name:        "parent missing → admit (defer to other layers)",
			parent:      nil,
			username:    humanUser,
			reqName:     notFoundName,
			oldReplicas: 0, newReplicas: 3,
			want: want{allowed: true},
		},
		{
			name:        "parent without autopilot label → admit",
			parent:      deployment(nil),
			username:    humanUser,
			reqName:     name,
			oldReplicas: 0, newReplicas: 3,
			want: want{allowed: true},
		},
		{
			name:        "0 → 0 noop",
			parent:      deployment(autopilotLabels),
			username:    humanUser,
			reqName:     name,
			oldReplicas: 0, newReplicas: 0,
			want: want{allowed: true},
		},
		{
			name:        "scale-down 2 → 0 (emergency stop)",
			parent:      deployment(autopilotLabels),
			username:    humanUser,
			reqName:     name,
			oldReplicas: 2, newReplicas: 0,
			want: want{allowed: true},
		},
		{
			name:        "labelled, human, 0 → 2 → deny",
			parent:      deployment(autopilotLabels),
			username:    humanUser,
			reqName:     name,
			oldReplicas: 0, newReplicas: 2,
			want: want{denyContains: "manual scale-up from 0 is rejected"},
		},
		{
			name:        "labelled, human, 1 → 3 (oldR != 0) → admit",
			parent:      deployment(autopilotLabels),
			username:    humanUser,
			reqName:     name,
			oldReplicas: 1, newReplicas: 3,
			want: want{allowed: true},
		},
		{
			name:     "malformed new Scale → 400",
			parent:   deployment(autopilotLabels),
			username: humanUser,
			reqName:  name,
			newRaw:   []byte("{not json"),
			want:     want{errorCode: http.StatusBadRequest},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := fake.NewClientBuilder().WithScheme(scheme)
			if tc.parent != nil {
				b = b.WithObjects(tc.parent)
			}
			reader := b.Build()

			v := &DeploymentScaleValidator{
				Reader:             reader,
				ControllerUsername: ctrlUser,
			}

			newRaw := tc.newRaw
			if newRaw == nil {
				newRaw = scaleRaw(tc.newReplicas)
			}
			oldRaw := tc.oldRaw
			if oldRaw == nil {
				oldRaw = scaleRaw(tc.oldReplicas)
			}
			req := admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					Operation:   admissionv1.Update,
					Name:        tc.reqName,
					Namespace:   ns,
					SubResource: "scale",
					UserInfo:    authnv1.UserInfo{Username: tc.username},
					Object:      runtime.RawExtension{Raw: newRaw},
					OldObject:   runtime.RawExtension{Raw: oldRaw},
				},
			}

			resp := v.Handle(context.Background(), req)
			switch {
			case tc.want.errorCode != 0:
				if resp.Allowed {
					t.Fatalf("expected error response, got allowed")
				}
				if resp.Result == nil || resp.Result.Code != tc.want.errorCode {
					t.Fatalf("expected error code %d, got %+v", tc.want.errorCode, resp.Result)
				}
			case tc.want.allowed:
				if !resp.Allowed {
					t.Fatalf("expected allowed, got %+v", resp.Result)
				}
			default:
				if resp.Allowed {
					t.Fatalf("expected denied with %q, got allowed", tc.want.denyContains)
				}
				if resp.Result == nil || !strings.Contains(resp.Result.Message, tc.want.denyContains) {
					t.Fatalf("deny message %q does not contain %q", resp.Result, tc.want.denyContains)
				}
			}
		})
	}
}

// silence unused-import linters in the rare case the test list is pruned.
var _ = client.Object(&appsv1.Deployment{})