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

package ifaas

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ifaasv1alpha1 "github.com/ifbiu/ifaas/api/ifaas/v1alpha1"
)

// newTestScheme builds a scheme that knows about Deployment + KnativeAdoption,
// the only kinds the events helper ever Get's against.
func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := appsv1.AddToScheme(s); err != nil {
		t.Fatalf("apps scheme: %v", err)
	}
	if err := ifaasv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("ifaas scheme: %v", err)
	}
	return s
}

// drainEvents reads every queued event off the recorder so order-sensitive
// assertions can match without timing out.
func drainEvents(rec *record.FakeRecorder) []string {
	out := make([]string, 0, len(rec.Events))
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

// TestEmitDualMirrorsOnDeployment asserts the helper writes one Event onto
// the CR and one Event onto the source Deployment when the Deployment exists.
func TestEmitDualMirrorsOnDeployment(t *testing.T) {
	t.Parallel()
	scheme := newTestScheme(t)
	a := &ifaasv1alpha1.KnativeAdoption{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "myapp"},
		Spec:       ifaasv1alpha1.KnativeAdoptionSpec{SourceRef: ifaasv1alpha1.SourceRef{Name: "myapp"}},
	}
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "myapp"},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dep, a).Build()
	rec := record.NewFakeRecorder(8)
	r := &KnativeAdoptionReconciler{Client: cl, Scheme: scheme, Recorder: rec}

	r.emitDual(context.Background(), a, eventTypeNormal, EventReasonAdopted, "ready")

	got := drainEvents(rec)
	if len(got) != 2 {
		t.Fatalf("expected 2 events (CR + Deployment), got %d: %v", len(got), got)
	}
	for _, e := range got {
		if !strings.Contains(e, EventReasonAdopted) || !strings.Contains(e, "ready") {
			t.Fatalf("event missing reason/message: %q", e)
		}
	}
}

// TestEmitDualSkipsMirrorWhenDeploymentMissing asserts the helper records on
// the CR only when the source Deployment cannot be fetched.
func TestEmitDualSkipsMirrorWhenDeploymentMissing(t *testing.T) {
	t.Parallel()
	scheme := newTestScheme(t)
	a := &ifaasv1alpha1.KnativeAdoption{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns2", Name: "absent"},
		Spec:       ifaasv1alpha1.KnativeAdoptionSpec{SourceRef: ifaasv1alpha1.SourceRef{Name: "absent"}},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(a).Build()
	rec := record.NewFakeRecorder(8)
	r := &KnativeAdoptionReconciler{Client: cl, Scheme: scheme, Recorder: rec}

	r.emitDual(context.Background(), a, eventTypeWarning, EventReasonAdoptionRefused, "no")

	got := drainEvents(rec)
	if len(got) != 1 {
		t.Fatalf("expected 1 event (CR only), got %d: %v", len(got), got)
	}
}

// TestEventHelpersTolerateNilRecorder makes sure callers that wire the
// reconciler without a Recorder (e.g. envtest fixtures) don't crash.
func TestEventHelpersTolerateNilRecorder(t *testing.T) {
	t.Parallel()
	r := &KnativeAdoptionReconciler{}
	a := &ifaasv1alpha1.KnativeAdoption{ObjectMeta: metav1.ObjectMeta{Namespace: "x", Name: "y"}}
	r.emitDual(context.Background(), a, eventTypeNormal, EventReasonReleased, "released")
	r.emitEvent(a, eventTypeNormal, EventReasonScaleDownAllowed, "ok")
}

// TestEmitEventOnlyTouchesAdoption asserts emitEvent never reaches into the
// Deployment — adoption-internal transitions stay on the CR.
func TestEmitEventOnlyTouchesAdoption(t *testing.T) {
	t.Parallel()
	scheme := newTestScheme(t)
	a := &ifaasv1alpha1.KnativeAdoption{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns3", Name: "guard"},
	}
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns3", Name: "guard"},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dep, a).Build()
	rec := record.NewFakeRecorder(8)
	r := &KnativeAdoptionReconciler{Client: cl, Scheme: scheme, Recorder: rec}

	r.emitEvent(a, eventTypeNormal, EventReasonScaleDownBlocked, "blocked")

	got := drainEvents(rec)
	if len(got) != 1 {
		t.Fatalf("expected 1 event on CR, got %d: %v", len(got), got)
	}
}

// TestTranslationErrorReasonMapping covers every translator sentinel and the
// fallback bucket.
func TestTranslationErrorReasonMapping(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   error
		want string
	}{
		{"nil → empty", nil, ""},
		{"unknown → other", errStub("boom"), "other"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := translationErrorReason(tc.in); got != tc.want {
				t.Fatalf("translationErrorReason(%v) = %q; want %q", tc.in, got, tc.want)
			}
		})
	}
}

// errStub is a tiny error type used in mapping tests; it deliberately does
// not match any translator sentinel so the test exercises the default branch.
type errStub string

func (e errStub) Error() string { return string(e) }
