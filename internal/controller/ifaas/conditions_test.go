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
	"testing"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ifaasv1alpha1 "github.com/ifbiu/ifaas/api/ifaas/v1alpha1"
)

// cond is a tiny constructor that keeps the table-driven cases readable.
func cond(t string, s metav1.ConditionStatus, reason string) metav1.Condition {
	return metav1.Condition{Type: t, Status: s, Reason: reason, Message: reason + " msg"}
}

// TestRecomputeReady covers the three priority tiers documented on
// recomputeReady: failure-condition wins, missing-positive falls through, all
// positives green flips Ready=True.
func TestRecomputeReady(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name           string
		trafficEnabled bool
		conds          []metav1.Condition
		wantStatus     metav1.ConditionStatus
		wantReason     string
	}{
		{
			name: "all positives true → Ready=True",
			conds: []metav1.Condition{
				cond(ifaasv1alpha1.ConditionAdopted, metav1.ConditionTrue, ReasonAdopted),
				cond(ifaasv1alpha1.ConditionServiceAdopted, metav1.ConditionTrue, ReasonKnativeServiceReady),
				cond(ifaasv1alpha1.ConditionSourceQuiesced, metav1.ConditionTrue, ReasonSourceQuiesced),
			},
			wantStatus: metav1.ConditionTrue,
			wantReason: ReasonAdopted,
		},
		{
			name: "Degraded=True overrides positives",
			conds: []metav1.Condition{
				cond(ifaasv1alpha1.ConditionAdopted, metav1.ConditionTrue, ReasonAdopted),
				cond(ifaasv1alpha1.ConditionServiceAdopted, metav1.ConditionTrue, ReasonKnativeServiceReady),
				cond(ifaasv1alpha1.ConditionSourceQuiesced, metav1.ConditionTrue, ReasonSourceQuiesced),
				cond(ifaasv1alpha1.ConditionDegraded, metav1.ConditionTrue, ReasonServiceApplyFailed),
			},
			wantStatus: metav1.ConditionFalse,
			wantReason: ReasonServiceApplyFailed,
		},
		{
			name:           "TrafficDegraded=True overrides positives",
			trafficEnabled: true,
			conds: []metav1.Condition{
				cond(ifaasv1alpha1.ConditionAdopted, metav1.ConditionTrue, ReasonAdopted),
				cond(ifaasv1alpha1.ConditionServiceAdopted, metav1.ConditionTrue, ReasonKnativeServiceReady),
				cond(ifaasv1alpha1.ConditionSourceQuiesced, metav1.ConditionTrue, ReasonSourceQuiesced),
				cond(ifaasv1alpha1.ConditionTrafficDegraded, metav1.ConditionTrue, ReasonTrafficReconcileFailed),
			},
			wantStatus: metav1.ConditionFalse,
			wantReason: ReasonTrafficReconcileFailed,
		},
		{
			name: "TranslationDegraded=True wins over missing positives",
			conds: []metav1.Condition{
				cond(ifaasv1alpha1.ConditionTranslationDegraded, metav1.ConditionTrue, ReasonTranslationFailed),
			},
			wantStatus: metav1.ConditionFalse,
			wantReason: ReasonTranslationFailed,
		},
		{
			name: "ServiceAdoptionRefused=True surfaces refusal",
			conds: []metav1.Condition{
				cond(ifaasv1alpha1.ConditionServiceAdoptionRefuse, metav1.ConditionTrue, ReasonServiceSwapTypeRefuse),
			},
			wantStatus: metav1.ConditionFalse,
			wantReason: ReasonServiceSwapTypeRefuse,
		},
		{
			name: "SourceMissing=True surfaces source loss",
			conds: []metav1.Condition{
				cond(ifaasv1alpha1.ConditionSourceMissing, metav1.ConditionTrue, ReasonSourceMissing),
			},
			wantStatus: metav1.ConditionFalse,
			wantReason: ReasonSourceMissing,
		},
		{
			name: "missing Adopted falls through to Reconciling",
			conds: []metav1.Condition{
				cond(ifaasv1alpha1.ConditionServiceAdopted, metav1.ConditionTrue, ReasonKnativeServiceReady),
				cond(ifaasv1alpha1.ConditionSourceQuiesced, metav1.ConditionTrue, ReasonSourceQuiesced),
			},
			wantStatus: metav1.ConditionFalse,
			wantReason: ReasonReconciling,
		},
		{
			name: "ServiceAdopted=False propagates its reason",
			conds: []metav1.Condition{
				cond(ifaasv1alpha1.ConditionAdopted, metav1.ConditionTrue, ReasonAdopted),
				cond(ifaasv1alpha1.ConditionServiceAdopted, metav1.ConditionFalse, ReasonKnativeServiceNotReady),
				cond(ifaasv1alpha1.ConditionSourceQuiesced, metav1.ConditionTrue, ReasonSourceQuiesced),
			},
			wantStatus: metav1.ConditionFalse,
			wantReason: ReasonKnativeServiceNotReady,
		},
		{
			name:           "traffic enabled waits TrafficReady",
			trafficEnabled: true,
			conds: []metav1.Condition{
				cond(ifaasv1alpha1.ConditionAdopted, metav1.ConditionTrue, ReasonAdopted),
				cond(ifaasv1alpha1.ConditionServiceAdopted, metav1.ConditionTrue, ReasonKnativeServiceReady),
				cond(ifaasv1alpha1.ConditionSourceQuiesced, metav1.ConditionTrue, ReasonSourceQuiesced),
			},
			wantStatus: metav1.ConditionFalse,
			wantReason: ReasonReconciling,
		},
		{
			name:           "traffic enabled with TrafficReady=True becomes Ready",
			trafficEnabled: true,
			conds: []metav1.Condition{
				cond(ifaasv1alpha1.ConditionAdopted, metav1.ConditionTrue, ReasonAdopted),
				cond(ifaasv1alpha1.ConditionServiceAdopted, metav1.ConditionTrue, ReasonKnativeServiceReady),
				cond(ifaasv1alpha1.ConditionSourceQuiesced, metav1.ConditionTrue, ReasonSourceQuiesced),
				cond(ifaasv1alpha1.ConditionTrafficReady, metav1.ConditionTrue, ReasonTrafficReady),
			},
			wantStatus: metav1.ConditionTrue,
			wantReason: ReasonAdopted,
		},
		{
			name: "ScaleDownAllowed=False does not affect Ready",
			conds: []metav1.Condition{
				cond(ifaasv1alpha1.ConditionAdopted, metav1.ConditionTrue, ReasonAdopted),
				cond(ifaasv1alpha1.ConditionServiceAdopted, metav1.ConditionTrue, ReasonKnativeServiceReady),
				cond(ifaasv1alpha1.ConditionSourceQuiesced, metav1.ConditionTrue, ReasonSourceQuiesced),
				cond(ifaasv1alpha1.ConditionScaleDownAllowed, metav1.ConditionFalse, ReasonScaleDownBlocked),
			},
			wantStatus: metav1.ConditionTrue,
			wantReason: ReasonAdopted,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := &ifaasv1alpha1.KnativeAdoption{}
			if tc.trafficEnabled {
				a.Spec.Traffic = &ifaasv1alpha1.Traffic{Enabled: true}
			}
			a.Status.Conditions = append([]metav1.Condition{}, tc.conds...)
			recomputeReady(a)
			ready := apimeta.FindStatusCondition(a.Status.Conditions, ifaasv1alpha1.ConditionReady)
			if ready == nil {
				t.Fatalf("Ready condition was not written")
			}
			if ready.Status != tc.wantStatus {
				t.Fatalf("Ready.Status = %s; want %s", ready.Status, tc.wantStatus)
			}
			if ready.Reason != tc.wantReason {
				t.Fatalf("Ready.Reason = %s; want %s", ready.Reason, tc.wantReason)
			}
		})
	}
}

// TestCondReasonIs covers the nil / mismatch / hit branches of the helper.
func TestCondReasonIs(t *testing.T) {
	t.Parallel()
	a := &ifaasv1alpha1.KnativeAdoption{}
	if condReasonIs(a, ifaasv1alpha1.ConditionScaleDownAllowed, ReasonScaleDownBlocked) {
		t.Fatalf("condReasonIs on empty conditions returned true")
	}
	a.Status.Conditions = []metav1.Condition{
		cond(ifaasv1alpha1.ConditionScaleDownAllowed, metav1.ConditionFalse, ReasonScaleDownAllowed),
	}
	if condReasonIs(a, ifaasv1alpha1.ConditionScaleDownAllowed, ReasonScaleDownBlocked) {
		t.Fatalf("condReasonIs reason mismatch returned true")
	}
	if !condReasonIs(a, ifaasv1alpha1.ConditionScaleDownAllowed, ReasonScaleDownAllowed) {
		t.Fatalf("condReasonIs reason match returned false")
	}
}
