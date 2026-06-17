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
	"fmt"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ifaasv1alpha1 "github.com/ifbiu/ifaas/api/ifaas/v1alpha1"
)

// Reason constants populated into KnativeAdoption.status.conditions[].reason.
// Kept here so the test package and the reconciler refer to the same strings.
const (
	ReasonAdopted                = "Adopted"
	ReasonSourceMissing          = "SourceMissing"
	ReasonTranslationFailed      = "TranslationFailed"
	ReasonServiceApplyFailed     = "ServiceApplyFailed"
	ReasonKnativeServiceNotReady = "KnativeServiceNotReady"
	ReasonKnativeServiceReady    = "KnativeServiceReady"
	ReasonSourceQuiesced         = "SourceQuiesced"
	ReasonSourceScaleFailed      = "SourceScaleFailed"
	ReasonReconciling            = "Reconciling"
	ReasonTrafficReady           = "TrafficReady"
	ReasonTrafficReconcileFailed = "TrafficReconcileFailed"

	// ServiceSwapper reasons.
	ReasonServiceSwapping       = "ServiceSwapping"
	ReasonServiceSwapTakenOver  = "ServiceSwapTakenOver"
	ReasonServiceSwapTypeRefuse = "ServiceTypeNotSupported"
	ReasonServiceSwapOwnedByOS  = "ServiceOwnedByExternal"
	ReasonServiceSwapDeleteFail = "ServiceDeleteFailed"

	// ScaleDownGuard reasons.
	ReasonScaleDownAllowed    = "ScaleDownAllowed"
	ReasonScaleDownBlocked    = "ScaleDownBlocked"
	ReasonScaleDownNoPods     = "ScaleDownNoPods"
	ReasonScaleDownProbeError = "ScaleDownProbeError"
	ReasonGuardSkipped        = "GuardSkipped"

	// Restore (S9) reasons.
	ReasonAwaitingChildrenGC = "AwaitingChildrenGC"
	ReasonRestoringService   = "RestoringService"
	ReasonRestoringSource    = "RestoringSource"
	ReasonRestoreFailed      = "RestoreFailed"
	ReasonReleased           = "Released"
)

// FieldOwner used for server-side apply of operator-owned KSvc objects.
const FieldOwner = "ifaas-autopilot"

// FinalizerRestoreSourceService keeps the KnativeAdoption alive until the
// ServiceSwapper teardown path (S9) can rebuild the pre-adoption Service from
// status.sourceSnapshot.service.
const FinalizerRestoreSourceService = "ifaas.ifbiu.com/restore-source-service"

// FinalizerRestoreSource keeps the KnativeAdoption alive until the teardown
// path (S9) can restore the source Deployment to its pre-adoption replica
// count from status.sourceSnapshot.replicas. Held independently from
// FinalizerRestoreSourceService so each side of the restore (Service vs.
// Deployment) can be drained on its own, in the right order: the original
// Service is rebuilt first (because its name collides with KSvc-derived
// resources and must wait for cascade GC), then the Deployment is scaled
// back up.
const FinalizerRestoreSource = "ifaas.ifbiu.com/restore-source"

// AnnoServiceManagedBy marks a Service the operator already adopted in a prior
// reconcile loop, so the swapper does not snapshot/delete it twice.
const (
	AnnoServiceManagedBy      = "ifaas.ifbiu.com/managed-by"
	AnnoServiceManagedByValue = "knative-autopilot"
)

// readyAggregateFailures lists the condition types that, if present at
// status=True, force Ready=False and dictate Ready's reason/message. The
// order is the priority order: the first match wins.
var readyAggregateFailures = []string{
	ifaasv1alpha1.ConditionDegraded,
	ifaasv1alpha1.ConditionTrafficDegraded,
	ifaasv1alpha1.ConditionTranslationDegraded,
	ifaasv1alpha1.ConditionServiceAdoptionRefuse,
	ifaasv1alpha1.ConditionSourceMissing,
}

// readyAggregatePositives lists the condition types that must all be at
// status=True for Ready to flip True. The order is the priority order: the
// first one missing/False supplies Ready's reason/message.
var readyAggregatePositives = []string{
	ifaasv1alpha1.ConditionAdopted,
	ifaasv1alpha1.ConditionServiceAdopted,
	ifaasv1alpha1.ConditionSourceQuiesced,
}

// condReasonIs reports whether the named condition currently exists on `a`
// and carries the given reason. Used by the guard to detect "did the verdict
// just flip" without taking a snapshot of the entire condition slice.
func condReasonIs(a *ifaasv1alpha1.KnativeAdoption, t, reason string) bool {
	c := apimeta.FindStatusCondition(a.Status.Conditions, t)
	return c != nil && c.Reason == reason
}

// recomputeReady aggregates Ready from the per-stage signals:
//  1. any failure condition at True → Ready=False with that condition's
//     reason/message;
//  2. any positive condition missing or not True → Ready=False with its
//     reason (or ReasonReconciling) and message;
//  3. otherwise → Ready=True / ReasonAdopted.
//
// ScaleDownAllowed is intentionally excluded — a workload that refuses to
// scale to zero is still serving traffic and therefore Ready. The scale-down
// status is exposed on its own condition (and as
// status.lastScaleDownProbe).
func recomputeReady(a *ifaasv1alpha1.KnativeAdoption) {
	for _, t := range readyAggregateFailures {
		c := apimeta.FindStatusCondition(a.Status.Conditions, t)
		if c != nil && c.Status == metav1.ConditionTrue {
			setReady(a, metav1.ConditionFalse, c.Reason, c.Message)
			return
		}
	}
	requiredPositives := make([]string, 0, len(readyAggregatePositives)+1)
	requiredPositives = append(requiredPositives, readyAggregatePositives...)
	if trafficEnabled(a) {
		requiredPositives = append(requiredPositives, ifaasv1alpha1.ConditionTrafficReady)
	}

	for _, t := range requiredPositives {
		c := apimeta.FindStatusCondition(a.Status.Conditions, t)
		if c == nil || c.Status != metav1.ConditionTrue {
			reason := ReasonReconciling
			msg := fmt.Sprintf("waiting for %s", t)
			if c != nil {
				reason = c.Reason
				msg = c.Message
			}
			setReady(a, metav1.ConditionFalse, reason, msg)
			return
		}
	}
	setReady(a, metav1.ConditionTrue, ReasonAdopted, "adoption complete")
}
