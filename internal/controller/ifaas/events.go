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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	ifaasv1alpha1 "github.com/ifbiu/ifaas/api/ifaas/v1alpha1"
)

// Event reasons emitted on KnativeAdoption (and mirrored on the source
// Deployment when present). Kept short and stable so dashboards / alert
// rules can match against them.
const (
	EventReasonAdopted          = "Adopted"
	EventReasonAdoptionRefused  = "AdoptionRefused"
	EventReasonScaleDownAllowed = "ScaleDownAllowed"
	EventReasonScaleDownBlocked = "ScaleDownBlocked"
	EventReasonGuardFlushFail   = "GuardFlushFailed"
	EventReasonRestoreFailed    = "RestoreFailed"
	EventReasonReleased         = "Released"
)

// emitDual records a Kubernetes Event against the KnativeAdoption and, on
// best effort, the source Deployment.
//
// "Best effort" here means: a missing Deployment (release path, or
// pre-creation race) silently skips the mirror; any other Get error is
// swallowed because Events are observability, not control flow. Production
// code that wants to fail when the Deployment is missing should use a
// dedicated Get + check instead of leaning on this helper.
func (r *KnativeAdoptionReconciler) emitDual(
	ctx context.Context,
	a *ifaasv1alpha1.KnativeAdoption,
	eventType, reason, message string,
) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Event(a, eventType, reason, message)

	dep := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Namespace: a.Namespace, Name: sourceName(a)}, dep)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			// fall through; we already wrote the primary event.
			return
		}
		return
	}
	r.Recorder.Event(dep, eventType, reason, message)
}

// emitEvent records an Event on the KnativeAdoption only. Used for
// adoption-internal transitions (guard verdicts, swap refusals) that have
// no semantic match on the source Deployment.
func (r *KnativeAdoptionReconciler) emitEvent(
	a *ifaasv1alpha1.KnativeAdoption,
	eventType, reason, message string,
) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Event(a, eventType, reason, message)
}

// eventTypeNormal / eventTypeWarning are aliases of the corev1 constants so
// call sites in this package don't have to import corev1 just for the two
// strings.
const (
	eventTypeNormal  = corev1.EventTypeNormal
	eventTypeWarning = corev1.EventTypeWarning
)
