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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	ifaasv1alpha1 "github.com/ifbiu/ifaas/api/ifaas/v1alpha1"
)

// SwapDecision is the result of inspecting the same-name Service before the
// operator applies its Knative Service. The reconciler short-circuits on
// SwapRefused, snapshots-and-deletes on SwapTakenOver, and continues unchanged
// on SwapPass.
type SwapDecision int

const (
	// SwapPass means there is nothing for the swapper to do this round.
	SwapPass SwapDecision = iota
	// SwapTakenOver means the Service exists, qualifies for adoption, and the
	// reconciler must snapshot + delete it before continuing.
	SwapTakenOver
	// SwapRefused means the Service exists but the operator must not touch it.
	// The reconciler propagates the reason and stops the pipeline.
	SwapRefused
)

// ClassifyServiceForSwap is a pure function: given the same-name Service it
// returns the decision the reconciler should act on plus a (reason, message)
// pair suitable for a status condition.
//
// Inputs:
//   - svc may be nil when no same-name Service exists; that maps to SwapPass.
//
// Rules (order matters):
//  1. Service is already operator-owned (annotation managed-by=knative-autopilot
//     or controllerRef of kind Route from serving.knative.dev) → SwapPass.
//  2. Service Type is LoadBalancer / NodePort / ExternalName → SwapRefused.
//  3. Service has a non-operator controllerRef (e.g. StatefulSet headless) →
//     SwapRefused.
//  4. Otherwise (ClusterIP/None, no controllerRef) → SwapTakenOver.
func ClassifyServiceForSwap(svc *corev1.Service) (decision SwapDecision, reason, message string) {
	if svc == nil {
		return SwapPass, "", ""
	}
	if svc.GetAnnotations()[AnnoServiceManagedBy] == AnnoServiceManagedByValue {
		return SwapPass, "", "service already adopted"
	}
	if owner := metav1.GetControllerOf(svc); owner != nil {
		if owner.APIVersion == "serving.knative.dev/v1" {
			return SwapPass, "", "service derived from Knative Route"
		}
		return SwapRefused, ReasonServiceSwapOwnedByOS,
			fmt.Sprintf("service is owned by %s/%s and cannot be adopted",
				owner.APIVersion, owner.Kind)
	}
	switch svc.Spec.Type {
	case corev1.ServiceTypeLoadBalancer, corev1.ServiceTypeNodePort, corev1.ServiceTypeExternalName:
		return SwapRefused, ReasonServiceSwapTypeRefuse,
			fmt.Sprintf("service type %q is not supported for adoption", svc.Spec.Type)
	}
	return SwapTakenOver, "", ""
}

// swapService runs the side-effectful side of S5 for a single reconcile pass.
//
// Returns the SwapDecision the caller should branch on:
//   - SwapPass     → continue the pipeline; no condition needs updating.
//   - SwapTakenOver→ snapshot was just written, Service was deleted; the
//     condition is left as ServiceAdopted=False, reason=ServiceSwapping until
//     the KSvc itself becomes Ready (handled by the existing readiness branch).
//   - SwapRefused  → caller must NOT create a KSvc; ServiceAdoptionRefused was
//     written with the reason+message returned by ClassifyServiceForSwap.
func (r *KnativeAdoptionReconciler) swapService(ctx context.Context, adoption *ifaasv1alpha1.KnativeAdoption) (SwapDecision, error) {
	log := logf.FromContext(ctx)

	svc := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Namespace: adoption.Namespace, Name: adoption.Name}, svc)
	switch {
	case apierrors.IsNotFound(err):
		svc = nil
	case err != nil:
		return SwapPass, fmt.Errorf("get same-name service: %w", err)
	}

	decision, reason, msg := ClassifyServiceForSwap(svc)
	switch decision {
	case SwapPass:
		// Clear any stale refusal so a previously rejected workload can recover
		// once the user fixes the offending Service.
		removeCondition(adoption, ifaasv1alpha1.ConditionServiceAdoptionRefuse)
		return SwapPass, nil

	case SwapRefused:
		setCondition(adoption, ifaasv1alpha1.ConditionServiceAdoptionRefuse,
			metav1.ConditionTrue, reason, msg)
		setCondition(adoption, ifaasv1alpha1.ConditionServiceAdopted,
			metav1.ConditionFalse, reason, msg)
		return SwapRefused, nil

	case SwapTakenOver:
		removeCondition(adoption, ifaasv1alpha1.ConditionServiceAdoptionRefuse)
		setCondition(adoption, ifaasv1alpha1.ConditionServiceAdopted,
			metav1.ConditionFalse, ReasonServiceSwapping,
			"deleting pre-existing same-name Service before creating KSvc")
		if err := r.snapshotAndDeleteService(ctx, adoption, svc); err != nil {
			setCondition(adoption, ifaasv1alpha1.ConditionDegraded,
				metav1.ConditionTrue, ReasonServiceSwapDeleteFail, err.Error())
			return SwapPass, err
		}
		log.Info("pre-existing service taken over", "service", svc.Name)
		return SwapTakenOver, nil
	}
	return SwapPass, nil
}

// snapshotAndDeleteService writes a deep copy of svc.Spec into
// adoption.Status.SourceSnapshot.Service (so S9 can rebuild it) and deletes the
// Service. The delete is best-effort: a concurrent delete is treated as success
// because that is the post-condition we want.
func (r *KnativeAdoptionReconciler) snapshotAndDeleteService(ctx context.Context, adoption *ifaasv1alpha1.KnativeAdoption, svc *corev1.Service) error {
	if adoption.Status.SourceSnapshot == nil {
		adoption.Status.SourceSnapshot = &ifaasv1alpha1.SourceSnapshot{}
	}
	if adoption.Status.SourceSnapshot.Service == nil {
		spec := *svc.Spec.DeepCopy()
		// ClusterIP / ClusterIPs are assigned by the apiserver and must be
		// stripped from the snapshot so the rebuild path does not collide with
		// the freshly allocated IPs on the KSvc-derived Service.
		spec.ClusterIP = ""
		spec.ClusterIPs = nil
		adoption.Status.SourceSnapshot.Service = &spec
	}
	if err := r.Delete(ctx, svc); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete pre-existing service: %w", err)
	}
	return nil
}

func controllerHasFinalizer(adoption *ifaasv1alpha1.KnativeAdoption, name string) bool {
	for _, f := range adoption.Finalizers {
		if f == name {
			return true
		}
	}
	return false
}
