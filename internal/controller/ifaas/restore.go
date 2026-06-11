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

// S9 — Restore chain for KnativeAdoption.
//
// On adoption (handled in §S5 + §S3) the operator captures two pieces of
// pre-adoption state that the user expects to get back when they release the
// workload:
//
//   1. status.sourceSnapshot.replicas — the Deployment.spec.replicas value
//      observed before the operator scaled it to zero.
//   2. status.sourceSnapshot.service  — a deep copy of the same-name core
//      Service that the swapper deleted to make room for the KSvc-derived
//      Service.
//
// Two finalizers — restore-source-service and restore-source — keep the CR
// alive past its deletionTimestamp so this file's handleDeletion routine can
// rebuild both sides of the snapshot in the correct order:
//
//   - Service first, because its name collides with KSvc-derived resources and
//     therefore has to wait for the KSvc cascade to finish.
//   - Deployment second, because scaling the source back up while the KSvc is
//     still serving traffic would re-introduce the very dual-writer problem
//     the operator was built to prevent.
//
// Every step is idempotent: each Reconcile pass advances the teardown by at
// most one finalizer, and a partial restore is detectable by inspecting which
// finalizers are still present.

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	kservingv1 "knative.dev/serving/pkg/apis/serving/v1"

	ifaasv1alpha1 "github.com/ifbiu/ifaas/api/ifaas/v1alpha1"
)

// requeueAfterChildGC is the back-off applied while we are waiting for an
// owned child object (KSvc, KSvc-derived Service) to disappear from the
// apiserver. Short enough that release feels responsive, long enough that
// we do not hot-loop while the upstream GC does its work.
const requeueAfterChildGC = time.Second

// ensureFinalizers stamps every finalizer the teardown path will need before
// the operator touches any external state. Both finalizers are added in a
// single MergeFrom patch so the apiserver only sees one spec write per
// Reconcile pass; on subsequent reconciles AddFinalizer is a no-op and the
// patch is skipped entirely.
func (r *KnativeAdoptionReconciler) ensureFinalizers(ctx context.Context, a *ifaasv1alpha1.KnativeAdoption) error {
	orig := a.DeepCopy()
	addedSvc := controllerutil.AddFinalizer(a, FinalizerRestoreSourceService)
	addedDep := controllerutil.AddFinalizer(a, FinalizerRestoreSource)
	if !addedSvc && !addedDep {
		return nil
	}
	if err := r.Patch(ctx, a, client.MergeFrom(orig)); err != nil {
		return fmt.Errorf("add finalizers: %w", err)
	}
	return nil
}

// handleDeletion is the deletion-branch reconciler. It is wired into the top
// of Reconcile when the CR has a deletionTimestamp, and runs the two-phase
// restore. Each phase is gated by the corresponding finalizer's presence:
// once a phase finishes, its finalizer is removed and the next Reconcile sees
// only the remaining work.
//
// The function is intentionally chatty about partial progress through the
// status conditions: a CR stuck at "AwaitingChildrenGC" is a hint that the
// upstream cascade has not finished, and "RestoreFailed" surfaces the actual
// error message to anyone running `kubectl describe`.
func (r *KnativeAdoptionReconciler) handleDeletion(ctx context.Context, a *ifaasv1alpha1.KnativeAdoption) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if controllerutil.ContainsFinalizer(a, FinalizerRestoreSourceService) {
		// Drive the cascade ourselves rather than relying solely on
		// background GC: the apiserver does cascade-delete owned children
		// while a parent's finalizers hold, but envtest has no GC at all,
		// and explicitly issuing the delete is idempotent in both worlds.
		if err := r.deleteKSvcIfPresent(ctx, a); err != nil {
			setCondition(a, ifaasv1alpha1.ConditionDegraded, metav1.ConditionTrue, ReasonRestoreFailed, err.Error())
			r.emitDual(ctx, a, eventTypeWarning, EventReasonRestoreFailed,
				fmt.Sprintf("delete ksvc: %v", err))
			return ctrl.Result{}, err
		}

		ksvcGone, err := r.isKSvcGone(ctx, a)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !ksvcGone {
			setReady(a, metav1.ConditionFalse, ReasonAwaitingChildrenGC, "waiting for Knative Service cascade")
			return ctrl.Result{RequeueAfter: requeueAfterChildGC}, nil
		}

		derivedGone, err := r.isDerivedServiceGone(ctx, a)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !derivedGone {
			setReady(a, metav1.ConditionFalse, ReasonAwaitingChildrenGC, "waiting for KSvc-derived Service cascade")
			return ctrl.Result{RequeueAfter: requeueAfterChildGC}, nil
		}

		setReady(a, metav1.ConditionFalse, ReasonRestoringService, "rebuilding pre-adoption Service")
		if err := r.rebuildSnapshotService(ctx, a); err != nil {
			setCondition(a, ifaasv1alpha1.ConditionDegraded, metav1.ConditionTrue, ReasonRestoreFailed, err.Error())
			r.emitDual(ctx, a, eventTypeWarning, EventReasonRestoreFailed,
				fmt.Sprintf("rebuild source service: %v", err))
			return ctrl.Result{}, err
		}
		if err := r.removeFinalizer(ctx, a, FinalizerRestoreSourceService); err != nil {
			return ctrl.Result{}, err
		}
		log.Info("restore-source-service finalizer removed")
	}

	if controllerutil.ContainsFinalizer(a, FinalizerRestoreSource) {
		setReady(a, metav1.ConditionFalse, ReasonRestoringSource, "restoring source Deployment replicas")
		if err := r.restoreDeploymentReplicas(ctx, a); err != nil {
			setCondition(a, ifaasv1alpha1.ConditionDegraded, metav1.ConditionTrue, ReasonRestoreFailed, err.Error())
			r.emitDual(ctx, a, eventTypeWarning, EventReasonRestoreFailed,
				fmt.Sprintf("restore replicas: %v", err))
			return ctrl.Result{}, err
		}
		if err := r.removeFinalizer(ctx, a, FinalizerRestoreSource); err != nil {
			return ctrl.Result{}, err
		}
		log.Info("restore-source finalizer removed; release complete")
		r.emitDual(ctx, a, eventTypeNormal, EventReasonReleased, "adoption released")
	}

	// Both finalizers are gone; the apiserver will physically delete the CR
	// on the next garbage-collection sweep. The status writes above will not
	// reach etcd (the object is already terminating), which is fine — there
	// is no observer left to read them.
	setReady(a, metav1.ConditionFalse, ReasonReleased, "adoption released")
	return ctrl.Result{}, nil
}

// deleteKSvcIfPresent issues a delete for the operator-owned KSvc. It is a
// no-op when the KSvc is already gone; concurrent deletes are tolerated by
// suppressing IsNotFound.
func (r *KnativeAdoptionReconciler) deleteKSvcIfPresent(ctx context.Context, a *ifaasv1alpha1.KnativeAdoption) error {
	ksvc := &kservingv1.Service{}
	ksvc.Namespace = a.Namespace
	ksvc.Name = a.Name
	if err := r.Delete(ctx, ksvc); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete ksvc: %w", err)
	}
	return nil
}

// isKSvcGone reports whether the Knative Service that backs the adoption is
// fully removed from the apiserver. Existence with a deletionTimestamp counts
// as "still here": until the object is physically gone, recreating an
// equally-named core Service from snapshot would race with the cascade.
func (r *KnativeAdoptionReconciler) isKSvcGone(ctx context.Context, a *ifaasv1alpha1.KnativeAdoption) (bool, error) {
	ksvc := &kservingv1.Service{}
	err := r.Get(ctx, types.NamespacedName{Namespace: a.Namespace, Name: a.Name}, ksvc)
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return false, nil
}

// isDerivedServiceGone reports whether the same-name core Service is either
// absent or no longer owned by Knative. The original Service we captured in
// the snapshot was deleted by the swapper; anything sitting at that name now
// is either Knative's KSvc-derived Service (must wait) or a third-party
// recreation (we simply hand the namespace back to the user).
func (r *KnativeAdoptionReconciler) isDerivedServiceGone(ctx context.Context, a *ifaasv1alpha1.KnativeAdoption) (bool, error) {
	svc := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Namespace: a.Namespace, Name: a.Name}, svc)
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if owner := metav1.GetControllerOf(svc); owner != nil && owner.APIVersion == "serving.knative.dev/v1" {
		return false, nil
	}
	return true, nil
}

// rebuildSnapshotService recreates the pre-adoption Service from the spec
// captured in status.sourceSnapshot.service. ClusterIP / ClusterIPs were
// already stripped at snapshot time so the apiserver allocates fresh IPs;
// everything else (ports, selector, sessionAffinity, …) is preserved verbatim
// to give the workload's clients exactly the resource they had before.
//
// AlreadyExists is treated as a successful no-op: a previous restore round
// may have already created the Service, or the user may have recreated it
// out-of-band. Either way the post-condition (a Service of that name exists)
// is satisfied.
func (r *KnativeAdoptionReconciler) rebuildSnapshotService(ctx context.Context, a *ifaasv1alpha1.KnativeAdoption) error {
	if a.Status.SourceSnapshot == nil || a.Status.SourceSnapshot.Service == nil {
		return nil
	}
	spec := a.Status.SourceSnapshot.Service.DeepCopy()
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      a.Name,
			Namespace: a.Namespace,
			Annotations: map[string]string{
				AnnoServiceManagedBy: AnnoServiceManagedByValue,
			},
		},
		Spec: *spec,
	}
	if err := r.Create(ctx, svc); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("rebuild source service: %w", err)
	}
	return nil
}

// restoreDeploymentReplicas writes the snapshotted replica count back onto
// the source Deployment. A missing Deployment (the user deleted the workload
// independently of releasing the adoption) is treated as a clean release —
// there is nothing to restore. A no-op return is also given when the
// Deployment is already at the target value, so repeated reconciles do not
// generate spurious patches.
func (r *KnativeAdoptionReconciler) restoreDeploymentReplicas(ctx context.Context, a *ifaasv1alpha1.KnativeAdoption) error {
	if a.Status.SourceSnapshot == nil || a.Status.SourceSnapshot.Replicas == nil {
		return nil
	}
	dep := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Namespace: a.Namespace, Name: sourceName(a)}, dep)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	target := *a.Status.SourceSnapshot.Replicas
	if dep.Spec.Replicas != nil && *dep.Spec.Replicas == target {
		return nil
	}
	patch := client.MergeFrom(dep.DeepCopy())
	dep.Spec.Replicas = &target
	if err := r.Patch(ctx, dep, patch); err != nil {
		return fmt.Errorf("restore replicas: %w", err)
	}
	return nil
}

// removeFinalizer drops a single finalizer entry. We use Update rather than
// MergeFrom because metav1.ObjectMeta.Finalizers carries a `patchStrategy=
// merge` tag: a strategic merge patch would only insert items, never remove
// them, which is the opposite of what we want here. Update writes the full
// object — safe in the deletion path because no other reconciler is racing
// on this CR's spec/metadata at this point.
func (r *KnativeAdoptionReconciler) removeFinalizer(ctx context.Context, a *ifaasv1alpha1.KnativeAdoption, name string) error {
	if !controllerutil.RemoveFinalizer(a, name) {
		return nil
	}
	if err := r.Update(ctx, a); err != nil {
		return fmt.Errorf("remove finalizer %q: %w", name, err)
	}
	return nil
}
