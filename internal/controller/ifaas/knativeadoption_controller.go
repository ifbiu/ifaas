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
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kservingv1 "knative.dev/serving/pkg/apis/serving/v1"

	ifaasv1alpha1 "github.com/ifbiu/ifaas/api/ifaas/v1alpha1"
	"github.com/ifbiu/ifaas/internal/translator"
)

// requeueAfterKSvcPending is the back-off applied while a KSvc has been
// applied but its Ready condition is still false. Short enough that adoption
// feels prompt, long enough that we do not hot-loop against KPA.
const requeueAfterKSvcPending = 10 * time.Second

// requeueAfterSourceMissing waits before re-checking a missing Deployment so
// that label-driven creation (S4) or a manual fix can land without busy-looping.
const requeueAfterSourceMissing = 30 * time.Second

// requeueAfterServiceSwap gives the apiserver enough time to finalize the
// deletion of a same-name Service before the reconciler tries to apply the
// KSvc against the same name.
const requeueAfterServiceSwap = 1 * time.Second

// KnativeAdoptionReconciler reconciles a KnativeAdoption object.
type KnativeAdoptionReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// ServiceReady inspects a freshly fetched Knative Service and returns
	// (ready, url). The default reads kservingv1.ServiceConditionReady.
	// Overridable from unit tests where no Knative controller is running.
	ServiceReady func(*kservingv1.Service) (bool, string)
}

// +kubebuilder:rbac:groups=ifaas.ifbiu.com,resources=knativeadoptions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ifaas.ifbiu.com,resources=knativeadoptions/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ifaas.ifbiu.com,resources=knativeadoptions/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;patch;update
// +kubebuilder:rbac:groups=apps,resources=deployments/scale,verbs=get;patch;update
// +kubebuilder:rbac:groups=serving.knative.dev,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;delete;create

// Reconcile implements the adoption pipeline described in
// docs/knative-autopilot-impl-plan.md §S3.
func (r *KnativeAdoptionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx).WithValues("namespace", req.Namespace, "name", req.Name)

	var adoption ifaasv1alpha1.KnativeAdoption
	if err := r.Get(ctx, req.NamespacedName, &adoption); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Snapshot so we can write status with a single patch at the end.
	original := adoption.DeepCopy()

	// S5: ensure the restore-source-service finalizer is present before we
	// touch any external state. Status patches happen at the end of Reconcile,
	// but this is a spec-level mutation so we apply it now via its own patch.
	if adoption.DeletionTimestamp.IsZero() {
		if err := r.ensureRestoreFinalizer(ctx, &adoption); err != nil {
			return ctrl.Result{}, err
		}
		// Re-snapshot after the finalizer patch so the end-of-Reconcile status
		// diff is computed against the latest spec.
		original = adoption.DeepCopy()
	} else {
		// Deletion path is owned by S9. For now we let the controller idle
		// while the CR awaits its restore-source-service finalizer; the
		// teardown logic will be wired up there.
		log.V(1).Info("adoption marked for deletion; skipping reconcile until S9 lands")
		return ctrl.Result{}, nil
	}

	res, reconcileErr := r.reconcileAdoption(ctx, &adoption)

	if statusErr := r.patchStatus(ctx, original, &adoption); statusErr != nil {
		log.Error(statusErr, "failed to patch status")
		if reconcileErr == nil {
			reconcileErr = statusErr
		}
	}
	return res, reconcileErr
}

func (r *KnativeAdoptionReconciler) reconcileAdoption(ctx context.Context, adoption *ifaasv1alpha1.KnativeAdoption) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Step 1: fetch source Deployment.
	dep, found, err := r.getSourceDeployment(ctx, adoption)
	if err != nil {
		setReady(adoption, metav1.ConditionFalse, ReasonReconciling, err.Error())
		return ctrl.Result{}, err
	}
	if !found {
		setCondition(adoption, ifaasv1alpha1.ConditionSourceMissing, metav1.ConditionTrue, ReasonSourceMissing,
			fmt.Sprintf("Deployment %s/%s not found", adoption.Namespace, sourceName(adoption)))
		setCondition(adoption, ifaasv1alpha1.ConditionAdopted, metav1.ConditionFalse, ReasonSourceMissing, "source workload missing")
		setReady(adoption, metav1.ConditionFalse, ReasonSourceMissing, "source workload missing")
		return ctrl.Result{RequeueAfter: requeueAfterSourceMissing}, nil
	}
	apimeta.RemoveStatusCondition(&adoption.Status.Conditions, ifaasv1alpha1.ConditionSourceMissing)

	// Step 2: translate.
	ksvc, err := translator.Translate(dep, adoption)
	if err != nil {
		setCondition(adoption, ifaasv1alpha1.ConditionTranslationDegraded, metav1.ConditionTrue, ReasonTranslationFailed, err.Error())
		setCondition(adoption, ifaasv1alpha1.ConditionAdopted, metav1.ConditionFalse, ReasonTranslationFailed, err.Error())
		setReady(adoption, metav1.ConditionFalse, ReasonTranslationFailed, err.Error())
		// translation errors are terminal until the user changes either the
		// Deployment or the CR; do not requeue, rely on watches.
		log.Info("translation refused", "err", err)
		return ctrl.Result{}, nil
	}
	apimeta.RemoveStatusCondition(&adoption.Status.Conditions, ifaasv1alpha1.ConditionTranslationDegraded)

	// Step 2.5 (S5): handle the same-name Service before we apply the KSvc.
	// SwapRefused short-circuits the pipeline; the operator must not race with
	// a Service the user explicitly wants to keep.
	swap, err := r.swapService(ctx, adoption)
	if err != nil {
		setReady(adoption, metav1.ConditionFalse, ReasonServiceSwapDeleteFail, err.Error())
		return ctrl.Result{}, err
	}
	if swap == SwapRefused {
		setCondition(adoption, ifaasv1alpha1.ConditionAdopted, metav1.ConditionFalse,
			ReasonServiceSwapTypeRefuse, "service adoption refused; KSvc not created")
		setReady(adoption, metav1.ConditionFalse, ReasonServiceSwapTypeRefuse,
			"service adoption refused")
		return ctrl.Result{}, nil
	}
	if swap == SwapTakenOver {
		// The Service has just been deleted; let the apiserver finish the
		// removal before we try to apply the KSvc against the same name.
		// A short requeue is enough — the cache will catch up almost immediately.
		return ctrl.Result{RequeueAfter: requeueAfterServiceSwap}, nil
	}

	// Step 3: own + server-side apply KSvc.
	if err := controllerutil.SetControllerReference(adoption, ksvc, r.Scheme); err != nil {
		return ctrl.Result{}, fmt.Errorf("set owner ref: %w", err)
	}
	if err := applyKnativeService(ctx, r.Client, r.Scheme, ksvc); err != nil {
		setCondition(adoption, ifaasv1alpha1.ConditionDegraded, metav1.ConditionTrue, ReasonServiceApplyFailed, err.Error())
		setReady(adoption, metav1.ConditionFalse, ReasonServiceApplyFailed, err.Error())
		return ctrl.Result{}, err
	}
	apimeta.RemoveStatusCondition(&adoption.Status.Conditions, ifaasv1alpha1.ConditionDegraded)
	setCondition(adoption, ifaasv1alpha1.ConditionAdopted, metav1.ConditionTrue, ReasonAdopted, "KnativeService applied")

	// Re-fetch the live KSvc so we observe Knative-written status fields.
	live := &kservingv1.Service{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: ksvc.Namespace, Name: ksvc.Name}, live); err != nil {
		return ctrl.Result{}, fmt.Errorf("read back ksvc: %w", err)
	}
	ready, url := r.serviceReady()(live)
	adoption.Status.URL = url

	if !ready {
		setCondition(adoption, ifaasv1alpha1.ConditionServiceAdopted, metav1.ConditionFalse, ReasonKnativeServiceNotReady, "waiting for KSvc Ready")
		setReady(adoption, metav1.ConditionFalse, ReasonKnativeServiceNotReady, "KSvc not Ready")
		return ctrl.Result{RequeueAfter: requeueAfterKSvcPending}, nil
	}
	setCondition(adoption, ifaasv1alpha1.ConditionServiceAdopted, metav1.ConditionTrue, ReasonKnativeServiceReady, "KSvc Ready")

	// Step 4: quiesce the source Deployment.
	if err := r.quiesceSource(ctx, adoption, dep); err != nil {
		setCondition(adoption, ifaasv1alpha1.ConditionSourceQuiesced, metav1.ConditionFalse, ReasonSourceScaleFailed, err.Error())
		setReady(adoption, metav1.ConditionFalse, ReasonSourceScaleFailed, err.Error())
		return ctrl.Result{}, err
	}
	setCondition(adoption, ifaasv1alpha1.ConditionSourceQuiesced, metav1.ConditionTrue, ReasonSourceQuiesced, "Deployment scaled to 0")

	setReady(adoption, metav1.ConditionTrue, ReasonAdopted, "adoption complete")
	return ctrl.Result{}, nil
}

func (r *KnativeAdoptionReconciler) getSourceDeployment(ctx context.Context, a *ifaasv1alpha1.KnativeAdoption) (*appsv1.Deployment, bool, error) {
	dep := &appsv1.Deployment{}
	key := types.NamespacedName{Namespace: a.Namespace, Name: sourceName(a)}
	if err := r.Get(ctx, key, dep); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return dep, true, nil
}

func (r *KnativeAdoptionReconciler) quiesceSource(ctx context.Context, a *ifaasv1alpha1.KnativeAdoption, dep *appsv1.Deployment) error {
	current := int32(1)
	if dep.Spec.Replicas != nil {
		current = *dep.Spec.Replicas
	}
	if current == 0 {
		return nil
	}
	// Snapshot replicas before scaling so S9 can restore.
	if a.Status.SourceSnapshot == nil || a.Status.SourceSnapshot.Replicas == nil {
		if a.Status.SourceSnapshot == nil {
			a.Status.SourceSnapshot = &ifaasv1alpha1.SourceSnapshot{}
		}
		snap := current
		a.Status.SourceSnapshot.Replicas = &snap
	}

	patch := client.MergeFrom(dep.DeepCopy())
	zero := int32(0)
	dep.Spec.Replicas = &zero
	if err := r.Patch(ctx, dep, patch); err != nil {
		return fmt.Errorf("scale deployment to zero: %w", err)
	}
	return nil
}

func (r *KnativeAdoptionReconciler) patchStatus(ctx context.Context, original, current *ifaasv1alpha1.KnativeAdoption) error {
	if statusEqual(original, current) {
		return nil
	}
	return r.Status().Patch(ctx, current, client.MergeFrom(original))
}

func statusEqual(a, b *ifaasv1alpha1.KnativeAdoption) bool {
	if a.Status.URL != b.Status.URL {
		return false
	}
	if (a.Status.SourceSnapshot == nil) != (b.Status.SourceSnapshot == nil) {
		return false
	}
	if a.Status.SourceSnapshot != nil && b.Status.SourceSnapshot != nil {
		ar := a.Status.SourceSnapshot.Replicas
		br := b.Status.SourceSnapshot.Replicas
		if (ar == nil) != (br == nil) || (ar != nil && *ar != *br) {
			return false
		}
		if (a.Status.SourceSnapshot.Service == nil) != (b.Status.SourceSnapshot.Service == nil) {
			return false
		}
	}
	if len(a.Status.Conditions) != len(b.Status.Conditions) {
		return false
	}
	for i := range a.Status.Conditions {
		x, y := a.Status.Conditions[i], b.Status.Conditions[i]
		if x.Type != y.Type || x.Status != y.Status || x.Reason != y.Reason || x.Message != y.Message {
			return false
		}
	}
	return true
}

func setCondition(a *ifaasv1alpha1.KnativeAdoption, t string, s metav1.ConditionStatus, reason, msg string) {
	apimeta.SetStatusCondition(&a.Status.Conditions, metav1.Condition{
		Type:               t,
		Status:             s,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: a.Generation,
	})
}

func removeCondition(a *ifaasv1alpha1.KnativeAdoption, t string) {
	apimeta.RemoveStatusCondition(&a.Status.Conditions, t)
}

func setReady(a *ifaasv1alpha1.KnativeAdoption, s metav1.ConditionStatus, reason, msg string) {
	setCondition(a, ifaasv1alpha1.ConditionReady, s, reason, msg)
}

func sourceName(a *ifaasv1alpha1.KnativeAdoption) string {
	if a.Spec.SourceRef.Name != "" {
		return a.Spec.SourceRef.Name
	}
	return a.Name
}

func (r *KnativeAdoptionReconciler) serviceReady() func(*kservingv1.Service) (bool, string) {
	if r.ServiceReady != nil {
		return r.ServiceReady
	}
	return knativeServiceReady
}

// SetupWithManager sets up the controller with the Manager.
// Watches:
//   - KnativeAdoption (primary)
//   - owned KSvc → owner reference fan-out
//   - Deployment with matching name in same namespace → mapDeploymentToAdoptions
//   - Service with matching name in same namespace → mapServiceToAdoptions (S5)
func (r *KnativeAdoptionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&ifaasv1alpha1.KnativeAdoption{}).
		Owns(&kservingv1.Service{}).
		Watches(
			&appsv1.Deployment{},
			handler.EnqueueRequestsFromMapFunc(r.mapDeploymentToAdoptions),
			builder.WithPredicates(),
		).
		Watches(
			&corev1.Service{},
			handler.EnqueueRequestsFromMapFunc(r.mapServiceToAdoptions),
			builder.WithPredicates(),
		).
		Named("ifaas-knativeadoption").
		Complete(r)
}

// mapDeploymentToAdoptions maps a Deployment event to a reconcile request for
// the KnativeAdoption that references it by name in the same namespace. The
// reverse lookup is by indexed list so we tolerate a Deployment event arriving
// before its matching CR is created.
func (r *KnativeAdoptionReconciler) mapDeploymentToAdoptions(ctx context.Context, obj client.Object) []reconcile.Request {
	dep, ok := obj.(*appsv1.Deployment)
	if !ok {
		return nil
	}
	var list ifaasv1alpha1.KnativeAdoptionList
	if err := r.List(ctx, &list, client.InNamespace(dep.Namespace)); err != nil {
		return nil
	}
	out := make([]reconcile.Request, 0, 1)
	for i := range list.Items {
		a := &list.Items[i]
		if sourceName(a) == dep.Name {
			out = append(out, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: a.Namespace, Name: a.Name}})
		}
	}
	return out
}

// mapServiceToAdoptions wakes the reconciler whenever a same-name core/Service
// is created or deleted in a namespace that hosts a KnativeAdoption. The match
// is by exact name because the operator always uses the CR name as the KSvc
// name and the KSvc-derived Service inherits that name.
func (r *KnativeAdoptionReconciler) mapServiceToAdoptions(ctx context.Context, obj client.Object) []reconcile.Request {
	svc, ok := obj.(*corev1.Service)
	if !ok {
		return nil
	}
	a := &ifaasv1alpha1.KnativeAdoption{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: svc.Namespace, Name: svc.Name}, a); err != nil {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: a.Namespace, Name: a.Name}}}
}
