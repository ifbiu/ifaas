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

// Package v1alpha1 hosts the admission webhooks for the ifaas.ifbiu.com
// v1alpha1 API. The package owns two webhook entry points:
//
//  1. KnativeAdoption webhook — defaulting + validation of the operator's
//     primary CR. See impl-plan §S8 Q1/Q4/Q5 for the decisions encoded
//     below.
//
//  2. Deployment webhook (deployment_webhook.go) — narrow validation of
//     Deployments that opted in via the ifaas.ifbiu.com/knative-autopilot
//     label. Constrained by the webhook's ObjectSelector at registration
//     time, so unrelated Deployments never even reach the handler.
//
// Both webhooks hold a client.Client so they can cross-reference live
// apiserver state during admission (Deployment existence, HPA targetRef
// scan, etc.). Production injects mgr.GetClient(); envtests inject the
// suite's k8sClient.
package v1alpha1

import (
	"context"
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	ifaasv1alpha1 "github.com/ifbiu/ifaas/api/ifaas/v1alpha1"
)

// log is for logging in this package.
//
//nolint:unused
var knativeadoptionlog = logf.Log.WithName("knativeadoption-resource")

// Reserved label / annotation keys the operator owns. Users cannot author
// these on CRs; the webhook rejects writes that include them so reconciler
// state ownership stays uncontested.
const (
	ReservedLabelManagedBy    = "ifaas.ifbiu.com/managed-by"
	ReservedLabelOwner        = "ifaas.ifbiu.com/owner"
	ReservedAnnoManagedBy     = "ifaas.ifbiu.com/managed-by"
	ReservedAnnoPrimaryWatchr = "ifaas.ifbiu.com/managed-by-watcher"
)

// SetupKnativeAdoptionWebhookWithManager registers the webhook for KnativeAdoption in the manager.
//
// The validator is wired to mgr.GetAPIReader() rather than mgr.GetClient()
// on purpose: admission requests can arrive the moment the apiserver
// learns about the webhook endpoint, which is *before* the manager's
// informer cache has had a chance to populate. A cache-backed read would
// then mis-report freshly created Deployments / HPAs as missing and
// reject otherwise valid CRs. The APIReader bypasses the cache and goes
// straight to the apiserver, which is the right consistency model for an
// admission gate: we want to see what the apiserver sees right now, not
// a possibly-stale local copy.
func SetupKnativeAdoptionWebhookWithManager(mgr ctrl.Manager) error {
	reader := mgr.GetAPIReader()
	if err := (ctrl.NewWebhookManagedBy(mgr, &ifaasv1alpha1.KnativeAdoption{}).
		WithValidator(&KnativeAdoptionCustomValidator{Reader: reader}).
		WithDefaulter(&KnativeAdoptionCustomDefaulter{}).
		Complete()); err != nil {
		return err
	}
	return SetupDeploymentWebhookWithManager(mgr)
}

// +kubebuilder:webhook:path=/mutate-ifaas-ifbiu-com-v1alpha1-knativeadoption,mutating=true,failurePolicy=fail,sideEffects=None,groups=ifaas.ifbiu.com,resources=knativeadoptions,verbs=create;update,versions=v1alpha1,name=mknativeadoption-v1alpha1.kb.io,admissionReviewVersions=v1

// KnativeAdoptionCustomDefaulter applies the defaulting rules that are
// inconvenient to express through kubebuilder markers (e.g. cross-field
// inference such as "default sourceRef.kind=Deployment when omitted").
type KnativeAdoptionCustomDefaulter struct{}

// Default implements webhook.CustomDefaulter for KnativeAdoption.
//
// kubebuilder markers already default Mode → "serving",
// SourceRef.Kind → "Deployment", Autoscaling.MinScale → 0, and the
// ScaleDownProbe knobs. This defaulter therefore only fills in
// `SourceRef.Namespace` because that field has no static default — it must
// inherit from the CR's metadata.namespace, which the CRD schema cannot
// reference.
func (d *KnativeAdoptionCustomDefaulter) Default(_ context.Context, obj *ifaasv1alpha1.KnativeAdoption) error {
	if obj.Spec.SourceRef.Namespace == "" {
		obj.Spec.SourceRef.Namespace = obj.Namespace
	}
	return nil
}

// +kubebuilder:webhook:path=/validate-ifaas-ifbiu-com-v1alpha1-knativeadoption,mutating=false,failurePolicy=fail,sideEffects=None,groups=ifaas.ifbiu.com,resources=knativeadoptions,verbs=create;update,versions=v1alpha1,name=vknativeadoption-v1alpha1.kb.io,admissionReviewVersions=v1

// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch
// +kubebuilder:rbac:groups=autoscaling,resources=horizontalpodautoscalers,verbs=get;list;watch

// KnativeAdoptionCustomValidator validates KnativeAdoption against live
// apiserver state. It is read-only: it MUST NOT mutate the cluster, so all
// cluster lookups go through the (uncached) Reader and are tolerant of
// IsNotFound (treated as "object does not exist" rather than transient
// errors).
type KnativeAdoptionCustomValidator struct {
	Reader client.Reader
}

// ValidateCreate is invoked at object creation. It is the canonical place
// to enforce "this CR can be admitted into the cluster".
func (v *KnativeAdoptionCustomValidator) ValidateCreate(ctx context.Context, obj *ifaasv1alpha1.KnativeAdoption) (admission.Warnings, error) {
	return nil, v.validateSpec(ctx, obj)
}

// ValidateUpdate enforces immutability of the adoption identity in
// addition to the structural rules. Once a CR adopted a Deployment, its
// `sourceRef` is frozen: changing it would silently rewire the operator
// onto a different workload and leave the prior Deployment dangling in
// "scaled to zero" without a ledger.
func (v *KnativeAdoptionCustomValidator) ValidateUpdate(ctx context.Context, oldObj, newObj *ifaasv1alpha1.KnativeAdoption) (admission.Warnings, error) {
	if oldObj.Spec.SourceRef != newObj.Spec.SourceRef {
		return nil, fmt.Errorf("spec.sourceRef is immutable: %+v → %+v", oldObj.Spec.SourceRef, newObj.Spec.SourceRef)
	}
	// Once the CR enters deletion, the operator's finalizer chain (S9)
	// issues spec-preserving Update calls to peel off its own finalizers
	// after restoring source state. By that point the source Deployment
	// may legitimately be gone (e.g. namespace-cascade delete, or
	// user-initiated cleanup that triggered the adoption to unwind).
	// Re-running validateSpec against post-restore state would re-fail
	// checkSourceExists / checkNoHPA and trap the CR in a webhook-vs-
	// finalizer deadlock. Immutability of sourceRef is still enforced
	// above, so this short-circuit only loosens the live-state checks.
	if newObj.DeletionTimestamp != nil {
		return nil, nil
	}
	return nil, v.validateSpec(ctx, newObj)
}

// ValidateDelete is a no-op in M1: the deletion lifecycle is managed by
// finalizers (S9). The webhook does not need to gate deletes.
func (v *KnativeAdoptionCustomValidator) ValidateDelete(_ context.Context, _ *ifaasv1alpha1.KnativeAdoption) (admission.Warnings, error) {
	return nil, nil
}

// validateSpec runs the four structural admission checks.
//
// Order matters: cheap checks (reserved labels, eventing struct) come
// before apiserver lookups (sourceRef existence, HPA scan) so the
// webhook can short-circuit invalid payloads without paying for a list.
func (v *KnativeAdoptionCustomValidator) validateSpec(ctx context.Context, a *ifaasv1alpha1.KnativeAdoption) error {
	if err := checkReservedKeys(a); err != nil {
		return err
	}
	if err := checkEventing(a); err != nil {
		return err
	}
	dep, err := v.checkSourceExists(ctx, a)
	if err != nil {
		return err
	}
	if err := checkPrimaryContainer(a, dep); err != nil {
		return err
	}
	if err := v.checkNoHPA(ctx, a, dep); err != nil {
		return err
	}
	return nil
}

// checkReservedKeys rejects user-authored CRs that try to occupy label /
// annotation keys reserved for the operator. This keeps the
// "label-driven auto-adopt" path (S4) the single writer of those keys and
// prevents an external actor from forging managed-by ownership.
func checkReservedKeys(a *ifaasv1alpha1.KnativeAdoption) error {
	for _, k := range []string{ReservedLabelManagedBy, ReservedLabelOwner} {
		if _, ok := a.Labels[k]; ok {
			return fmt.Errorf("label %q is reserved for the operator", k)
		}
	}
	for _, k := range []string{ReservedAnnoManagedBy, ReservedAnnoPrimaryWatchr} {
		if _, ok := a.Annotations[k]; ok {
			return fmt.Errorf("annotation %q is reserved for the operator", k)
		}
	}
	return nil
}

// checkEventing implements the Q5 decision: only enforce broker non-empty
// when mode=eventing. Broker existence and Trigger structure are
// deferred to M2 where the eventing reconciler lands; checking them here
// would lock the CRD into shape decisions we haven't validated yet.
func checkEventing(a *ifaasv1alpha1.KnativeAdoption) error {
	if a.Spec.Mode != ifaasv1alpha1.ModeEventing {
		return nil
	}
	if a.Spec.Eventing == nil || strings.TrimSpace(a.Spec.Eventing.Broker) == "" {
		return fmt.Errorf("spec.eventing.broker is required when spec.mode=eventing")
	}
	return nil
}

// checkSourceExists implements Q1: the referenced Deployment must already
// exist when the CR is admitted. Returning the resolved Deployment lets
// downstream checks (PrimaryContainer, HPA) avoid a second Get.
func (v *KnativeAdoptionCustomValidator) checkSourceExists(ctx context.Context, a *ifaasv1alpha1.KnativeAdoption) (*appsv1.Deployment, error) {
	if a.Spec.SourceRef.Kind != "" && a.Spec.SourceRef.Kind != ifaasv1alpha1.SourceKindDeployment {
		return nil, fmt.Errorf("spec.sourceRef.kind=%q is not supported in M1 (only Deployment)", a.Spec.SourceRef.Kind)
	}
	if a.Spec.SourceRef.Name == "" {
		return nil, fmt.Errorf("spec.sourceRef.name is required")
	}
	ns := a.Spec.SourceRef.Namespace
	if ns == "" {
		ns = a.Namespace
	}
	if ns != a.Namespace {
		return nil, fmt.Errorf("cross-namespace adoption is not supported in M1 (sourceRef.namespace=%q, CR namespace=%q)", ns, a.Namespace)
	}

	dep := &appsv1.Deployment{}
	if err := v.Reader.Get(ctx, types.NamespacedName{Namespace: ns, Name: a.Spec.SourceRef.Name}, dep); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("spec.sourceRef points at Deployment %s/%s which does not exist", ns, a.Spec.SourceRef.Name)
		}
		return nil, fmt.Errorf("lookup source Deployment: %w", err)
	}
	return dep, nil
}

// checkPrimaryContainer enforces the multi-container rule (Q5 / design §):
// if the source Deployment has more than one container, the CR must name
// the primary one. The translator (S2) returns ErrAmbiguousPrimary
// post-admission for the same condition, so this is a friendlier early
// rejection.
func checkPrimaryContainer(a *ifaasv1alpha1.KnativeAdoption, dep *appsv1.Deployment) error {
	containers := dep.Spec.Template.Spec.Containers
	if a.Spec.PrimaryContainer != "" {
		for _, c := range containers {
			if c.Name == a.Spec.PrimaryContainer {
				return nil
			}
		}
		return fmt.Errorf("spec.primaryContainer=%q not found in source Deployment containers", a.Spec.PrimaryContainer)
	}
	if len(containers) > 1 {
		return fmt.Errorf("source Deployment has %d containers; spec.primaryContainer must be set", len(containers))
	}
	return nil
}

// checkNoHPA implements Q2: reject if any autoscaling/v2 HPA in the same
// namespace targets the source Deployment. An HPA writes Deployment.spec.
// replicas on its own schedule; combining it with the operator's quiesce-
// to-zero would cause a write-write race the user cannot reason about.
func (v *KnativeAdoptionCustomValidator) checkNoHPA(ctx context.Context, a *ifaasv1alpha1.KnativeAdoption, dep *appsv1.Deployment) error {
	var list autoscalingv2.HorizontalPodAutoscalerList
	if err := v.Reader.List(ctx, &list, client.InNamespace(a.Namespace)); err != nil {
		return fmt.Errorf("scan HPAs: %w", err)
	}
	for i := range list.Items {
		t := list.Items[i].Spec.ScaleTargetRef
		if t.Kind != "Deployment" {
			continue
		}
		if t.APIVersion != "" && t.APIVersion != "apps/v1" {
			continue
		}
		if t.Name != dep.Name {
			continue
		}
		return fmt.Errorf("source Deployment %s/%s is the scale target of HPA %q; remove the HPA before adopting",
			dep.Namespace, dep.Name, list.Items[i].Name)
	}
	return nil
}
