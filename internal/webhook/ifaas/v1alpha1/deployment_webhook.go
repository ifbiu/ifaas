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
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// labelEnabled / labelEnabledValue are duplicated here from
// internal/controller/ifaas/labels.go so the webhook package does not
// import the controller package (which would pull in controller-runtime
// reconcilers). The two strings are public API surface; if they change,
// both files change together.
const (
	labelEnabled      = "ifaas.ifbiu.com/knative-autopilot"
	labelEnabledValue = "enabled"
)

// SetupDeploymentWebhookWithManager registers the validating webhook for
// Deployments. The manifest produced by the +kubebuilder:webhook marker
// below covers every Deployment in the cluster; the in-handler label
// check then narrows the effective scope to Deployments that opted in
// via labelEnabled. We chose the in-handler filter (over the marker's
// objectSelector field, which controller-tools does not surface
// directly) so that adding/removing the operator label takes effect
// instantly without re-applying admission manifests.
func SetupDeploymentWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &appsv1.Deployment{}).
		WithValidator(&DeploymentCustomValidator{}).
		Complete()
}

// +kubebuilder:webhook:path=/validate-apps-v1-deployment,mutating=false,failurePolicy=fail,sideEffects=None,groups=apps,resources=deployments,verbs=update,versions=v1,name=vdeployment-v1.kb.io,admissionReviewVersions=v1

// DeploymentCustomValidator gates manual replicas changes on Deployments
// the operator has adopted. Per impl-plan §S8 Q3 we only refuse the
// "0 → >0" transition: scaling further down (or staying at 0) is the
// emergency-stop the user is supposed to reach for, while scaling back
// up bypasses the guard's quiesced state and would re-introduce the very
// race S5/S6 are designed to prevent.
//
// The validator is intentionally stateless: every decision is computed
// from the inbound Deployment object alone. No apiserver round-trip is
// needed, so it adds essentially zero latency to the admission path.
type DeploymentCustomValidator struct{}

// ValidateCreate is a no-op: we only police mutations on already-adopted
// Deployments, and creation is the moment before adoption.
func (v *DeploymentCustomValidator) ValidateCreate(_ context.Context, _ *appsv1.Deployment) (admission.Warnings, error) {
	return nil, nil
}

// ValidateUpdate enforces the 0→>0 ban for opted-in Deployments. Any
// other change (image, env, resources, replicas going down) passes
// straight through; the operator will reconcile against the new spec on
// its own schedule.
func (v *DeploymentCustomValidator) ValidateUpdate(_ context.Context, oldObj, newObj *appsv1.Deployment) (admission.Warnings, error) {
	if !hasAutopilotLabel(newObj) {
		return nil, nil
	}
	oldR := replicas(oldObj)
	newR := replicas(newObj)
	if oldR == 0 && newR > 0 {
		return nil, fmt.Errorf("deployment %s/%s is managed by ifaas autopilot; manual scale-up from 0 is rejected — delete the KnativeAdoption to release the workload",
			newObj.Namespace, newObj.Name)
	}
	return nil, nil
}

// ValidateDelete is a no-op. Cleanup of operator-owned state on delete
// is the finalizer's job (S9), not the webhook's.
func (v *DeploymentCustomValidator) ValidateDelete(_ context.Context, _ *appsv1.Deployment) (admission.Warnings, error) {
	return nil, nil
}

func hasAutopilotLabel(d *appsv1.Deployment) bool {
	return d.Labels[labelEnabled] == labelEnabledValue
}

func replicas(d *appsv1.Deployment) int32 {
	if d.Spec.Replicas == nil {
		return 1
	}
	return *d.Spec.Replicas
}
