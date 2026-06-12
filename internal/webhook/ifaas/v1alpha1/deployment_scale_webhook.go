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
	"fmt"
	"net/http"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// DeploymentScaleWebhookPath is the HTTP path the validating webhook for
// the Deployment `scale` subresource is served on. The +kubebuilder marker
// below renders into config/webhook/manifests.yaml; both must move
// together if the path is renamed.
const DeploymentScaleWebhookPath = "/validate-apps-v1-deployments-scale"

// +kubebuilder:webhook:path=/validate-apps-v1-deployments-scale,mutating=false,failurePolicy=fail,sideEffects=None,groups=apps,resources=deployments/scale,verbs=update,versions=v1,name=vdeploymentscale-v1.kb.io,admissionReviewVersions=v1

// DeploymentScaleValidator gates writes to apps/v1 deployments/scale for
// autopilot-adopted workloads. It is a separate handler from
// DeploymentCustomValidator because the K8s admission contract for
// subresources delivers a different object type and shape:
//
//   - The admission request's Object is autoscaling/v1.Scale, not
//     apps/v1.Deployment, so the typed CustomValidator wired to
//     *appsv1.Deployment never matches and never fires.
//   - The Scale object only carries spec.replicas + status. It exposes
//     neither metadata.labels nor metadata.managedFields, so we can
//     reach the autopilot label and the operator's fieldManager only by
//     issuing a side Get against the parent Deployment.
//
// The discriminator that powers the main Deployment webhook (ManagedFields
// gate, defending against OpenAPI defaulting that turns nil replicas into
// ptr(1)) does not apply here: a write to /scale always carries an
// explicit replica count, defaulting cannot synthesise one. The handler
// therefore uses the simpler "0 → >0" shape: any inbound transition that
// shape, on a labelled Deployment, by anyone other than the operator's own
// SA, is real intent and rejected.
//
// Reader is the same uncached APIReader the KnativeAdoption webhook uses,
// for the same reason: admission can run before the manager's informer
// cache populates, and a stale cached miss would dangerously *admit*
// scale-ups by treating the parent Deployment as nonexistent.
//
// ControllerUsername mirrors DeploymentCustomValidator. The S9 restore
// chain may write replicas via /scale (kubectl rollout restart, etc.);
// admitting requests authenticated as the operator unconditionally is the
// only way to keep the finalizer chain unblocked.
type DeploymentScaleValidator struct {
	Reader             client.Reader
	ControllerUsername string
}

// Handle implements admission.Handler.
//
// Decision tree (top-to-bottom, first match wins):
//  1. operator's own SA → admit (S9 restore safety)
//  2. parent Deployment lookup fails (NotFound or transient) → admit;
//     other layers will redo the right thing once the cache catches up
//     and we'd rather over-admit than wedge the apiserver
//  3. parent Deployment is not opted in → admit (defense in depth; the
//     manifest-layer ObjectSelector that protects /validate-apps-v1-deployment
//     does NOT apply to /scale because Scale has no labels for it to match)
//  4. transition is not 0 → >0 → admit (every "real" reduction or no-op
//     stays through; only forbidden transition is reviving from 0)
//  5. otherwise → deny with the same human-facing message the main
//     Deployment validator emits, so users see one consistent error
func (v *DeploymentScaleValidator) Handle(ctx context.Context, req admission.Request) admission.Response {
	if v.ControllerUsername != "" && req.UserInfo.Username == v.ControllerUsername {
		return admission.Allowed("")
	}

	var newScale autoscalingv1.Scale
	if err := json.Unmarshal(req.Object.Raw, &newScale); err != nil {
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("decode new Scale: %w", err))
	}
	var oldScale autoscalingv1.Scale
	if len(req.OldObject.Raw) > 0 {
		if err := json.Unmarshal(req.OldObject.Raw, &oldScale); err != nil {
			return admission.Errored(http.StatusBadRequest, fmt.Errorf("decode old Scale: %w", err))
		}
	}
	if !(oldScale.Spec.Replicas == 0 && newScale.Spec.Replicas > 0) {
		return admission.Allowed("")
	}

	dep := &appsv1.Deployment{}
	err := v.Reader.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: req.Name}, dep)
	switch {
	case apierrors.IsNotFound(err):
		return admission.Allowed("")
	case err != nil:
		return admission.Errored(http.StatusInternalServerError, fmt.Errorf("lookup parent Deployment: %w", err))
	}
	if !hasAutopilotLabel(dep) {
		return admission.Allowed("")
	}
	return admission.Denied(fmt.Sprintf(
		"deployment %s/%s is managed by ifaas autopilot; manual scale-up from 0 is rejected — delete the KnativeAdoption to release the workload",
		req.Namespace, req.Name))
}