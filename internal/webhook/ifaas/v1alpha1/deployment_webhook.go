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
	"os"

	appsv1 "k8s.io/api/apps/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
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

// fieldOwner* mirror the SSA fieldManager names used by the operator's
// reconcilers (controller package: FieldOwner / FieldOwnerWatcher).
// Duplicated for the same reason as labelEnabled above. The webhook uses
// these names as the canonical anchor for "is spec.replicas currently
// owned by ifaas itself?": any other manager wins ownership only when a
// caller has explicitly declared the field, which is the discriminator
// the validator needs to tell phantom defaulting from real intent.
const (
	fieldOwnerAutopilot = "ifaas-autopilot"
	fieldOwnerWatcher   = "ifaas-watcher"
)

// envControllerUsername names the env var that pins the operator's own
// kube-apiserver username so the Deployment validator can recognise its
// own restore writes (S9) and let them through the 0→>0 ban. The default
// matches the SA emitted by config/default/kustomization (namespace
// prefix `ifaas-` + SA name `controller-manager`); production deployments
// that rename either component must override the env var to keep the
// finalizer chain unblocked.
const (
	envControllerUsername     = "IFAAS_CONTROLLER_USERNAME"
	defaultControllerUsername = "system:serviceaccount:ifaas-system:ifaas-controller-manager"
)

// SetupDeploymentWebhookWithManager registers the validating webhook for
// Deployments. The +kubebuilder:webhook marker below covers every
// Deployment in the cluster at the API level; the actual scope is then
// narrowed to opted-in workloads in two layers:
//
//   - manifest layer: a kustomize patch under config/webhook/patches
//     injects an objectSelector that matches labelEnabled=labelEnabledValue,
//     so non-adopted Deployments (including the ifaas controller-manager
//     itself) never reach the webhook server. This is what prevents the
//     classic "webhook self-deadlock" during rollout when the webhook
//     Service has no ready endpoints.
//   - handler layer: ValidateUpdate below also checks the label, so a
//     mis-edited admission manifest can never accidentally police more
//     than the operator owns.
func SetupDeploymentWebhookWithManager(mgr ctrl.Manager) error {
	user := os.Getenv(envControllerUsername)
	if user == "" {
		user = defaultControllerUsername
	}
	if err := ctrl.NewWebhookManagedBy(mgr, &appsv1.Deployment{}).
		WithValidator(&DeploymentCustomValidator{ControllerUsername: user}).
		Complete(); err != nil {
		return err
	}
	// Subresource webhook for /scale. Cannot be expressed via
	// NewWebhookManagedBy because controller-runtime keys the validator on
	// the Go type *appsv1.Deployment, while the apiserver delivers
	// autoscaling/v1.Scale on this path. We register the raw admission
	// handler on the same webhook server so both endpoints share the cert
	// material and lifecycle.
	mgr.GetWebhookServer().Register(DeploymentScaleWebhookPath, &webhook.Admission{
		Handler: &DeploymentScaleValidator{
			Reader:             mgr.GetAPIReader(),
			ControllerUsername: user,
		},
	})
	return nil
}

// +kubebuilder:webhook:path=/validate-apps-v1-deployment,mutating=false,failurePolicy=fail,sideEffects=None,groups=apps,resources=deployments,verbs=update,versions=v1,name=vdeployment-v1.kb.io,admissionReviewVersions=v1

// DeploymentCustomValidator gates manual replicas changes on Deployments
// the operator has adopted. Per impl-plan §S8 Q3 we only refuse the
// "0 → >0" transition: scaling further down (or staying at 0) is the
// emergency-stop the user is supposed to reach for, while scaling back
// up bypasses the guard's quiesced state and would re-introduce the very
// race S5/S6 are designed to prevent.
//
// Discriminator — phantom defaulting vs real intent:
//
// The naive "oldReplicas==0 && newReplicas>0" check breaks under the
// K8s admission pipeline: PATCH operations are decoded → defaulted →
// (mutating webhooks) → validated, and the OpenAPI defaulter rewrites
// a nil Deployment.spec.replicas to ptr(1) before this validator ever
// runs. A client-side `kubectl apply` that removes the field from its
// last-applied configuration sends a strategic-merge patch with
// `{"spec":{"replicas":null}}` whose only effect is to relinquish the
// caller's ownership of the field; the apiserver's default-fill makes
// the resulting object look identical to a deliberate "scale to 1",
// even though the caller has expressed no such intent. GitOps tools
// (Argo with ignoreDifferences, fluxcd with --dry-run=server, …) hit
// this corner all the time, and end up self-blocked against their
// own correct reconciliation.
//
// We resolve the ambiguity by reading newObj.ManagedFields:
// `f:spec.f:replicas` is owned by the *last* fieldManager that wrote
// it, and ownership can only transfer when a client declares the
// field in its patch. If ownership is still held by the operator's
// own fieldManagers (FieldOwner / FieldOwnerWatcher) the inbound
// request did not declare replicas — the 0→>0 we're seeing is
// defaulting, not intent, and the request is admitted. If ownership
// has moved to any other manager (kubectl-*, argocd-controller, a
// human's CLI) we honour the original ban.
//
// ControllerUsername names the operator's own kube-apiserver username.
// The S9 restore chain — which intentionally writes 0→>=1 to revive the
// source workload after a CR delete — would otherwise be self-blocked by
// the same rule it enforces. Requests whose UserInfo matches this
// username are admitted unconditionally; every other client (humans,
// HPAs, GitOps controllers) still goes through the managed-fields gate.
//
// The validator is otherwise stateless: every decision is computed from
// the inbound Deployment plus the AdmissionRequest in ctx. No apiserver
// round-trip is needed, so it adds essentially zero latency to the
// admission path.
type DeploymentCustomValidator struct {
	ControllerUsername string
}

// ValidateCreate is a no-op: we only police mutations on already-adopted
// Deployments, and creation is the moment before adoption.
func (v *DeploymentCustomValidator) ValidateCreate(_ context.Context, _ *appsv1.Deployment) (admission.Warnings, error) {
	return nil, nil
}

// ValidateUpdate enforces the 0→>0 ban for opted-in Deployments. Any
// other change (image, env, resources, replicas going down) passes
// straight through; the operator will reconcile against the new spec on
// its own schedule.
//
// The operator's own restore writes (S9 finalizer chain) are admitted
// regardless of the replica delta: identifying them by UserInfo is the
// only way to distinguish a legitimate restore from a user trying to
// undo the quiesce by hand.
func (v *DeploymentCustomValidator) ValidateUpdate(ctx context.Context, oldObj, newObj *appsv1.Deployment) (admission.Warnings, error) {
	if !hasAutopilotLabel(newObj) {
		return nil, nil
	}
	if v.ControllerUsername != "" {
		if req, err := admission.RequestFromContext(ctx); err == nil &&
			req.UserInfo.Username == v.ControllerUsername {
			return nil, nil
		}
	}
	oldR := replicas(oldObj)
	newR := replicas(newObj)
	if !(oldR == 0 && newR > 0) {
		return nil, nil
	}
	// 0→>0 is necessary but not sufficient. Only count it as manual
	// scale-up when an external manager actually owns the field.
	if !replicasOwnedByExternalManager(newObj) {
		return nil, nil
	}
	return nil, fmt.Errorf("deployment %s/%s is managed by ifaas autopilot; manual scale-up from 0 is rejected — delete the KnativeAdoption to release the workload",
		newObj.Namespace, newObj.Name)
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

// replicasOwnedByExternalManager reports whether spec.replicas is held
// by any fieldManager other than the operator's own. A `true` here
// means the inbound request — or some earlier write the inbound
// request did not undo — explicitly declared the field, so the
// 0→>0 transition we observe is real intent.
//
// A `false` here covers two harmless cases:
//
//   - the operator is the only owner (the steady state after adoption),
//     so the request did not touch replicas;
//   - nobody owns the field at all (rare, transitional), in which case
//     we admit and let the next reconcile re-stamp ownership.
//
// Multiple entries can claim the same field under SSA (set semantics),
// so a single external owner is enough to fail the check. We do not
// look at entry timestamps: ownership transfer is the apiserver's job,
// and by the time admission runs newObj.ManagedFields already reflects
// the post-patch ownership state.
func replicasOwnedByExternalManager(d *appsv1.Deployment) bool {
	for _, e := range d.ManagedFields {
		if e.FieldsV1 == nil || len(e.FieldsV1.Raw) == 0 {
			continue
		}
		if !entryOwnsSpecReplicas(e.FieldsV1.Raw) {
			continue
		}
		switch e.Manager {
		case fieldOwnerAutopilot, fieldOwnerWatcher:
			continue
		default:
			return true
		}
	}
	return false
}

// entryOwnsSpecReplicas decodes the FieldsV1 tree of a single
// ManagedFieldsEntry and reports whether it claims `spec.replicas`.
// The encoding is the K8s "set" notation: every owned scalar shows up
// as `"f:<name>": {}` under nested `"f:<parent>"` maps. We only need
// to walk two levels: f:spec → f:replicas.
func entryOwnsSpecReplicas(raw []byte) bool {
	var tree map[string]json.RawMessage
	if err := json.Unmarshal(raw, &tree); err != nil {
		return false
	}
	specRaw, ok := tree["f:spec"]
	if !ok {
		return false
	}
	var spec map[string]json.RawMessage
	if err := json.Unmarshal(specRaw, &spec); err != nil {
		return false
	}
	_, ok = spec["f:replicas"]
	return ok
}