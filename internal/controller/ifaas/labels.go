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

// Labels and annotations are the public contract between cluster users and the
// operator. Anything here is consumed by:
//   - the DeploymentWatcher (S4) to decide whether to adopt a workload,
//   - the projector to map Deployment annotations onto KnativeAdoption.Spec,
//   - the validating webhook (S8) to reject user-managed mutations.
const (
	// LabelEnabled selects Deployments the operator should adopt.
	// Setting the value to LabelEnabledValue opts the Deployment in.
	LabelEnabled      = "ifaas.ifbiu.com/knative-autopilot"
	LabelEnabledValue = "enabled"

	// LabelManagedByWatcher is stamped on KnativeAdoption objects the
	// DeploymentWatcher created from a labeled Deployment. The watcher only
	// ever mutates or deletes adoptions carrying this label, so user-authored
	// CRs are left untouched.
	LabelManagedByWatcher      = "ifaas.ifbiu.com/managed-by-watcher"
	LabelManagedByWatcherValue = "true"

	// FieldOwnerWatcher is the server-side-apply field manager used by the
	// DeploymentWatcher. It must be distinct from FieldOwner (the adoption
	// reconciler) so the two controllers do not steal fields from each other.
	FieldOwnerWatcher = "ifaas-watcher"
)

// Annotation keys carried on the source Deployment. The DeploymentWatcher
// projects each known key onto the corresponding KnativeAdoption.Spec field.
// Unknown keys are ignored.
const (
	AnnoMode             = "ifaas.ifbiu.com/mode"
	AnnoPrimaryContainer = "ifaas.ifbiu.com/primary-container"

	AnnoMinScale          = "ifaas.ifbiu.com/min-scale"
	AnnoMaxScale          = "ifaas.ifbiu.com/max-scale"
	AnnoTargetConcurrency = "ifaas.ifbiu.com/target-concurrency"

	AnnoScaleDownPath             = "ifaas.ifbiu.com/scaledown-path"
	AnnoScaleDownPort             = "ifaas.ifbiu.com/scaledown-port"
	AnnoScaleDownIntervalSeconds  = "ifaas.ifbiu.com/scaledown-interval-seconds"
	AnnoScaleDownTimeoutSeconds   = "ifaas.ifbiu.com/scaledown-timeout-seconds"
	AnnoScaleDownFailureThreshold = "ifaas.ifbiu.com/scaledown-failure-threshold"

	AnnoEventingBroker = "ifaas.ifbiu.com/eventing-broker"
)
