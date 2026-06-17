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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// Mode controls which Knative components the operator wires up for an adoption.
//
// +kubebuilder:validation:Enum=serving;eventing
type Mode string

const (
	// ModeServing creates a Knative Service only.
	ModeServing Mode = "serving"
	// ModeEventing creates a Knative Service plus Trigger(s) bound to a Broker.
	ModeEventing Mode = "eventing"
)

// SourceKind enumerates the workload kinds that the operator may adopt.
// M1 only supports Deployment.
//
// +kubebuilder:validation:Enum=Deployment
type SourceKind string

const (
	SourceKindDeployment SourceKind = "Deployment"
)

// ProbeResult is the tri-state outcome of a /scaledownz guard cycle.
//
// Values are capitalised on purpose: kubebuilder marker `Enum=true;false;...`
// emits OpenAPI booleans for `true`/`false`, which the apiserver then refuses
// to match against a string field. Picking `True` / `False` / `Unknown`
// sidesteps the YAML type-inference trap and matches Kubernetes convention
// for status enums (e.g. corev1.ConditionStatus).
//
// +kubebuilder:validation:Enum=True;False;Unknown
type ProbeResult string

const (
	ProbeResultTrue    ProbeResult = "True"
	ProbeResultFalse   ProbeResult = "False"
	ProbeResultUnknown ProbeResult = "Unknown"
)

// Condition type constants written into KnativeAdoption.status.conditions.
const (
	ConditionAdopted               = "Adopted"
	ConditionSourceQuiesced        = "SourceQuiesced"
	ConditionServiceAdopted        = "ServiceAdopted"
	ConditionScaleDownAllowed      = "ScaleDownAllowed"
	ConditionEventingReady         = "EventingReady"
	ConditionTrafficReady          = "TrafficReady"
	ConditionTrafficDegraded       = "TrafficDegraded"
	ConditionReady                 = "Ready"
	ConditionDegraded              = "Degraded"
	ConditionSourceMissing         = "SourceMissing"
	ConditionTranslationDegraded   = "TranslationDegraded"
	ConditionServiceAdoptionRefuse = "ServiceAdoptionRefused"
)

// SourceRef references the workload that the operator should adopt.
type SourceRef struct {
	// Kind is the source workload kind. M1 only supports "Deployment".
	// +kubebuilder:default=Deployment
	// +optional
	Kind SourceKind `json:"kind,omitempty"`

	// Name of the source workload (same namespace as the KnativeAdoption CR).
	// +required
	Name string `json:"name"`

	// Namespace of the source workload. Defaults to the CR's namespace.
	// Cross-namespace adoption is not supported in M1; this field is reserved.
	// +optional
	Namespace string `json:"namespace,omitempty"`
}

// Autoscaling is transcribed into KPA annotations on the generated Knative Service.
type Autoscaling struct {
	// MinScale is the lower bound the operator may set on autoscaling.knative.dev/min-scale.
	// Defaults to 0 — requires the workload to implement /scaledownz.
	// +kubebuilder:default=0
	// +kubebuilder:validation:Minimum=0
	// +optional
	MinScale *int32 `json:"minScale,omitempty"`

	// MaxScale caps autoscaling.knative.dev/max-scale.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxScale *int32 `json:"maxScale,omitempty"`

	// TargetConcurrency maps to autoscaling.knative.dev/target.
	// +kubebuilder:validation:Minimum=1
	// +optional
	TargetConcurrency *int32 `json:"targetConcurrency,omitempty"`
}

// ScaleDownProbe configures the /scaledownz guard described in design §8.5.
type ScaleDownProbe struct {
	// Path on the workload that returns a boolean "allow scale-to-zero".
	// +kubebuilder:default=/scaledownz
	// +optional
	Path string `json:"path,omitempty"`

	// Port on the pod to probe. Defaults to the container user-port.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +optional
	Port *int32 `json:"port,omitempty"`

	// IntervalSeconds is the guard polling interval.
	// Must be <= KPA stable-window/2 (Knative default stable-window = 60s).
	// +kubebuilder:default=30
	// +kubebuilder:validation:Minimum=1
	// +optional
	IntervalSeconds int32 `json:"intervalSeconds,omitempty"`

	// TimeoutSeconds bounds a single proxy probe call.
	// +kubebuilder:default=2
	// +kubebuilder:validation:Minimum=1
	// +optional
	TimeoutSeconds int32 `json:"timeoutSeconds,omitempty"`

	// ConsecutiveFailureThreshold raises Degraded after this many back-to-back failures.
	// +kubebuilder:default=20
	// +kubebuilder:validation:Minimum=1
	// +optional
	ConsecutiveFailureThreshold int32 `json:"consecutiveFailureThreshold,omitempty"`
}

// Eventing wires the adopted KSvc to one or more Triggers (mode=eventing only).
type Eventing struct {
	// Broker name (same namespace) that the operator-owned Triggers subscribe to.
	// +required
	Broker string `json:"broker"`

	// Filters defines one Trigger per entry; empty list creates a single
	// pass-through Trigger.
	// +optional
	Filters []EventingFilter `json:"filters,omitempty"`
}

// EventingFilter mirrors the attribute filter of a Knative Trigger.
type EventingFilter struct {
	// Type filters CloudEvents by ce-type.
	// +optional
	Type string `json:"type,omitempty"`

	// Source filters CloudEvents by ce-source.
	// +optional
	Source string `json:"source,omitempty"`
}

// TrafficMode controls how the operator reconciles externally managed traffic objects.
//
// +kubebuilder:validation:Enum=inplace
type TrafficMode string

const (
	// TrafficModeInplace mutates declared traffic objects in place.
	TrafficModeInplace TrafficMode = "inplace"
)

// NamespacedObjectRef references an object in the same namespace as the CR.
type NamespacedObjectRef struct {
	// Name is the object name.
	// +required
	Name string `json:"name"`
}

// IstioTraffic lists the Istio objects that traffic adaptation may reconcile.
type IstioTraffic struct {
	// VirtualServiceRefs points at VirtualService objects to reconcile.
	// +optional
	VirtualServiceRefs []NamespacedObjectRef `json:"virtualServiceRefs,omitempty"`

	// DestinationRuleRefs points at DestinationRule objects to reconcile.
	// +optional
	DestinationRuleRefs []NamespacedObjectRef `json:"destinationRuleRefs,omitempty"`
}

// Traffic configures optional Istio traffic adaptation for this adoption.
type Traffic struct {
	// Enabled toggles traffic adaptation.
	// +kubebuilder:default=false
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// Mode selects how declared traffic objects are reconciled.
	// +kubebuilder:default=inplace
	// +optional
	Mode TrafficMode `json:"mode,omitempty"`

	// Istio lists concrete traffic objects to adapt.
	// +optional
	Istio *IstioTraffic `json:"istio,omitempty"`

	// ServicePort is the service port traffic adaptation targets.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +optional
	ServicePort *int32 `json:"servicePort,omitempty"`
}

// KnativeAdoptionSpec defines the desired state of KnativeAdoption.
type KnativeAdoptionSpec struct {
	// SourceRef points at the Deployment to adopt.
	// +required
	SourceRef SourceRef `json:"sourceRef"`

	// Mode selects serving-only or serving+eventing behaviour.
	// +kubebuilder:default=serving
	// +optional
	Mode Mode `json:"mode,omitempty"`

	// PrimaryContainer is required when the source Deployment has more than one
	// container; the named container is mapped to the KSvc user-container.
	// +optional
	PrimaryContainer string `json:"primaryContainer,omitempty"`

	// Autoscaling configures KPA annotations on the generated KSvc.
	// +optional
	Autoscaling Autoscaling `json:"autoscaling,omitzero"`

	// ScaleDownProbe configures the /scaledownz guard.
	// +optional
	ScaleDownProbe ScaleDownProbe `json:"scaleDownProbe,omitzero"`

	// Eventing is only honoured when mode=eventing.
	// +optional
	Eventing *Eventing `json:"eventing,omitempty"`

	// Traffic configures optional Istio traffic adaptation.
	// +optional
	Traffic *Traffic `json:"traffic,omitempty"`
}

// TrafficObjectSnapshot captures one traffic object's pre-adoption spec.
type TrafficObjectSnapshot struct {
	// Name is the traffic object name.
	// +optional
	Name string `json:"name,omitempty"`

	// Spec is the object's original spec payload before ifaas mutates it.
	// +optional
	Spec runtime.RawExtension `json:"spec,omitempty"`
}

// TrafficSnapshot captures pre-adoption traffic objects for restore.
type TrafficSnapshot struct {
	// VirtualServices stores pre-adoption VirtualService specs.
	// +optional
	VirtualServices []TrafficObjectSnapshot `json:"virtualServices,omitempty"`

	// DestinationRules stores pre-adoption DestinationRule specs.
	// +optional
	DestinationRules []TrafficObjectSnapshot `json:"destinationRules,omitempty"`
}

// SourceSnapshot captures pre-adoption state used during teardown.
type SourceSnapshot struct {
	// Replicas is the Deployment.spec.replicas value observed before quiescing.
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	// Service is the pre-adoption ServiceSpec of the same-name K8s Service.
	// Used by the ServiceSwapper finalizer to rebuild the original Service on release.
	// +optional
	Service *corev1.ServiceSpec `json:"service,omitempty"`

	// Traffic captures pre-adoption VS/DR specs for restore.
	// +optional
	Traffic *TrafficSnapshot `json:"traffic,omitempty"`
}

// ProbeStatus records the most recent /scaledownz guard result.
type ProbeStatus struct {
	// +optional
	Time metav1.Time `json:"time,omitempty"`

	// +optional
	Result ProbeResult `json:"result,omitempty"`

	// +optional
	Message string `json:"message,omitempty"`

	// ConsecutiveErrors counts back-to-back guard rounds in which at least
	// one pod probe failed at the transport layer (timeout, 5xx, unreachable).
	// It is reset to zero on any error-free round. When it reaches
	// spec.scaleDownProbe.consecutiveFailureThreshold the reconciler raises
	// the Degraded condition.
	// +optional
	ConsecutiveErrors int32 `json:"consecutiveErrors,omitempty"`
}

// ObservedRoute reports the latest reconcile state for one traffic object.
type ObservedRoute struct {
	// Type identifies the traffic object type (e.g. VirtualService, DestinationRule).
	// +optional
	Type string `json:"type,omitempty"`

	// Name is the traffic object name.
	// +optional
	Name string `json:"name,omitempty"`

	// Ready indicates whether this route object is reconciled as expected.
	// +optional
	Ready *bool `json:"ready,omitempty"`

	// Message carries the latest reconcile detail for this route object.
	// +optional
	Message string `json:"message,omitempty"`
}

// TrafficStatus captures observed status of traffic adaptation.
type TrafficStatus struct {
	// ObservedRoutes lists per-object reconciliation outcomes.
	// +optional
	ObservedRoutes []ObservedRoute `json:"observedRoutes,omitempty"`
}

// KnativeAdoptionStatus defines the observed state of KnativeAdoption.
type KnativeAdoptionStatus struct {
	// conditions represent the current state of the KnativeAdoption resource.
	// Standard condition types: Adopted, SourceQuiesced, ServiceAdopted,
	// ScaleDownAllowed, EventingReady, TrafficReady, TrafficDegraded, Ready, Degraded.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// URL is the public address exposed by the generated Knative Service.
	// +optional
	URL string `json:"url,omitempty"`

	// SourceSnapshot is the pre-adoption snapshot used to restore on release.
	// +optional
	SourceSnapshot *SourceSnapshot `json:"sourceSnapshot,omitempty"`

	// LastScaleDownProbe is the most recent /scaledownz guard outcome.
	// +optional
	LastScaleDownProbe *ProbeStatus `json:"lastScaleDownProbe,omitempty"`

	// Traffic captures observed status of traffic adaptation.
	// +optional
	Traffic *TrafficStatus `json:"traffic,omitempty"`

	// ObservedSourceHash is the hash of Deployment.spec last reconciled into the KSvc.
	// +optional
	ObservedSourceHash string `json:"observedSourceHash,omitempty"`

	// OwnedTriggers lists the eventing.knative.dev/Trigger names created for this CR.
	// Populated only when mode=eventing.
	// +optional
	OwnedTriggers []string `json:"ownedTriggers,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=ka;adoption
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=`.spec.mode`
// +kubebuilder:printcolumn:name="URL",type=string,JSONPath=`.status.url`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// KnativeAdoption is the operator-managed ledger that adopts a Deployment into a
// Knative Service. See docs/knative-autopilot-design.md for full semantics.
type KnativeAdoption struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of KnativeAdoption
	// +required
	Spec KnativeAdoptionSpec `json:"spec"`

	// status defines the observed state of KnativeAdoption
	// +optional
	Status KnativeAdoptionStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// KnativeAdoptionList contains a list of KnativeAdoption
type KnativeAdoptionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []KnativeAdoption `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &KnativeAdoption{}, &KnativeAdoptionList{})
		return nil
	})
}
