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

// Package translator turns a Deployment + KnativeAdoption pair into a Knative
// Service spec. It is a pure function package: no client-go, no logger, no
// global state. All decisions and errors are derivable from inputs alone, so
// it can be exercised with table-driven unit tests.
//
// See docs/knative-autopilot-impl-plan.md (S2) for the contract.
package translator

import (
	"fmt"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	kservingv1 "knative.dev/serving/pkg/apis/serving/v1"

	ifaasv1alpha1 "github.com/ifbiu/ifaas/api/ifaas/v1alpha1"
)

const (
	// KPA annotation keys written onto the KSvc RevisionTemplate.
	AutoscalingMinScaleAnnotation = "autoscaling.knative.dev/min-scale"
	AutoscalingMaxScaleAnnotation = "autoscaling.knative.dev/max-scale"
	AutoscalingTargetAnnotation   = "autoscaling.knative.dev/target"

	// Bookkeeping labels stamped onto the KSvc so the operator can later
	// distinguish operator-owned objects from user-authored ones.
	ManagedByLabel = "ifaas.ifbiu.com/managed-by"
	ManagedByValue = "knative-autopilot"
	OwnerLabel     = "ifaas.ifbiu.com/owner"

	defaultScaleDownProbePath             = "/scaledownz"
	defaultScaleDownProbeIntervalSeconds  = int64(30)
	defaultScaleDownProbeFailureThreshold = int64(20)
	terminationGracePeriodPaddingSeconds  = int64(5)
)

// Translate is the only public entry point. It returns a fully populated
// *kservingv1.Service ready for server-side apply, or a sentinel error from
// errors.go describing why the input is not adoptable.
func Translate(dep *appsv1.Deployment, adoption *ifaasv1alpha1.KnativeAdoption) (*kservingv1.Service, error) {
	if err := guardPodSpec(&dep.Spec.Template.Spec); err != nil {
		return nil, err
	}
	primary, err := pickPrimaryContainer(dep, adoption)
	if err != nil {
		return nil, err
	}
	port, err := pickUserPort(primary)
	if err != nil {
		return nil, err
	}

	labels := map[string]string{
		ManagedByLabel: ManagedByValue,
		OwnerLabel:     adoption.Name,
	}

	ksvc := &kservingv1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dep.Name,
			Namespace: dep.Namespace,
			Labels:    labels,
		},
		Spec: kservingv1.ServiceSpec{
			ConfigurationSpec: kservingv1.ConfigurationSpec{
				Template: kservingv1.RevisionTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Annotations: buildKPAAnnotations(adoption),
						Labels:      labels,
					},
					Spec: kservingv1.RevisionSpec{
						PodSpec: corev1.PodSpec{
							ServiceAccountName:            dep.Spec.Template.Spec.ServiceAccountName,
							ImagePullSecrets:              dep.Spec.Template.Spec.ImagePullSecrets,
							Volumes:                       dep.Spec.Template.Spec.Volumes,
							NodeSelector:                  dep.Spec.Template.Spec.NodeSelector,
							Tolerations:                   dep.Spec.Template.Spec.Tolerations,
							Affinity:                      dep.Spec.Template.Spec.Affinity,
							Containers:                    []corev1.Container{buildUserContainer(primary, adoption, port)},
							TerminationGracePeriodSeconds: terminationGracePeriod(dep, adoption),
						},
					},
				},
			},
		},
	}
	return ksvc, nil
}

func guardPodSpec(s *corev1.PodSpec) error {
	if s.HostNetwork {
		return ErrHostNetwork
	}
	if s.HostPID {
		return ErrHostPID
	}
	if s.HostIPC {
		return ErrHostIPC
	}
	for _, c := range s.Containers {
		for _, p := range c.Ports {
			if p.HostPort != 0 {
				return fmt.Errorf("%w: container %q port %d", ErrHostPort, c.Name, p.HostPort)
			}
		}
	}
	return nil
}

func pickPrimaryContainer(dep *appsv1.Deployment, a *ifaasv1alpha1.KnativeAdoption) (corev1.Container, error) {
	containers := dep.Spec.Template.Spec.Containers
	switch {
	case len(containers) == 0:
		return corev1.Container{}, ErrNoContainer
	case a.Spec.PrimaryContainer != "":
		for _, c := range containers {
			if c.Name == a.Spec.PrimaryContainer {
				return c, nil
			}
		}
		return corev1.Container{}, fmt.Errorf("%w: %q", ErrPrimaryNotFound, a.Spec.PrimaryContainer)
	case len(containers) > 1:
		return corev1.Container{}, ErrAmbiguousPrimary
	default:
		return containers[0], nil
	}
}

func pickUserPort(c corev1.Container) (int32, error) {
	if len(c.Ports) == 0 {
		return 0, ErrNoContainerPort
	}
	p := c.Ports[0]
	if p.Protocol != "" && p.Protocol != corev1.ProtocolTCP {
		return 0, fmt.Errorf("%w: %s", ErrUnsupportedProtocol, p.Protocol)
	}
	return p.ContainerPort, nil
}

func buildKPAAnnotations(a *ifaasv1alpha1.KnativeAdoption) map[string]string {
	ann := map[string]string{}
	ann[AutoscalingMinScaleAnnotation] = strconv.FormatInt(int64(EffectiveMinScale(a)), 10)
	if a.Spec.Autoscaling.MaxScale != nil {
		ann[AutoscalingMaxScaleAnnotation] = strconv.FormatInt(int64(*a.Spec.Autoscaling.MaxScale), 10)
	}
	if a.Spec.Autoscaling.TargetConcurrency != nil {
		ann[AutoscalingTargetAnnotation] = strconv.FormatInt(int64(*a.Spec.Autoscaling.TargetConcurrency), 10)
	}
	return ann
}

// EffectiveMinScale is the single source of truth for the
// `autoscaling.knative.dev/min-scale` annotation written onto the KSvc.
//
// Inputs (priority, top wins):
//  1. Latest /scaledownz round refused scale-to-zero
//     (status.lastScaleDownProbe.Result == "false") → "1"
//  2. spec.autoscaling.minScale, defaulting to 0 when unset
//
// Rule (1) only applies when the user-declared baseline is 0; when the user
// already pinned minScale ≥ 1, the guard is irrelevant — there is nothing to
// gate against. Keeping this derivation pure (status → annotation, never the
// reverse) means SSA always reapplies the same value and the field-owner
// contract stays uncontested across reconcile cycles. See impl-plan §S6.
func EffectiveMinScale(a *ifaasv1alpha1.KnativeAdoption) int32 {
	base := int32(0)
	if a.Spec.Autoscaling.MinScale != nil {
		base = *a.Spec.Autoscaling.MinScale
	}
	if base == 0 && guardBlocked(a) {
		return 1
	}
	return base
}

func guardBlocked(a *ifaasv1alpha1.KnativeAdoption) bool {
	probe := a.Status.LastScaleDownProbe
	if probe == nil {
		return false
	}
	return probe.Result == ifaasv1alpha1.ProbeResultFalse
}

func buildUserContainer(src corev1.Container, a *ifaasv1alpha1.KnativeAdoption, port int32) corev1.Container {
	c := *src.DeepCopy()
	// Knative restricts a revision template to a single declared port; downstream
	// validation runs again at KSvc creation, but trimming here keeps the spec
	// minimal and the diff readable.
	if len(c.Ports) > 1 {
		c.Ports = []corev1.ContainerPort{c.Ports[0]}
	}
	c.Lifecycle = injectPreStop(c.Lifecycle, a, port)
	return c
}

func injectPreStop(cur *corev1.Lifecycle, a *ifaasv1alpha1.KnativeAdoption, userPort int32) *corev1.Lifecycle {
	path := a.Spec.ScaleDownProbe.Path
	if path == "" {
		path = defaultScaleDownProbePath
	}
	port := userPort
	if a.Spec.ScaleDownProbe.Port != nil {
		port = *a.Spec.ScaleDownProbe.Port
	}
	handler := corev1.LifecycleHandler{
		HTTPGet: &corev1.HTTPGetAction{
			Path: path,
			Port: intstr.FromInt32(port),
		},
	}
	if cur == nil {
		return &corev1.Lifecycle{PreStop: &handler}
	}
	out := cur.DeepCopy()
	out.PreStop = &handler
	return out
}

func terminationGracePeriod(dep *appsv1.Deployment, a *ifaasv1alpha1.KnativeAdoption) *int64 {
	interval := int64(a.Spec.ScaleDownProbe.IntervalSeconds)
	if interval <= 0 {
		interval = defaultScaleDownProbeIntervalSeconds
	}
	threshold := int64(a.Spec.ScaleDownProbe.ConsecutiveFailureThreshold)
	if threshold <= 0 {
		threshold = defaultScaleDownProbeFailureThreshold
	}
	desired := interval*threshold + terminationGracePeriodPaddingSeconds

	var origin int64
	if dep.Spec.Template.Spec.TerminationGracePeriodSeconds != nil {
		origin = *dep.Spec.Template.Spec.TerminationGracePeriodSeconds
	}
	if origin > desired {
		return &origin
	}
	return &desired
}
