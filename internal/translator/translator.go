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
	if _, err := pickUserPort(primary); err != nil {
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
						// PodSpec subset accepted by stock Knative serving with no
						// feature gates open. NodeSelector / Tolerations / Affinity
						// / TerminationGracePeriodSeconds / DNSPolicy / RestartPolicy
						// / HostAliases / SchedulerName / PriorityClassName are all
						// gated behind kubernetes.podspec-* features; the operator
						// does not assume any are open cluster-side, so adopted
						// Deployments surrender those scheduling hints when
						// projected. PreStop / TerminationGracePeriodSeconds opt-in
						// surfaces are deferred — see plan §12.
						PodSpec: corev1.PodSpec{
							ServiceAccountName: dep.Spec.Template.Spec.ServiceAccountName,
							ImagePullSecrets:   dep.Spec.Template.Spec.ImagePullSecrets,
							Volumes:            sanitizeVolumes(dep.Spec.Template.Spec.Volumes),
							Containers:         []corev1.Container{sanitizeContainer(primary)},
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

// sanitizeContainer returns a copy of src restricted to the Container fields
// stock Knative serving accepts in a RevisionTemplate with no feature gates
// open. Anything outside the allow-list is intentionally dropped:
//   - Lifecycle (gated by kubernetes.podspec-lifecycle)
//   - VolumeDevices (gated by kubernetes.podspec-volumes-devices)
//   - Stdin / StdinOnce / TTY (not declared in KSvc Container schema)
//   - Env entries with valueFrom.fieldRef / resourceFieldRef (gated by
//     kubernetes.podspec-fieldref); the entire EnvVar is dropped because
//     surfacing a name-only entry would silently change app behaviour.
//
// ScaleDownGuard already enforces graceful scale-to-zero via /scaledownz, so
// dropping Lifecycle.PreStop is semantically harmless; the gate-aware opt-in
// surface is deferred — see plan §12.
func sanitizeContainer(src corev1.Container) corev1.Container {
	return corev1.Container{
		Name:                     src.Name,
		Image:                    src.Image,
		Command:                  src.Command,
		Args:                     src.Args,
		WorkingDir:               src.WorkingDir,
		Ports:                    sanitizePorts(src.Ports),
		EnvFrom:                  src.EnvFrom,
		Env:                      sanitizeEnv(src.Env),
		Resources:                src.Resources,
		VolumeMounts:             src.VolumeMounts,
		LivenessProbe:            src.LivenessProbe,
		ReadinessProbe:           src.ReadinessProbe,
		StartupProbe:             src.StartupProbe,
		TerminationMessagePath:   src.TerminationMessagePath,
		TerminationMessagePolicy: src.TerminationMessagePolicy,
		ImagePullPolicy:          src.ImagePullPolicy,
		SecurityContext:          src.SecurityContext,
	}
}

// sanitizeEnv drops EnvVar entries whose value comes from a downward API
// reference. Stock Knative gates `kubernetes.podspec-fieldref`, so SSA
// rejects the patch otherwise. We drop the entire entry rather than blank
// the value — leaving an empty Env slot would let the application read an
// empty string instead of (e.g.) the pod IP, which is a surprise we'd
// rather surface as "the variable is not set" than as "it is set wrongly".
func sanitizeEnv(src []corev1.EnvVar) []corev1.EnvVar {
	if len(src) == 0 {
		return nil
	}
	out := make([]corev1.EnvVar, 0, len(src))
	for _, e := range src {
		if e.ValueFrom != nil &&
			(e.ValueFrom.FieldRef != nil || e.ValueFrom.ResourceFieldRef != nil) {
			continue
		}
		out = append(out, e)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// sanitizePorts keeps at most one port and clears Name / HostPort / HostIP.
// Knative's serving webhook only accepts an empty port name or one of "h2c" /
// "http1"; arbitrary user names like "http" are rejected outright. HostPort
// is rejected upstream by guardPodSpec, so it never reaches the sanitiser;
// HostIP is silently cleared as a defensive nicety.
func sanitizePorts(ports []corev1.ContainerPort) []corev1.ContainerPort {
	if len(ports) == 0 {
		return nil
	}
	p := *ports[0].DeepCopy()
	p.Name = ""
	p.HostPort = 0
	p.HostIP = ""
	return []corev1.ContainerPort{p}
}

// sanitizeVolumes keeps only volume sources stock Knative accepts. PVC,
// generic ephemeral, hostPath, csi, and friends are all gated behind
// kubernetes.podspec-* feature flags and silently dropped here.
func sanitizeVolumes(src []corev1.Volume) []corev1.Volume {
	if len(src) == 0 {
		return nil
	}
	out := make([]corev1.Volume, 0, len(src))
	for _, v := range src {
		switch {
		case v.ConfigMap != nil,
			v.Secret != nil,
			v.EmptyDir != nil,
			v.Projected != nil,
			v.DownwardAPI != nil:
			out = append(out, v)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
