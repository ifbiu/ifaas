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

// Package projector translates the public Deployment annotation contract into
// a KnativeAdoption.Spec. It is a pure function package: no apiserver, no
// logger, no defaults beyond what the CRD schema itself supplies.
package projector

import (
	"fmt"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"

	ifaasv1alpha1 "github.com/ifbiu/ifaas/api/ifaas/v1alpha1"
)

// Annotation keys consumed by the projector. Mirrors
// internal/controller/ifaas/labels.go to keep the projector free of cyclic
// imports with the controller package.
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

// FromDeployment builds the KnativeAdoption.Spec the DeploymentWatcher will
// server-side-apply for the given Deployment. The returned spec always sets
// SourceRef to point back at the Deployment and defaults Mode to serving when
// the annotation is absent. Annotation values are parsed loosely; invalid
// numeric values yield a non-fatal warning rather than failing the projection
// because the validating webhook (S8) is the authoritative gatekeeper.
func FromDeployment(dep *appsv1.Deployment) (ifaasv1alpha1.KnativeAdoptionSpec, []string) {
	ann := dep.GetAnnotations()
	var warnings []string

	spec := ifaasv1alpha1.KnativeAdoptionSpec{
		SourceRef: ifaasv1alpha1.SourceRef{
			Kind: ifaasv1alpha1.SourceKindDeployment,
			Name: dep.Name,
		},
		Mode: ifaasv1alpha1.ModeServing,
	}

	if v, ok := ann[AnnoMode]; ok && v != "" {
		spec.Mode = ifaasv1alpha1.Mode(v)
	}
	if v, ok := ann[AnnoPrimaryContainer]; ok {
		spec.PrimaryContainer = v
	}

	spec.Autoscaling, warnings = projectAutoscaling(ann, warnings)
	spec.ScaleDownProbe, warnings = projectScaleDownProbe(ann, warnings)

	if v, ok := ann[AnnoEventingBroker]; ok && v != "" {
		spec.Eventing = &ifaasv1alpha1.Eventing{Broker: v}
	}

	return spec, warnings
}

func projectAutoscaling(ann map[string]string, warnings []string) (ifaasv1alpha1.Autoscaling, []string) {
	var out ifaasv1alpha1.Autoscaling
	if v, ok := parseInt32(ann, AnnoMinScale, &warnings); ok {
		out.MinScale = &v
	}
	if v, ok := parseInt32(ann, AnnoMaxScale, &warnings); ok {
		out.MaxScale = &v
	}
	if v, ok := parseInt32(ann, AnnoTargetConcurrency, &warnings); ok {
		out.TargetConcurrency = &v
	}
	return out, warnings
}

func projectScaleDownProbe(ann map[string]string, warnings []string) (ifaasv1alpha1.ScaleDownProbe, []string) {
	var out ifaasv1alpha1.ScaleDownProbe
	if v, ok := ann[AnnoScaleDownPath]; ok {
		out.Path = v
	}
	if v, ok := parseInt32(ann, AnnoScaleDownPort, &warnings); ok {
		out.Port = &v
	}
	if v, ok := parseInt32(ann, AnnoScaleDownIntervalSeconds, &warnings); ok {
		out.IntervalSeconds = v
	}
	if v, ok := parseInt32(ann, AnnoScaleDownTimeoutSeconds, &warnings); ok {
		out.TimeoutSeconds = v
	}
	if v, ok := parseInt32(ann, AnnoScaleDownFailureThreshold, &warnings); ok {
		out.ConsecutiveFailureThreshold = v
	}
	return out, warnings
}

func parseInt32(ann map[string]string, key string, warnings *[]string) (int32, bool) {
	raw, ok := ann[key]
	if !ok || raw == "" {
		return 0, false
	}
	n, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		*warnings = append(*warnings, fmt.Sprintf("annotation %s=%q is not a valid int32: %v", key, raw, err))
		return 0, false
	}
	return int32(n), true
}
