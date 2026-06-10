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
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ifaasv1alpha1 "github.com/ifbiu/ifaas/api/ifaas/v1alpha1"
)

// satisfy go vet imported-and-not-used during incremental edits.
var _ = NewWithT

// makeDeployment builds a single-container Deployment whose container name
// matches the conventional Knative user-container so PrimaryContainer
// resolution in the webhook is unambiguous.
func makeDeployment(name, ns string, replicas int32, autopilot bool) *appsv1.Deployment {
	labels := map[string]string{"app": name}
	if autopilot {
		labels[labelEnabled] = labelEnabledValue
	}
	r := replicas
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &r,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": name}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "user-container",
						Image: "registry.example/echo:v1",
						Ports: []corev1.ContainerPort{{ContainerPort: 8080}},
					}},
				},
			},
		},
	}
}

// makeMultiContainerDeployment exists so PrimaryContainer validation has
// something realistic to chew on.
func makeMultiContainerDeployment(name, ns string) *appsv1.Deployment {
	d := makeDeployment(name, ns, 1, false)
	d.Spec.Template.Spec.Containers = []corev1.Container{
		{Name: "user-container", Image: "registry.example/echo:v1"},
		{Name: "sidecar", Image: "registry.example/sidecar:v1"},
	}
	return d
}

func makeAdoption(name, ns, sourceName string) *ifaasv1alpha1.KnativeAdoption {
	return &ifaasv1alpha1.KnativeAdoption{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: ifaasv1alpha1.KnativeAdoptionSpec{
			SourceRef: ifaasv1alpha1.SourceRef{
				Kind: ifaasv1alpha1.SourceKindDeployment,
				Name: sourceName,
			},
		},
	}
}

func makeHPA(name, ns, targetDeployment string) *autoscalingv2.HorizontalPodAutoscaler {
	min := int32(1)
	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       targetDeployment,
			},
			MinReplicas: &min,
			MaxReplicas: 5,
		},
	}
}
