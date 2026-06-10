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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("Deployment Webhook", func() {
	const ns = "default"

	AfterEach(func() {
		dl := &appsv1.DeploymentList{}
		_ = k8sClient.List(ctx, dl, client.InNamespace(ns))
		for i := range dl.Items {
			_ = k8sClient.Delete(ctx, &dl.Items[i])
		}
	})

	It("ignores Deployments without the autopilot label", func() {
		dep := makeDeployment("dw-noop", ns, 0, false)
		Expect(k8sClient.Create(ctx, dep)).To(Succeed())

		got := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "dw-noop", Namespace: ns}, got)).To(Succeed())
		r := int32(3)
		got.Spec.Replicas = &r
		Expect(k8sClient.Update(ctx, got)).To(Succeed(),
			"unlabeled Deployment must not be policed by the autopilot webhook")
	})

	It("rejects 0 → >0 replicas on labelled Deployments", func() {
		dep := makeDeployment("dw-block", ns, 0, true)
		Expect(k8sClient.Create(ctx, dep)).To(Succeed())

		got := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "dw-block", Namespace: ns}, got)).To(Succeed())
		r := int32(2)
		got.Spec.Replicas = &r
		Expect(k8sClient.Update(ctx, got)).NotTo(Succeed(),
			"manual scale-up from 0 on an adopted Deployment must be rejected")
	})

	It("admits scale-down to 0 on labelled Deployments", func() {
		dep := makeDeployment("dw-down", ns, 3, true)
		Expect(k8sClient.Create(ctx, dep)).To(Succeed())

		got := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "dw-down", Namespace: ns}, got)).To(Succeed())
		r := int32(0)
		got.Spec.Replicas = &r
		Expect(k8sClient.Update(ctx, got)).To(Succeed(),
			"scale-down to zero is the documented emergency-stop")
	})

	It("admits non-scale changes on labelled Deployments", func() {
		dep := makeDeployment("dw-other", ns, 2, true)
		Expect(k8sClient.Create(ctx, dep)).To(Succeed())

		got := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "dw-other", Namespace: ns}, got)).To(Succeed())
		got.Spec.Template.Spec.Containers[0].Image = "registry.example/echo:v2"
		Expect(k8sClient.Update(ctx, got)).To(Succeed())
	})
})
