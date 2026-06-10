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
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ifaasv1alpha1 "github.com/ifbiu/ifaas/api/ifaas/v1alpha1"
)

// Every spec runs against the live envtest apiserver: the webhook server
// is started in webhook_suite_test.go and the Setup* helpers wire both
// the KnativeAdoption and Deployment webhooks. The tests therefore
// observe end-to-end admission (apiserver → webhook → client.Create
// return code), not just the in-process validator method.
var _ = Describe("KnativeAdoption Webhook", func() {
	const ns = "default"

	AfterEach(func() {
		// Best-effort sweep so each spec starts from a clean slate.
		_ = k8sClient.DeleteAllOf(ctx, &ifaasv1alpha1.KnativeAdoption{}, client.InNamespace(ns))
		_ = k8sClient.DeleteAllOf(ctx, &autoscalingv2.HorizontalPodAutoscaler{}, client.InNamespace(ns))
		dl := &appsv1.DeploymentList{}
		_ = k8sClient.List(ctx, dl, client.InNamespace(ns))
		for i := range dl.Items {
			_ = k8sClient.Delete(ctx, &dl.Items[i])
		}
	})

	Describe("Defaulting", func() {
		It("inherits sourceRef.namespace from the CR's namespace", func() {
			dep := makeDeployment("def-target", ns, 1, false)
			Expect(k8sClient.Create(ctx, dep)).To(Succeed())

			a := &ifaasv1alpha1.KnativeAdoption{
				ObjectMeta: metav1.ObjectMeta{Name: "def", Namespace: ns},
				Spec: ifaasv1alpha1.KnativeAdoptionSpec{
					SourceRef: ifaasv1alpha1.SourceRef{Name: "def-target"},
				},
			}
			Expect(k8sClient.Create(ctx, a)).To(Succeed())

			got := &ifaasv1alpha1.KnativeAdoption{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "def", Namespace: ns}, got)).To(Succeed())
			Expect(got.Spec.SourceRef.Namespace).To(Equal(ns))
		})
	})

	Describe("Validation on Create", func() {
		It("rejects when the source Deployment is missing", func() {
			a := makeAdoption("ka-missing", ns, "no-such-dep")
			Expect(k8sClient.Create(ctx, a)).NotTo(Succeed())
		})

		It("admits when the source Deployment exists", func() {
			dep := makeDeployment("ka-ok-dep", ns, 1, false)
			Expect(k8sClient.Create(ctx, dep)).To(Succeed())

			a := makeAdoption("ka-ok", ns, "ka-ok-dep")
			Expect(k8sClient.Create(ctx, a)).To(Succeed())
		})

		It("rejects when an HPA targets the source Deployment", func() {
			dep := makeDeployment("ka-hpa-dep", ns, 1, false)
			Expect(k8sClient.Create(ctx, dep)).To(Succeed())
			hpa := makeHPA("ka-hpa", ns, "ka-hpa-dep")
			Expect(k8sClient.Create(ctx, hpa)).To(Succeed())

			a := makeAdoption("ka-hpa-cr", ns, "ka-hpa-dep")
			Expect(k8sClient.Create(ctx, a)).NotTo(Succeed())
		})

		It("requires primaryContainer when the source Deployment has multiple containers", func() {
			dep := makeMultiContainerDeployment("ka-multi-dep", ns)
			Expect(k8sClient.Create(ctx, dep)).To(Succeed())

			a := makeAdoption("ka-multi-no-primary", ns, "ka-multi-dep")
			Expect(k8sClient.Create(ctx, a)).NotTo(Succeed())

			a2 := makeAdoption("ka-multi-bad-primary", ns, "ka-multi-dep")
			a2.Spec.PrimaryContainer = "does-not-exist"
			Expect(k8sClient.Create(ctx, a2)).NotTo(Succeed())

			a3 := makeAdoption("ka-multi-ok", ns, "ka-multi-dep")
			a3.Spec.PrimaryContainer = "user-container"
			Expect(k8sClient.Create(ctx, a3)).To(Succeed())
		})

		It("requires spec.eventing.broker when mode=eventing", func() {
			dep := makeDeployment("ka-eventing-dep", ns, 1, false)
			Expect(k8sClient.Create(ctx, dep)).To(Succeed())

			a := makeAdoption("ka-eventing-no-broker", ns, "ka-eventing-dep")
			a.Spec.Mode = ifaasv1alpha1.ModeEventing
			Expect(k8sClient.Create(ctx, a)).NotTo(Succeed())

			a2 := makeAdoption("ka-eventing-ok", ns, "ka-eventing-dep")
			a2.Spec.Mode = ifaasv1alpha1.ModeEventing
			a2.Spec.Eventing = &ifaasv1alpha1.Eventing{Broker: "default"}
			Expect(k8sClient.Create(ctx, a2)).To(Succeed())
		})

		It("rejects user-authored reserved labels and annotations", func() {
			dep := makeDeployment("ka-reserved-dep", ns, 1, false)
			Expect(k8sClient.Create(ctx, dep)).To(Succeed())

			a := makeAdoption("ka-reserved-label", ns, "ka-reserved-dep")
			a.Labels = map[string]string{ReservedLabelManagedBy: "knative-autopilot"}
			Expect(k8sClient.Create(ctx, a)).NotTo(Succeed())

			a2 := makeAdoption("ka-reserved-anno", ns, "ka-reserved-dep")
			a2.Annotations = map[string]string{ReservedAnnoManagedBy: "x"}
			Expect(k8sClient.Create(ctx, a2)).NotTo(Succeed())
		})
	})

	Describe("Validation on Update", func() {
		It("rejects spec.sourceRef changes", func() {
			depA := makeDeployment("upd-a", ns, 1, false)
			depB := makeDeployment("upd-b", ns, 1, false)
			Expect(k8sClient.Create(ctx, depA)).To(Succeed())
			Expect(k8sClient.Create(ctx, depB)).To(Succeed())

			a := makeAdoption("upd-cr", ns, "upd-a")
			Expect(k8sClient.Create(ctx, a)).To(Succeed())

			got := &ifaasv1alpha1.KnativeAdoption{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "upd-cr", Namespace: ns}, got)).To(Succeed())
			got.Spec.SourceRef.Name = "upd-b"
			Expect(k8sClient.Update(ctx, got)).NotTo(Succeed())
		})

		It("admits spec changes that leave sourceRef intact", func() {
			dep := makeDeployment("upd-keep-dep", ns, 1, false)
			Expect(k8sClient.Create(ctx, dep)).To(Succeed())

			a := makeAdoption("upd-keep", ns, "upd-keep-dep")
			Expect(k8sClient.Create(ctx, a)).To(Succeed())

			got := &ifaasv1alpha1.KnativeAdoption{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "upd-keep", Namespace: ns}, got)).To(Succeed())
			max := int32(7)
			got.Spec.Autoscaling.MaxScale = &max
			Expect(k8sClient.Update(ctx, got)).To(Succeed())
		})
	})
})
