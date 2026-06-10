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

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kservingv1 "knative.dev/serving/pkg/apis/serving/v1"

	ifaasv1alpha1 "github.com/ifbiu/ifaas/api/ifaas/v1alpha1"
)

// envtest does not run a Knative controller, so KSvc Ready never flips by
// itself. The reconciler exposes a hook for tests to substitute their own
// readiness inspector.
func alwaysReady(_ *kservingv1.Service) (bool, string) {
	return true, "http://hello.default.example.com"
}

func alwaysPending(_ *kservingv1.Service) (bool, string) {
	return false, ""
}

func makeDeployment(name, ns string, replicas int32) *appsv1.Deployment {
	r := replicas
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: appsv1.DeploymentSpec{
			Replicas: &r,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": name}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "app",
						Image: "registry.example/" + name + ":v1",
						Ports: []corev1.ContainerPort{{ContainerPort: 8080}},
					}},
				},
			},
		},
	}
}

func makeAdoption(name, ns string) *ifaasv1alpha1.KnativeAdoption {
	return &ifaasv1alpha1.KnativeAdoption{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: ifaasv1alpha1.KnativeAdoptionSpec{
			SourceRef: ifaasv1alpha1.SourceRef{Kind: ifaasv1alpha1.SourceKindDeployment, Name: name},
			Mode:      ifaasv1alpha1.ModeServing,
		},
	}
}

var _ = Describe("KnativeAdoptionReconciler", func() {
	const ns = "default"

	var (
		rec  *KnativeAdoptionReconciler
		ictx context.Context
	)

	BeforeEach(func() {
		ictx = context.Background()
		rec = &KnativeAdoptionReconciler{
			Client:       k8sClient,
			Scheme:       k8sClient.Scheme(),
			ServiceReady: alwaysReady,
		}
	})

	Context("when the source Deployment is missing", func() {
		const name = "ghost"

		AfterEach(func() {
			_ = k8sClient.Delete(ictx, &ifaasv1alpha1.KnativeAdoption{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
		})

		It("marks SourceMissing and requeues", func() {
			Expect(k8sClient.Create(ictx, makeAdoption(name, ns))).To(Succeed())

			res, err := rec.Reconcile(ictx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(Equal(requeueAfterSourceMissing))

			got := &ifaasv1alpha1.KnativeAdoption{}
			Expect(k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, got)).To(Succeed())
			Expect(apimeta.IsStatusConditionTrue(got.Status.Conditions, ifaasv1alpha1.ConditionSourceMissing)).To(BeTrue())
			Expect(apimeta.IsStatusConditionTrue(got.Status.Conditions, ifaasv1alpha1.ConditionReady)).To(BeFalse())
		})
	})

	Context("when both CR and Deployment exist", func() {
		const name = "hello"

		AfterEach(func() {
			_ = k8sClient.Delete(ictx, &ifaasv1alpha1.KnativeAdoption{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
			_ = k8sClient.Delete(ictx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
			_ = k8sClient.Delete(ictx, &kservingv1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
		})

		It("applies the KSvc, scales the Deployment to zero and reports Ready", func() {
			Expect(k8sClient.Create(ictx, makeDeployment(name, ns, 3))).To(Succeed())
			Expect(k8sClient.Create(ictx, makeAdoption(name, ns))).To(Succeed())

			_, err := rec.Reconcile(ictx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
			Expect(err).NotTo(HaveOccurred())

			By("a same-named Knative Service is created with controller reference back to the adoption")
			ksvc := &kservingv1.Service{}
			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, ksvc)).To(Succeed())
				g.Expect(ksvc.GetOwnerReferences()).NotTo(BeEmpty())
				g.Expect(ksvc.GetOwnerReferences()[0].Kind).To(Equal("KnativeAdoption"))
				g.Expect(ksvc.Spec.Template.Spec.Containers).To(HaveLen(1))
				g.Expect(ksvc.Spec.Template.Spec.Containers[0].Image).To(Equal("registry.example/" + name + ":v1"))
			}, 5*time.Second, 100*time.Millisecond).Should(Succeed())

			By("the source Deployment is scaled to zero and the original replica count is snapshotted")
			Eventually(func(g Gomega) {
				dep := &appsv1.Deployment{}
				g.Expect(k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, dep)).To(Succeed())
				g.Expect(dep.Spec.Replicas).NotTo(BeNil())
				g.Expect(*dep.Spec.Replicas).To(Equal(int32(0)))

				got := &ifaasv1alpha1.KnativeAdoption{}
				g.Expect(k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, got)).To(Succeed())
				g.Expect(got.Status.SourceSnapshot).NotTo(BeNil())
				g.Expect(got.Status.SourceSnapshot.Replicas).NotTo(BeNil())
				g.Expect(*got.Status.SourceSnapshot.Replicas).To(Equal(int32(3)))
				g.Expect(got.Status.URL).To(Equal("http://hello.default.example.com"))
				g.Expect(apimeta.IsStatusConditionTrue(got.Status.Conditions, ifaasv1alpha1.ConditionAdopted)).To(BeTrue())
				g.Expect(apimeta.IsStatusConditionTrue(got.Status.Conditions, ifaasv1alpha1.ConditionServiceAdopted)).To(BeTrue())
				g.Expect(apimeta.IsStatusConditionTrue(got.Status.Conditions, ifaasv1alpha1.ConditionSourceQuiesced)).To(BeTrue())
				g.Expect(apimeta.IsStatusConditionTrue(got.Status.Conditions, ifaasv1alpha1.ConditionReady)).To(BeTrue())
			}, 5*time.Second, 100*time.Millisecond).Should(Succeed())
		})

		It("holds Ready=False with ServiceAdopted=False when KSvc is still pending", func() {
			rec.ServiceReady = alwaysPending
			Expect(k8sClient.Create(ictx, makeDeployment(name, ns, 1))).To(Succeed())
			Expect(k8sClient.Create(ictx, makeAdoption(name, ns))).To(Succeed())

			res, err := rec.Reconcile(ictx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(Equal(requeueAfterKSvcPending))

			got := &ifaasv1alpha1.KnativeAdoption{}
			Expect(k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, got)).To(Succeed())
			Expect(apimeta.IsStatusConditionTrue(got.Status.Conditions, ifaasv1alpha1.ConditionAdopted)).To(BeTrue())
			Expect(apimeta.IsStatusConditionTrue(got.Status.Conditions, ifaasv1alpha1.ConditionServiceAdopted)).To(BeFalse())
			Expect(apimeta.IsStatusConditionTrue(got.Status.Conditions, ifaasv1alpha1.ConditionReady)).To(BeFalse())

			By("the Deployment is left untouched so business traffic is not yanked away")
			dep := &appsv1.Deployment{}
			Expect(k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, dep)).To(Succeed())
			Expect(*dep.Spec.Replicas).To(Equal(int32(1)))
		})

		It("cascades the KSvc when the adoption is deleted", func() {
			Expect(k8sClient.Create(ictx, makeDeployment(name, ns, 2))).To(Succeed())
			Expect(k8sClient.Create(ictx, makeAdoption(name, ns))).To(Succeed())
			_, err := rec.Reconcile(ictx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
			Expect(err).NotTo(HaveOccurred())

			ksvc := &kservingv1.Service{}
			Expect(k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, ksvc)).To(Succeed())

			Expect(k8sClient.Delete(ictx, &ifaasv1alpha1.KnativeAdoption{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})).To(Succeed())

			// envtest has no garbage collector, so the KSvc lingers; the
			// assertion here is the link itself: ownerRef + blockOwnerDeletion
			// is what tells a real apiserver GC to remove the KSvc once the
			// adoption is gone. S9 will introduce explicit teardown for the
			// non-cascading bits (Deployment replicas restore).
			refs := ksvc.GetOwnerReferences()
			Expect(refs).NotTo(BeEmpty())
			Expect(refs[0].Controller).NotTo(BeNil())
			Expect(*refs[0].Controller).To(BeTrue())
			Expect(refs[0].Name).To(Equal(name))
		})
	})

	Context("when the Deployment has incompatible pod spec", func() {
		const name = "bad"

		AfterEach(func() {
			_ = k8sClient.Delete(ictx, &ifaasv1alpha1.KnativeAdoption{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
			_ = k8sClient.Delete(ictx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
		})

		It("marks TranslationDegraded and does not create a KSvc", func() {
			dep := makeDeployment(name, ns, 1)
			dep.Spec.Template.Spec.HostNetwork = true
			Expect(k8sClient.Create(ictx, dep)).To(Succeed())
			Expect(k8sClient.Create(ictx, makeAdoption(name, ns))).To(Succeed())

			_, err := rec.Reconcile(ictx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
			Expect(err).NotTo(HaveOccurred())

			got := &ifaasv1alpha1.KnativeAdoption{}
			Expect(k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, got)).To(Succeed())
			Expect(apimeta.IsStatusConditionTrue(got.Status.Conditions, ifaasv1alpha1.ConditionTranslationDegraded)).To(BeTrue())
			Expect(apimeta.IsStatusConditionTrue(got.Status.Conditions, ifaasv1alpha1.ConditionAdopted)).To(BeFalse())

			ksvc := &kservingv1.Service{}
			err = k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, ksvc)
			Expect(apierrors.IsNotFound(err)).To(BeTrue(), fmt.Sprintf("expected NotFound, got %v", err))
		})
	})
})

// keep import in case future tests need it
var _ = client.IgnoreNotFound
