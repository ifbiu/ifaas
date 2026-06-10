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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	ifaasv1alpha1 "github.com/ifbiu/ifaas/api/ifaas/v1alpha1"
)

func labeledDeployment(name, ns string, ann map[string]string) *appsv1.Deployment {
	d := makeDeployment(name, ns, 1)
	if d.Labels == nil {
		d.Labels = map[string]string{}
	}
	d.Labels[LabelEnabled] = LabelEnabledValue
	d.Annotations = ann
	return d
}

var _ = Describe("DeploymentWatcher", func() {
	const ns = "default"

	var (
		ictx context.Context
		w    *DeploymentWatcher
	)

	BeforeEach(func() {
		ictx = context.Background()
		w = &DeploymentWatcher{Client: k8sClient, Scheme: k8sClient.Scheme()}
	})

	Context("when a Deployment is labeled", func() {
		const name = "labeled"

		AfterEach(func() {
			_ = k8sClient.Delete(ictx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
			_ = k8sClient.Delete(ictx, &ifaasv1alpha1.KnativeAdoption{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
		})

		It("creates a watcher-owned adoption projecting annotations onto spec", func() {
			Expect(k8sClient.Create(ictx, labeledDeployment(name, ns, map[string]string{
				"ifaas.ifbiu.com/min-scale":         "0",
				"ifaas.ifbiu.com/max-scale":         "7",
				"ifaas.ifbiu.com/primary-container": "app",
			}))).To(Succeed())

			_, err := w.Reconcile(ictx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
			Expect(err).NotTo(HaveOccurred())

			got := &ifaasv1alpha1.KnativeAdoption{}
			Expect(k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, got)).To(Succeed())
			Expect(got.Labels[LabelManagedByWatcher]).To(Equal(LabelManagedByWatcherValue))
			Expect(got.Spec.SourceRef.Name).To(Equal(name))
			Expect(got.Spec.SourceRef.Kind).To(Equal(ifaasv1alpha1.SourceKindDeployment))
			Expect(got.Spec.PrimaryContainer).To(Equal("app"))
			Expect(got.Spec.Autoscaling.MinScale).NotTo(BeNil())
			Expect(*got.Spec.Autoscaling.MinScale).To(Equal(int32(0)))
			Expect(got.Spec.Autoscaling.MaxScale).NotTo(BeNil())
			Expect(*got.Spec.Autoscaling.MaxScale).To(Equal(int32(7)))
		})

		It("updates the watcher-owned adoption when annotations change", func() {
			Expect(k8sClient.Create(ictx, labeledDeployment(name, ns, map[string]string{
				"ifaas.ifbiu.com/max-scale": "3",
			}))).To(Succeed())
			_, err := w.Reconcile(ictx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
			Expect(err).NotTo(HaveOccurred())

			fresh := &appsv1.Deployment{}
			Expect(k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, fresh)).To(Succeed())
			fresh.Annotations = map[string]string{"ifaas.ifbiu.com/max-scale": "9"}
			Expect(k8sClient.Update(ictx, fresh)).To(Succeed())

			_, err = w.Reconcile(ictx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
			Expect(err).NotTo(HaveOccurred())

			got := &ifaasv1alpha1.KnativeAdoption{}
			Expect(k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, got)).To(Succeed())
			Expect(*got.Spec.Autoscaling.MaxScale).To(Equal(int32(9)))
		})
	})

	Context("when the label is removed", func() {
		const name = "drop-label"

		AfterEach(func() {
			_ = k8sClient.Delete(ictx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
			_ = k8sClient.Delete(ictx, &ifaasv1alpha1.KnativeAdoption{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
		})

		It("deletes the watcher-owned adoption", func() {
			Expect(k8sClient.Create(ictx, labeledDeployment(name, ns, nil))).To(Succeed())
			_, err := w.Reconcile(ictx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
			Expect(err).NotTo(HaveOccurred())

			fresh := &appsv1.Deployment{}
			Expect(k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, fresh)).To(Succeed())
			delete(fresh.Labels, LabelEnabled)
			Expect(k8sClient.Update(ictx, fresh)).To(Succeed())

			_, err = w.Reconcile(ictx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
			Expect(err).NotTo(HaveOccurred())

			err = k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, &ifaasv1alpha1.KnativeAdoption{})
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})
	})

	Context("when the Deployment is deleted", func() {
		const name = "vanish"

		AfterEach(func() {
			_ = k8sClient.Delete(ictx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
			_ = k8sClient.Delete(ictx, &ifaasv1alpha1.KnativeAdoption{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
		})

		It("deletes the watcher-owned adoption", func() {
			Expect(k8sClient.Create(ictx, labeledDeployment(name, ns, nil))).To(Succeed())
			_, err := w.Reconcile(ictx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Delete(ictx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})).To(Succeed())

			_, err = w.Reconcile(ictx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
			Expect(err).NotTo(HaveOccurred())

			err = k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, &ifaasv1alpha1.KnativeAdoption{})
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})
	})

	Context("when a user-authored adoption already exists", func() {
		const name = "user-owned"

		AfterEach(func() {
			_ = k8sClient.Delete(ictx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
			_ = k8sClient.Delete(ictx, &ifaasv1alpha1.KnativeAdoption{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
		})

		It("does not touch it on label-enabled events", func() {
			Expect(k8sClient.Create(ictx, &ifaasv1alpha1.KnativeAdoption{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
				Spec: ifaasv1alpha1.KnativeAdoptionSpec{
					SourceRef:        ifaasv1alpha1.SourceRef{Kind: ifaasv1alpha1.SourceKindDeployment, Name: name},
					Mode:             ifaasv1alpha1.ModeServing,
					PrimaryContainer: "human-pick",
				},
			})).To(Succeed())
			Expect(k8sClient.Create(ictx, labeledDeployment(name, ns, map[string]string{
				"ifaas.ifbiu.com/primary-container": "robot-pick",
			}))).To(Succeed())

			_, err := w.Reconcile(ictx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
			Expect(err).NotTo(HaveOccurred())

			got := &ifaasv1alpha1.KnativeAdoption{}
			Expect(k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, got)).To(Succeed())
			Expect(got.Labels[LabelManagedByWatcher]).To(BeEmpty())
			Expect(got.Spec.PrimaryContainer).To(Equal("human-pick"))
		})

		It("does not delete it on label-disabled events", func() {
			Expect(k8sClient.Create(ictx, &ifaasv1alpha1.KnativeAdoption{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
				Spec: ifaasv1alpha1.KnativeAdoptionSpec{
					SourceRef: ifaasv1alpha1.SourceRef{Kind: ifaasv1alpha1.SourceKindDeployment, Name: name},
					Mode:      ifaasv1alpha1.ModeServing,
				},
			})).To(Succeed())

			// Deployment intentionally absent.
			_, err := w.Reconcile(ictx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
			Expect(err).NotTo(HaveOccurred())

			got := &ifaasv1alpha1.KnativeAdoption{}
			Expect(k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, got)).To(Succeed())
			Expect(got.DeletionTimestamp).To(BeNil())
		})
	})
})
