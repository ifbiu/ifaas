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
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kservingv1 "knative.dev/serving/pkg/apis/serving/v1"

	ifaasv1alpha1 "github.com/ifbiu/ifaas/api/ifaas/v1alpha1"
)

func makeService(name, ns string, t corev1.ServiceType) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: corev1.ServiceSpec{
			Type:     t,
			Selector: map[string]string{"app": name},
			Ports: []corev1.ServicePort{{
				Name:       "http",
				Port:       80,
				TargetPort: intstr.FromInt32(8080),
				Protocol:   corev1.ProtocolTCP,
			}},
		},
	}
}

func TestClassifyServiceForSwap(t *testing.T) {
	cases := []struct {
		name string
		svc  *corev1.Service
		want SwapDecision
	}{
		{name: "nil", svc: nil, want: SwapPass},
		{name: "managed-by", svc: func() *corev1.Service {
			s := makeService("x", "ns", corev1.ServiceTypeClusterIP)
			s.Annotations = map[string]string{AnnoServiceManagedBy: AnnoServiceManagedByValue}
			return s
		}(), want: SwapPass},
		{name: "knative-route", svc: func() *corev1.Service {
			s := makeService("x", "ns", corev1.ServiceTypeClusterIP)
			tr := true
			s.OwnerReferences = []metav1.OwnerReference{{
				APIVersion: "serving.knative.dev/v1", Kind: "Route",
				Name: "x", Controller: &tr,
			}}
			return s
		}(), want: SwapPass},
		{name: "loadbalancer", svc: makeService("x", "ns", corev1.ServiceTypeLoadBalancer), want: SwapRefused},
		{name: "nodeport", svc: makeService("x", "ns", corev1.ServiceTypeNodePort), want: SwapRefused},
		{name: "externalname", svc: makeService("x", "ns", corev1.ServiceTypeExternalName), want: SwapRefused},
		{name: "stateful-headless", svc: func() *corev1.Service {
			s := makeService("x", "ns", corev1.ServiceTypeClusterIP)
			tr := true
			s.OwnerReferences = []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "StatefulSet",
				Name: "x", Controller: &tr,
			}}
			return s
		}(), want: SwapRefused},
		{name: "plain-clusterip", svc: makeService("x", "ns", corev1.ServiceTypeClusterIP), want: SwapTakenOver},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, _, _ := ClassifyServiceForSwap(tc.svc)
			if got != tc.want {
				t.Errorf("decision: got %v want %v", got, tc.want)
			}
		})
	}
}

var _ = Describe("ServiceSwapper", func() {
	const ns = "default"

	var (
		ictx context.Context
		rec  *KnativeAdoptionReconciler
	)

	BeforeEach(func() {
		ictx = context.Background()
		rec = &KnativeAdoptionReconciler{
			Client:       k8sClient,
			Scheme:       k8sClient.Scheme(),
			ServiceReady: alwaysReady,
		}
	})

	Context("when a plain ClusterIP Service occupies the same name", func() {
		const name = "swap-clusterip"

		AfterEach(func() {
			cleanupAdoption(ictx, name, ns)
			_ = k8sClient.Delete(ictx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
			_ = k8sClient.Delete(ictx, &kservingv1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
			cleanupDeployment(ictx, name, ns)
		})

		It("snapshots the Service spec and deletes the original", func() {
			Expect(k8sClient.Create(ictx, makeService(name, ns, corev1.ServiceTypeClusterIP))).To(Succeed())
			Expect(k8sClient.Create(ictx, makeDeployment(name, ns, 2))).To(Succeed())
			Expect(k8sClient.Create(ictx, makeAdoption(name, ns))).To(Succeed())

			res, err := rec.Reconcile(ictx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(Equal(requeueAfterServiceSwap))

			By("the original Service is deleted")
			Eventually(func() bool {
				err := k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, &corev1.Service{})
				return apierrors.IsNotFound(err)
			}, 5*time.Second, 100*time.Millisecond).Should(BeTrue())

			By("the snapshot is written and the finalizer is attached")
			got := &ifaasv1alpha1.KnativeAdoption{}
			Expect(k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, got)).To(Succeed())
			Expect(got.Status.SourceSnapshot).NotTo(BeNil())
			Expect(got.Status.SourceSnapshot.Service).NotTo(BeNil())
			Expect(got.Status.SourceSnapshot.Service.Type).To(Equal(corev1.ServiceTypeClusterIP))
			Expect(got.Status.SourceSnapshot.Service.Ports).To(HaveLen(1))
			Expect(got.Finalizers).To(ContainElement(FinalizerRestoreSourceService))

			By("the next reconcile succeeds and creates the KSvc")
			_, err = rec.Reconcile(ictx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, &kservingv1.Service{})).To(Succeed())
		})
	})

	Context("when a LoadBalancer Service occupies the same name", func() {
		const name = "swap-lb"

		AfterEach(func() {
			cleanupAdoption(ictx, name, ns)
			_ = k8sClient.Delete(ictx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
			cleanupDeployment(ictx, name, ns)
		})

		It("refuses adoption and does not create a KSvc", func() {
			Expect(k8sClient.Create(ictx, makeService(name, ns, corev1.ServiceTypeLoadBalancer))).To(Succeed())
			Expect(k8sClient.Create(ictx, makeDeployment(name, ns, 1))).To(Succeed())
			Expect(k8sClient.Create(ictx, makeAdoption(name, ns))).To(Succeed())

			_, err := rec.Reconcile(ictx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
			Expect(err).NotTo(HaveOccurred())

			got := &ifaasv1alpha1.KnativeAdoption{}
			Expect(k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, got)).To(Succeed())
			Expect(apimeta.IsStatusConditionTrue(got.Status.Conditions, ifaasv1alpha1.ConditionServiceAdoptionRefuse)).To(BeTrue())
			Expect(apimeta.IsStatusConditionTrue(got.Status.Conditions, ifaasv1alpha1.ConditionAdopted)).To(BeFalse())
			Expect(apimeta.IsStatusConditionTrue(got.Status.Conditions, ifaasv1alpha1.ConditionReady)).To(BeFalse())

			By("the original LoadBalancer Service is preserved")
			svc := &corev1.Service{}
			Expect(k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, svc)).To(Succeed())
			Expect(svc.Spec.Type).To(Equal(corev1.ServiceTypeLoadBalancer))

			By("no KSvc has been created")
			err = k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, &kservingv1.Service{})
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})
	})
})

func cleanupDeployment(ctx context.Context, name, ns string) {
	_ = k8sClient.Delete(ctx, &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
	})
}
