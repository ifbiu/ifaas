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
	"errors"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kservingv1 "knative.dev/serving/pkg/apis/serving/v1"

	ifaasv1alpha1 "github.com/ifbiu/ifaas/api/ifaas/v1alpha1"
	"github.com/ifbiu/ifaas/internal/scaledown"
	"github.com/ifbiu/ifaas/internal/translator"
)

// fakeProber is the unit-test side of scaledown.Prober. It returns a fixed
// Result per pod plus an atomic call counter so tests can assert "the guard
// ran exactly N times this round" without relying on timing.
type fakeProber struct {
	results       map[string]scaledown.Result
	defaultResult scaledown.Result
	calls         int64
}

func (f *fakeProber) Probe(_ context.Context, _, podName string, _ int32, _ string, _ time.Duration) scaledown.Result {
	atomic.AddInt64(&f.calls, 1)
	if r, ok := f.results[podName]; ok {
		return r
	}
	r := f.defaultResult
	r.PodName = podName
	return r
}

func makeKSvcPod(name, ns, ksvcName string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels: map[string]string{
				"serving.knative.dev/service": ksvcName,
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "user-container",
				Image: "registry.example/echo:v1",
				Ports: []corev1.ContainerPort{{ContainerPort: 8080}},
			}},
		},
	}
}

// markPodRunning bypasses kubelet and patches Pod.status.phase=Running so
// listKSvcPods accepts the pod. envtest exposes the /status subresource just
// like a real apiserver.
func markPodRunning(ctx context.Context, pod *corev1.Pod) {
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{{
		Type:   corev1.PodReady,
		Status: corev1.ConditionTrue,
	}}
	Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
}

var _ = Describe("ScaleDownGuard", func() {
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

	Context("when the prober refuses scale-to-zero", func() {
		const name = "guard-block"

		AfterEach(func() {
			cleanupAdoption(ictx, name, ns)
			_ = k8sClient.Delete(ictx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
			_ = k8sClient.Delete(ictx, &kservingv1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
			_ = k8sClient.Delete(ictx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name + "-0", Namespace: ns}})
		})

		It("pins min-scale=1 in the next translation and reports ScaleDownAllowed=False", func() {
			rec.Prober = &fakeProber{defaultResult: scaledown.Result{Allowed: false}}

			Expect(k8sClient.Create(ictx, makeDeployment(name, ns, 1))).To(Succeed())
			Expect(k8sClient.Create(ictx, makeAdoption(name, ns))).To(Succeed())

			By("first reconcile creates the KSvc (no guard pass yet)")
			_, err := rec.Reconcile(ictx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
			Expect(err).NotTo(HaveOccurred())

			ksvc := &kservingv1.Service{}
			Expect(k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, ksvc)).To(Succeed())
			Expect(ksvc.Spec.Template.GetAnnotations()[translator.AutoscalingMinScaleAnnotation]).To(Equal("0"))

			By("a Running pod is materialized to give the prober a target")
			pod := makeKSvcPod(name+"-0", ns, name)
			Expect(k8sClient.Create(ictx, pod)).To(Succeed())
			markPodRunning(ictx, pod)

			By("second reconcile runs the guard pre-pass and bumps min-scale to 1")
			_, err = rec.Reconcile(ictx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, ksvc)).To(Succeed())
			Expect(ksvc.Spec.Template.GetAnnotations()[translator.AutoscalingMinScaleAnnotation]).To(Equal("1"))

			got := &ifaasv1alpha1.KnativeAdoption{}
			Expect(k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, got)).To(Succeed())
			Expect(apimeta.IsStatusConditionTrue(got.Status.Conditions, ifaasv1alpha1.ConditionScaleDownAllowed)).To(BeFalse())
			Expect(apimeta.IsStatusConditionFalse(got.Status.Conditions, ifaasv1alpha1.ConditionScaleDownAllowed)).To(BeTrue())
			Expect(got.Status.LastScaleDownProbe).NotTo(BeNil())
			Expect(got.Status.LastScaleDownProbe.Result).To(Equal(ifaasv1alpha1.ProbeResultFalse))
		})
	})

	Context("when the prober allows scale-to-zero", func() {
		const name = "guard-allow"

		AfterEach(func() {
			cleanupAdoption(ictx, name, ns)
			_ = k8sClient.Delete(ictx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
			_ = k8sClient.Delete(ictx, &kservingv1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
			_ = k8sClient.Delete(ictx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name + "-0", Namespace: ns}})
		})

		It("keeps min-scale=0 and reports ScaleDownAllowed=True", func() {
			rec.Prober = &fakeProber{defaultResult: scaledown.Result{Allowed: true}}

			Expect(k8sClient.Create(ictx, makeDeployment(name, ns, 1))).To(Succeed())
			Expect(k8sClient.Create(ictx, makeAdoption(name, ns))).To(Succeed())
			_, err := rec.Reconcile(ictx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
			Expect(err).NotTo(HaveOccurred())

			pod := makeKSvcPod(name+"-0", ns, name)
			Expect(k8sClient.Create(ictx, pod)).To(Succeed())
			markPodRunning(ictx, pod)

			_, err = rec.Reconcile(ictx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
			Expect(err).NotTo(HaveOccurred())

			ksvc := &kservingv1.Service{}
			Expect(k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, ksvc)).To(Succeed())
			Expect(ksvc.Spec.Template.GetAnnotations()[translator.AutoscalingMinScaleAnnotation]).To(Equal("0"))

			got := &ifaasv1alpha1.KnativeAdoption{}
			Expect(k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, got)).To(Succeed())
			Expect(apimeta.IsStatusConditionTrue(got.Status.Conditions, ifaasv1alpha1.ConditionScaleDownAllowed)).To(BeTrue())
			Expect(got.Status.LastScaleDownProbe.Result).To(Equal(ifaasv1alpha1.ProbeResultTrue))
		})
	})

	Context("when the prober errors past the threshold", func() {
		const name = "guard-degraded"

		AfterEach(func() {
			cleanupAdoption(ictx, name, ns)
			_ = k8sClient.Delete(ictx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
			_ = k8sClient.Delete(ictx, &kservingv1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
			_ = k8sClient.Delete(ictx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name + "-0", Namespace: ns}})
		})

		It("raises Degraded after consecutiveFailureThreshold rounds", func() {
			rec.Prober = &fakeProber{defaultResult: scaledown.Result{Err: errors.New("boom")}}

			adoption := makeAdoption(name, ns)
			t := int32(2)
			adoption.Spec.ScaleDownProbe.ConsecutiveFailureThreshold = t
			Expect(k8sClient.Create(ictx, makeDeployment(name, ns, 1))).To(Succeed())
			Expect(k8sClient.Create(ictx, adoption)).To(Succeed())

			_, err := rec.Reconcile(ictx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
			Expect(err).NotTo(HaveOccurred())

			pod := makeKSvcPod(name+"-0", ns, name)
			Expect(k8sClient.Create(ictx, pod)).To(Succeed())
			markPodRunning(ictx, pod)

			By("round 1: counter = 1, Degraded not yet raised")
			_, err = rec.Reconcile(ictx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
			Expect(err).NotTo(HaveOccurred())
			got := &ifaasv1alpha1.KnativeAdoption{}
			Expect(k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, got)).To(Succeed())
			Expect(got.Status.LastScaleDownProbe.ConsecutiveErrors).To(Equal(int32(1)))
			Expect(apimeta.IsStatusConditionTrue(got.Status.Conditions, ifaasv1alpha1.ConditionDegraded)).To(BeFalse())

			By("round 2: counter hits threshold, Degraded is raised")
			_, err = rec.Reconcile(ictx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, got)).To(Succeed())
			Expect(got.Status.LastScaleDownProbe.ConsecutiveErrors).To(BeNumerically(">=", t))
			Expect(apimeta.IsStatusConditionTrue(got.Status.Conditions, ifaasv1alpha1.ConditionDegraded)).To(BeTrue())
		})
	})
})

// keep the client import attached so future helpers (e.g. Patch-based
// pod-status flips) don't trip the unused-import lint.
var _ = client.IgnoreNotFound
