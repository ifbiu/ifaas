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
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kservingv1 "knative.dev/serving/pkg/apis/serving/v1"

	"github.com/ifbiu/ifaas/internal/flusher"
	"github.com/ifbiu/ifaas/internal/scaledown"
)

// recordingEnqueuer is the unit-test FlusherEnqueuer. It captures every
// decision in order so each spec can assert what the guard handed off.
type recordingEnqueuer struct {
	mu    sync.Mutex
	calls []flusher.Decision
}

func (e *recordingEnqueuer) Enqueue(d flusher.Decision) error {
	e.mu.Lock()
	e.calls = append(e.calls, d)
	e.mu.Unlock()
	return nil
}

func (e *recordingEnqueuer) snapshot() []flusher.Decision {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]flusher.Decision, len(e.calls))
	copy(out, e.calls)
	return out
}

var _ = Describe("ScaleDownGuard → Flusher wiring", func() {
	const ns = "default"

	var (
		ictx context.Context
		rec  *KnativeAdoptionReconciler
		enq  *recordingEnqueuer
	)

	BeforeEach(func() {
		ictx = context.Background()
		enq = &recordingEnqueuer{}
		rec = &KnativeAdoptionReconciler{
			Client:       k8sClient,
			Scheme:       k8sClient.Scheme(),
			ServiceReady: alwaysReady,
			Flusher:      enq,
		}
	})

	Context("when the prober refuses scale-to-zero", func() {
		const name = "wire-block"

		AfterEach(func() {
			cleanupAdoption(ictx, name, ns)
			_ = k8sClient.Delete(ictx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
			_ = k8sClient.Delete(ictx, &kservingv1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
			_ = k8sClient.Delete(ictx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name + "-0", Namespace: ns}})
		})

		It("enqueues a min-scale=1 decision against the flusher", func() {
			rec.Prober = &fakeProber{defaultResult: scaledown.Result{Allowed: false}}

			Expect(k8sClient.Create(ictx, makeDeployment(name, ns, 1))).To(Succeed())
			Expect(k8sClient.Create(ictx, makeAdoption(name, ns))).To(Succeed())

			// First reconcile: creates the KSvc, no guard pass yet.
			_, err := rec.Reconcile(ictx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
			Expect(err).NotTo(HaveOccurred())
			Expect(enq.snapshot()).To(BeEmpty(), "no enqueue before a guard round runs")

			// Materialise a Running pod so the second reconcile's guard
			// pre-pass has something to vote on.
			pod := makeKSvcPod(name+"-0", ns, name)
			Expect(k8sClient.Create(ictx, pod)).To(Succeed())
			markPodRunning(ictx, pod)

			// Second reconcile: guard runs, votes Block, enqueues to flusher.
			_, err = rec.Reconcile(ictx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
			Expect(err).NotTo(HaveOccurred())

			calls := enq.snapshot()
			Expect(calls).To(HaveLen(1))
			Expect(calls[0].Namespace).To(Equal(ns))
			Expect(calls[0].KSvcName).To(Equal(name))
			Expect(calls[0].DesiredMinScale).To(Equal(int32(1)))
		})
	})

	Context("when the prober allows scale-to-zero", func() {
		const name = "wire-allow"

		AfterEach(func() {
			cleanupAdoption(ictx, name, ns)
			_ = k8sClient.Delete(ictx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
			_ = k8sClient.Delete(ictx, &kservingv1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
			_ = k8sClient.Delete(ictx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name + "-0", Namespace: ns}})
		})

		It("enqueues a min-scale=0 decision against the flusher", func() {
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

			calls := enq.snapshot()
			Expect(calls).To(HaveLen(1))
			Expect(calls[0].DesiredMinScale).To(Equal(int32(0)))
		})
	})
})
