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
	"encoding/json"
	"errors"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kservingv1 "knative.dev/serving/pkg/apis/serving/v1"

	ifaasv1alpha1 "github.com/ifbiu/ifaas/api/ifaas/v1alpha1"
)

// makeServiceForRestore returns a ClusterIP Service spec the test can persist
// into the apiserver before the swapper takes over. We keep the shape minimal
// (one TCP port, one selector pair) so post-restore equality assertions are
// easy to read.
func makeServiceForRestore(name, ns string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: map[string]string{"app": name},
			Ports: []corev1.ServicePort{
				{Name: "http", Port: 80, TargetPort: intstr.FromInt(8080), Protocol: corev1.ProtocolTCP},
			},
		},
	}
}

var _ = Describe("KnativeAdoption Restore (S9)", func() {
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

	Context("end-to-end adopt → release cycle", func() {
		const name = "restore-e2e"

		AfterEach(func() {
			cleanupAdoption(ictx, name, ns)
			_ = k8sClient.Delete(ictx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
			_ = k8sClient.Delete(ictx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
			_ = k8sClient.Delete(ictx, &kservingv1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
		})

		It("rebuilds the original Service and restores Deployment replicas when the CR is deleted", func() {
			By("seeding a pre-existing Deployment+Service the operator will adopt (Deployment carries the autopilot trigger label)")
			origSvc := makeServiceForRestore(name, ns)
			seedDep := makeDeployment(name, ns, 4)
			seedDep.Labels = map[string]string{LabelEnabled: LabelEnabledValue}
			Expect(k8sClient.Create(ictx, seedDep)).To(Succeed())
			Expect(k8sClient.Create(ictx, origSvc)).To(Succeed())
			Expect(k8sClient.Create(ictx, makeAdoption(name, ns))).To(Succeed())

			By("the first reconcile takes over the Service (snapshot+delete) and asks for a short requeue")
			res, err := rec.Reconcile(ictx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(Equal(requeueAfterServiceSwap))

			Eventually(func() bool {
				err := k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, &corev1.Service{})
				return apierrors.IsNotFound(err)
			}, 5*time.Second, 50*time.Millisecond).Should(BeTrue(), "swapper must delete the original Service before applying KSvc")

			By("the next reconcile applies the KSvc, scales the Deployment to zero, and snapshots the originals")
			_, err = rec.Reconcile(ictx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
			Expect(err).NotTo(HaveOccurred())

			ksvc := &kservingv1.Service{}
			Expect(k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, ksvc)).To(Succeed())

			adopted := &ifaasv1alpha1.KnativeAdoption{}
			Expect(k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, adopted)).To(Succeed())
			Expect(adopted.Finalizers).To(ContainElement(FinalizerRestoreSourceService))
			Expect(adopted.Finalizers).To(ContainElement(FinalizerRestoreSource))
			Expect(adopted.Status.SourceSnapshot).NotTo(BeNil())
			Expect(adopted.Status.SourceSnapshot.Replicas).NotTo(BeNil())
			Expect(*adopted.Status.SourceSnapshot.Replicas).To(Equal(int32(4)))
			Expect(adopted.Status.SourceSnapshot.Service).NotTo(BeNil())
			Expect(adopted.Status.SourceSnapshot.Service.Type).To(Equal(corev1.ServiceTypeClusterIP))
			Expect(adopted.Status.SourceSnapshot.Service.Ports).To(HaveLen(1))

			By("the user deletes the CR; envtest has no GC, so handleDeletion drives the cascade itself")
			Expect(k8sClient.Delete(ictx, adopted)).To(Succeed())

			By("repeated reconciles drain the finalizers in order: service rebuild → service finalizer → replicas restore → source finalizer")
			Eventually(func(g Gomega) {
				_, err := rec.Reconcile(ictx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
				g.Expect(err).NotTo(HaveOccurred())

				cur := &ifaasv1alpha1.KnativeAdoption{}
				err = k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, cur)
				g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "CR must be fully released")
			}, 10*time.Second, 100*time.Millisecond).Should(Succeed())

			By("the original Service has been rebuilt from the snapshot")
			rebuilt := &corev1.Service{}
			Expect(k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, rebuilt)).To(Succeed())
			Expect(rebuilt.Spec.Type).To(Equal(corev1.ServiceTypeClusterIP))
			Expect(rebuilt.Spec.Selector).To(Equal(map[string]string{"app": name}))
			Expect(rebuilt.Spec.Ports).To(HaveLen(1))
			Expect(rebuilt.Spec.Ports[0].Port).To(Equal(int32(80)))
			Expect(rebuilt.Annotations[AnnoServiceManagedBy]).To(Equal(AnnoServiceManagedByValue))

			By("the source Deployment is back at its original replica count and the autopilot trigger label has been consumed")
			dep := &appsv1.Deployment{}
			Expect(k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, dep)).To(Succeed())
			Expect(dep.Spec.Replicas).NotTo(BeNil())
			Expect(*dep.Spec.Replicas).To(Equal(int32(4)))
			Expect(dep.Labels).NotTo(HaveKey(LabelEnabled),
				"finalizer must strip the adoption-trigger label so DeploymentWatcher does not re-materialise the CR after physical deletion")
		})
	})

	Context("partial restore is resumable across reconciles", func() {
		const name = "restore-resume"

		AfterEach(func() {
			cleanupAdoption(ictx, name, ns)
			_ = k8sClient.Delete(ictx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
			_ = k8sClient.Delete(ictx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
		})

		It("removes only the source-finalizer when the service-finalizer is already gone", func() {
			By("constructing an adoption that has already finished phase A (service rebuilt, svc finalizer dropped)")
			r := int32(2)
			Expect(k8sClient.Create(ictx, &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: ns,
					Labels:    map[string]string{LabelEnabled: LabelEnabledValue},
				},
				Spec: appsv1.DeploymentSpec{
					Replicas: ptrInt32(0),
					Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": name}},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{Name: "c", Image: "registry.example/echo:v1"}},
						},
					},
				},
			})).To(Succeed())

			a := &ifaasv1alpha1.KnativeAdoption{
				ObjectMeta: metav1.ObjectMeta{
					Name:       name,
					Namespace:  ns,
					Finalizers: []string{FinalizerRestoreSource},
				},
				Spec: ifaasv1alpha1.KnativeAdoptionSpec{
					SourceRef: ifaasv1alpha1.SourceRef{Kind: ifaasv1alpha1.SourceKindDeployment, Name: name},
					Mode:      ifaasv1alpha1.ModeServing,
				},
			}
			Expect(k8sClient.Create(ictx, a)).To(Succeed())

			Expect(k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, a)).To(Succeed())
			a.Status.SourceSnapshot = &ifaasv1alpha1.SourceSnapshot{Replicas: &r}
			Expect(k8sClient.Status().Update(ictx, a)).To(Succeed())

			Expect(k8sClient.Delete(ictx, a)).To(Succeed())

			By("a single reconcile is enough to restore replicas and drop the last finalizer")
			Eventually(func(g Gomega) {
				_, err := rec.Reconcile(ictx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
				g.Expect(err).NotTo(HaveOccurred())

				cur := &ifaasv1alpha1.KnativeAdoption{}
				err = k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, cur)
				g.Expect(apierrors.IsNotFound(err)).To(BeTrue())
			}, 5*time.Second, 50*time.Millisecond).Should(Succeed())

			dep := &appsv1.Deployment{}
			Expect(k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, dep)).To(Succeed())
			Expect(*dep.Spec.Replicas).To(Equal(int32(2)))
			Expect(dep.Labels).NotTo(HaveKey(LabelEnabled),
				"phase B must consume the autopilot trigger label even when phase A has already finished")
		})
	})

	Context("traffic snapshot restore in deletion phase", func() {
		It("restores VS/DR objects to the snapshotted spec payload", func() {
			const name = "restore-traffic-fake"
			vsSpec := map[string]interface{}{
				"hosts": []interface{}{"restore.example.com"},
				"gateways": []interface{}{"netops/common-inbound-gateway"},
				"http": []interface{}{
					map[string]interface{}{
						"route": []interface{}{
							map[string]interface{}{
								"destination": map[string]interface{}{
									"host": "gops-ckbackup.default.svc.cluster.local",
									"port": map[string]interface{}{"number": int64(5555)},
								},
								"weight": int64(100),
							},
						},
					},
				},
			}
			drSpec := map[string]interface{}{
				"host": "gops-ckbackup.default.svc.cluster.local",
				"subsets": []interface{}{
					map[string]interface{}{
						"name": "stable",
						"labels": map[string]interface{}{"app": "gops-ckbackup"},
					},
				},
			}

			driftDR := makeDestinationRule(
				"managed-dr",
				ns,
				"gops-ckbackup.default.svc.cluster.local",
				[]interface{}{makeSubset("drifted")},
			)

			fakeRec := newRestoreFakeReconciler(driftDR)
			adoption := makeAdoption(name, ns)
			adoption.Status.SourceSnapshot = &ifaasv1alpha1.SourceSnapshot{
				Traffic: &ifaasv1alpha1.TrafficSnapshot{
					VirtualServices: []ifaasv1alpha1.TrafficObjectSnapshot{{
						Name: "managed-vs",
						Spec: mustRawExtension(vsSpec),
					}},
					DestinationRules: []ifaasv1alpha1.TrafficObjectSnapshot{{
						Name: "managed-dr",
						Spec: mustRawExtension(drSpec),
					}},
				},
			}

			Expect(fakeRec.restoreTrafficSnapshots(ictx, adoption)).To(Succeed())

			vs := &unstructured.Unstructured{}
			vs.SetGroupVersionKind(virtualServiceGVK)
			Expect(fakeRec.Get(ictx, types.NamespacedName{Namespace: ns, Name: "managed-vs"}, vs)).To(Succeed())
			vsRestoredSpec, err := rawSpecSnapshot(vs)
			Expect(err).NotTo(HaveOccurred())
			Expect(vsRestoredSpec.Raw).To(MatchJSON(string(adoption.Status.SourceSnapshot.Traffic.VirtualServices[0].Spec.Raw)))

			dr := &unstructured.Unstructured{}
			dr.SetGroupVersionKind(destinationRuleGVK)
			Expect(fakeRec.Get(ictx, types.NamespacedName{Namespace: ns, Name: "managed-dr"}, dr)).To(Succeed())
			drRestoredSpec, err := rawSpecSnapshot(dr)
			Expect(err).NotTo(HaveOccurred())
			Expect(drRestoredSpec.Raw).To(MatchJSON(string(adoption.Status.SourceSnapshot.Traffic.DestinationRules[0].Spec.Raw)))

			Expect(fakeRec.restoreTrafficSnapshots(ictx, adoption)).To(Succeed())
			vsAfterSecond := &unstructured.Unstructured{}
			vsAfterSecond.SetGroupVersionKind(virtualServiceGVK)
			Expect(fakeRec.Get(ictx, types.NamespacedName{Namespace: ns, Name: "managed-vs"}, vsAfterSecond)).To(Succeed())
			vsSpecAfterSecond, err := rawSpecSnapshot(vsAfterSecond)
			Expect(err).NotTo(HaveOccurred())
			Expect(vsSpecAfterSecond.Raw).To(MatchJSON(string(adoption.Status.SourceSnapshot.Traffic.VirtualServices[0].Spec.Raw)))
		})

		It("fails before service restore when traffic restore cannot proceed", func() {
			const name = "restore-order-traffic-first"
			svcSpec := makeServiceForRestore(name, ns).Spec
			adoption := makeAdoption(name, ns)
			adoption.Finalizers = []string{FinalizerRestoreSourceService}
			adoption.Status.SourceSnapshot = &ifaasv1alpha1.SourceSnapshot{
				Service: &svcSpec,
				Traffic: &ifaasv1alpha1.TrafficSnapshot{
					VirtualServices: []ifaasv1alpha1.TrafficObjectSnapshot{{
						Name: "managed-vs",
						Spec: mustRawExtension(map[string]interface{}{
							"hosts": []interface{}{"restore.example.com"},
							"http":  []interface{}{},
						}),
					}},
				},
			}

			baseRec := newRestoreFakeReconciler(adoption)
			failingRec := &KnativeAdoptionReconciler{
				Client:       &failVirtualServiceGetClient{Client: baseRec.Client},
				Scheme:       baseRec.Scheme,
				ServiceReady: alwaysReady,
			}
			cur := &ifaasv1alpha1.KnativeAdoption{}
			Expect(failingRec.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, cur)).To(Succeed())

			_, err := failingRec.handleDeletion(ictx, cur)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("injected virtualservice get failure"))

			service := &corev1.Service{}
			err = failingRec.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, service)
			Expect(apierrors.IsNotFound(err)).To(BeTrue(), "service must not be rebuilt when traffic restore fails")
			Expect(controllerutil.ContainsFinalizer(cur, FinalizerRestoreSourceService)).To(BeTrue(),
				"service finalizer should remain so deletion can resume after traffic restore recovers")
		})
	})

	Context("ensureFinalizers is idempotent", func() {
		const name = "restore-finalizers"

		AfterEach(func() {
			cleanupAdoption(ictx, name, ns)
		})

		It("adds both finalizers on first call and is a no-op afterwards", func() {
			a := makeAdoption(name, ns)
			Expect(k8sClient.Create(ictx, a)).To(Succeed())

			Expect(rec.ensureFinalizers(ictx, a)).To(Succeed())
			got := &ifaasv1alpha1.KnativeAdoption{}
			Expect(k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, got)).To(Succeed())
			Expect(controllerutil.ContainsFinalizer(got, FinalizerRestoreSourceService)).To(BeTrue())
			Expect(controllerutil.ContainsFinalizer(got, FinalizerRestoreSource)).To(BeTrue())

			rvBefore := got.ResourceVersion
			Expect(rec.ensureFinalizers(ictx, got)).To(Succeed())
			Expect(k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: name}, got)).To(Succeed())
			Expect(got.ResourceVersion).To(Equal(rvBefore), "second ensureFinalizers must not patch the apiserver")
		})
	})
})

func ptrInt32(v int32) *int32 { return &v }

func newRestoreFakeReconciler(objs ...client.Object) *KnativeAdoptionReconciler {
	s := newRestoreFakeScheme()
	cl := fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()
	return &KnativeAdoptionReconciler{Client: cl, Scheme: s, ServiceReady: alwaysReady}
}

func newRestoreFakeScheme() *runtime.Scheme {
	s := newRestoreBaseScheme()
	s.AddKnownTypeWithName(virtualServiceGVK, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group:   virtualServiceGVK.Group,
		Version: virtualServiceGVK.Version,
		Kind:    "VirtualServiceList",
	}, &unstructured.UnstructuredList{})
	s.AddKnownTypeWithName(destinationRuleGVK, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group:   destinationRuleGVK.Group,
		Version: destinationRuleGVK.Version,
		Kind:    "DestinationRuleList",
	}, &unstructured.UnstructuredList{})
	return s
}

func newRestoreBaseScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	mustAddToScheme(s, ifaasv1alpha1.AddToScheme)
	mustAddToScheme(s, appsv1.AddToScheme)
	mustAddToScheme(s, corev1.AddToScheme)
	mustAddToScheme(s, kservingv1.AddToScheme)
	return s
}

func mustAddToScheme(s *runtime.Scheme, add func(*runtime.Scheme) error) {
	if err := add(s); err != nil {
		panic(err)
	}
}

func mustRawExtension(spec map[string]interface{}) runtime.RawExtension {
	raw, err := json.Marshal(spec)
	if err != nil {
		panic(err)
	}
	return runtime.RawExtension{Raw: raw}
}

type failVirtualServiceGetClient struct {
	client.Client
}

func (c *failVirtualServiceGetClient) Get(ctx context.Context, key types.NamespacedName, obj client.Object, opts ...client.GetOption) error {
	if u, ok := obj.(*unstructured.Unstructured); ok {
		if u.GroupVersionKind() == virtualServiceGVK {
			return errors.New("injected virtualservice get failure")
		}
	}
	return c.Client.Get(ctx, key, obj, opts...)
}
