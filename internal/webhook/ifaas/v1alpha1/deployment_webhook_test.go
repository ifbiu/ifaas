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
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
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

	// SA bypass: the operator's S9 finalizer chain restores adopted
	// Deployments by writing 0→>=1; the same write from any other client
	// is the manual scale-up the webhook is supposed to refuse. The
	// validator distinguishes them by AdmissionRequest.UserInfo.Username,
	// so we drive both halves with two impersonating clients.
	//
	// Both impersonations carry the system:masters group so RBAC stops
	// gating the request: this isolates the test to webhook behaviour
	// (UserInfo.Username matching), which is what we want to assert.
	It("admits 0 → >0 from the ifaas controller SA, rejects from anyone else", func() {
		dep := makeDeployment("dw-sa-bypass", ns, 0, true)
		Expect(k8sClient.Create(ctx, dep)).To(Succeed())

		ctrlCfg := rest.CopyConfig(cfg)
		ctrlCfg.Impersonate = rest.ImpersonationConfig{
			UserName: testControllerUsername,
			Groups:   []string{"system:masters"},
		}
		ctrlClient, err := client.New(ctrlCfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())

		userCfg := rest.CopyConfig(cfg)
		userCfg.Impersonate = rest.ImpersonationConfig{
			UserName: "alice@example.com",
			Groups:   []string{"system:masters"},
		}
		userClient, err := client.New(userCfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())

		got := &appsv1.Deployment{}
		Expect(userClient.Get(ctx, types.NamespacedName{Name: "dw-sa-bypass", Namespace: ns}, got)).To(Succeed())
		r := int32(1)
		got.Spec.Replicas = &r
		Expect(userClient.Update(ctx, got)).NotTo(Succeed(),
			"non-controller users must still hit the 0→>0 ban")

		Expect(ctrlClient.Get(ctx, types.NamespacedName{Name: "dw-sa-bypass", Namespace: ns}, got)).To(Succeed())
		got.Spec.Replicas = &r
		Expect(ctrlClient.Update(ctx, got)).To(Succeed(),
			"the ifaas controller SA must be admitted so the S9 restore chain can complete")
	})

	// Phantom defaulting bypass: a GitOps-style SSA that claims a
	// non-replicas field on a labelled, quiesced Deployment is what
	// Argo emits in steady state once `syncOptions: ServerSideApply=true`
	// is on (see helmrepo/appmeta/templates/application.yaml). The
	// caller's manifest legitimately owns container image, env, etc.,
	// but never declares spec.replicas — so the apiserver leaves the
	// field on its current owner (ifaas-autopilot) and never feeds the
	// validator a synthetic 0→>0 to argue about. The naive replica-delta
	// rule still admits this case (newR == oldR == 0), so what we are
	// really asserting is "the ManagedFields rewiring did not start
	// rejecting routine reconciles by accident".
	//
	// The complementary case — caller SSA that *does* claim replicas —
	// is covered by the next spec; together they pin both halves of the
	// gate to real apiserver behaviour. The third corner, a client-side
	// `kubectl apply` SMP `{"spec":{"replicas":null}}`, transfers
	// ownership to the caller exactly the same way as `{"replicas": 1}`
	// (the K8s field tracker keys on the path, not the value), so it
	// cannot be distinguished from real intent at admission time. That
	// path is fenced off upstream by ServerSideApply=true +
	// RespectIgnoreDifferences=true on the Argo Application; no test
	// here can paper over it.
	It("admits a GitOps SSA that omits replicas (phantom defaulting bypass)", func() {
		dep := makeDeployment("dw-phantom", ns, 0, true)
		Expect(k8sClient.Create(ctx, dep)).To(Succeed())

		anchor := newReplicasAnchor("dw-phantom", ns, 0)
		Expect(k8sClient.Patch(ctx, anchor, client.Apply,
			client.FieldOwner(fieldOwnerAutopilot),
			client.ForceOwnership)).To(Succeed())

		sync := newGitOpsSync("dw-phantom", ns, "registry.example/echo:v2")
		Expect(k8sClient.Patch(ctx, sync, client.Apply,
			client.FieldOwner("argocd-controller"),
			client.ForceOwnership)).To(Succeed(),
			"GitOps SSA that omits replicas must not be conflated with manual scale-up")

		got := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "dw-phantom", Namespace: ns}, got)).To(Succeed())
		Expect(got.Spec.Replicas).NotTo(BeNil())
		Expect(*got.Spec.Replicas).To(BeEquivalentTo(0),
			"ifaas-autopilot still owns spec.replicas, so the quiesced value must persist")
		Expect(got.Spec.Template.Spec.Containers[0].Image).To(Equal("registry.example/echo:v2"))
	})

	// Symmetric counterpart of the phantom case: a *real* SSA from a
	// non-ifaas fieldManager that explicitly claims spec.replicas=1
	// must still hit the 0→>0 ban. This is the case where the
	// caller has expressed intent — ownership of the field transfers
	// to the external manager — and the validator's job is to refuse.
	It("rejects a server-side apply from an external manager that sets replicas", func() {
		dep := makeDeployment("dw-external-ssa", ns, 0, true)
		Expect(k8sClient.Create(ctx, dep)).To(Succeed())

		anchor := newReplicasAnchor("dw-external-ssa", ns, 0)
		Expect(k8sClient.Patch(ctx, anchor, client.Apply,
			client.FieldOwner(fieldOwnerAutopilot),
			client.ForceOwnership)).To(Succeed())

		external := newReplicasAnchor("dw-external-ssa", ns, 1)
		err := k8sClient.Patch(ctx, external, client.Apply,
			client.FieldOwner("argocd-controller"),
			client.ForceOwnership)
		Expect(err).To(HaveOccurred(),
			"external SSA that claims replicas must hit the 0→>0 ban")
		Expect(err.Error()).To(ContainSubstring("manual scale-up from 0 is rejected"))
	})

	// kubectl-scale bypass closure: the main Deployment webhook only
	// matches PUT/PATCH on the apps/v1 Deployment resource. A
	// `kubectl scale deploy/x --replicas=N` call goes to the
	// `deployments/scale` *subresource*, whose admission request body
	// is autoscaling/v1.Scale and carries no labels or managedFields —
	// the typed CustomValidator wired to *appsv1.Deployment never sees
	// it. P7 case B caught this in the kind cluster and surfaced as
	// the dedicated DeploymentScaleValidator (deployment_scale_webhook.go).
	//
	// These two specs lock the closure to live apiserver behaviour, in
	// the user-facing direction (typed clientset.UpdateScale): no
	// in-process shortcuts that could mask a future regression. The
	// validator's other branches (controller-SA bypass, malformed
	// payloads, missing parent) are pinned in the Handle-level table
	// test next door; here we only assert the two paths a real user
	// can reach.
	It("rejects kubectl-scale-style 0→>0 on labelled Deployments via the /scale subresource", func() {
		dep := makeDeployment("dw-scale-block", ns, 0, true)
		Expect(k8sClient.Create(ctx, dep)).To(Succeed())

		clientset, err := kubernetes.NewForConfig(cfg)
		Expect(err).NotTo(HaveOccurred())

		scale, err := clientset.AppsV1().Deployments(ns).GetScale(ctx, "dw-scale-block", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		scale.Spec.Replicas = 2
		_, err = clientset.AppsV1().Deployments(ns).UpdateScale(ctx, "dw-scale-block", scale, metav1.UpdateOptions{})
		Expect(err).To(HaveOccurred(),
			"kubectl scale on a labelled, quiesced Deployment must hit the same 0→>0 ban as a direct .spec.replicas update")
		Expect(err.Error()).To(ContainSubstring("manual scale-up from 0 is rejected"))

		got := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "dw-scale-block", Namespace: ns}, got)).To(Succeed())
		Expect(got.Spec.Replicas).NotTo(BeNil())
		Expect(*got.Spec.Replicas).To(BeEquivalentTo(0),
			"the rejected scale-up must not have leaked through to the parent object")
	})

	It("admits kubectl-scale-style 0→>0 on Deployments without the autopilot label", func() {
		dep := makeDeployment("dw-scale-noop", ns, 0, false)
		Expect(k8sClient.Create(ctx, dep)).To(Succeed())

		clientset, err := kubernetes.NewForConfig(cfg)
		Expect(err).NotTo(HaveOccurred())

		scale, err := clientset.AppsV1().Deployments(ns).GetScale(ctx, "dw-scale-noop", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		scale.Spec.Replicas = 2
		_, err = clientset.AppsV1().Deployments(ns).UpdateScale(ctx, "dw-scale-noop", scale, metav1.UpdateOptions{})
		Expect(err).NotTo(HaveOccurred(),
			"the scale validator must defer to the autopilot label and not police arbitrary Deployments")
	})
})

// silence unused-import linters until autoscalingv1 is referenced inside
// a spec body (which it isn't yet — the GetScale flow uses the typed
// clientset, which round-trips its own autoscalingv1.Scale internally).
var _ = autoscalingv1.Scale{}

// newReplicasAnchor builds the minimal SSA payload the operator's own
// quiesce path uses: only apiVersion/kind/name/namespace and spec.replicas.
// A typed appsv1.Deployment{} would marshal `"selector": null` from its
// zero-valued *LabelSelector, which the apiserver rejects as an attempt
// to clear an immutable field; Unstructured lets us emit exactly the
// fields we mean to claim and nothing else.
func newReplicasAnchor(name, ns string, replicas int64) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion("apps/v1")
	u.SetKind("Deployment")
	u.SetNamespace(ns)
	u.SetName(name)
	if err := unstructured.SetNestedField(u.Object, replicas, "spec", "replicas"); err != nil {
		panic(err)
	}
	return u
}

// newGitOpsSync builds the SSA payload an Argo-style GitOps controller
// emits in steady state: it claims selector + template (the parts that
// actually live in the user's manifest) but *not* spec.replicas, which
// is autopilot territory. The resulting fieldset is what tells the
// validator the caller never expressed a scale-up intent — without it
// any external SSA on this Deployment would look identical at the
// ManagedFields layer to a real "scale to N".
func newGitOpsSync(name, ns, image string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion("apps/v1")
	u.SetKind("Deployment")
	u.SetNamespace(ns)
	u.SetName(name)
	spec := map[string]interface{}{
		"selector": map[string]interface{}{
			"matchLabels": map[string]interface{}{"app": name},
		},
		"template": map[string]interface{}{
			"metadata": map[string]interface{}{
				"labels": map[string]interface{}{"app": name},
			},
			"spec": map[string]interface{}{
				"containers": []interface{}{
					map[string]interface{}{
						"name":  "user-container",
						"image": image,
					},
				},
			},
		},
	}
	if err := unstructured.SetNestedField(u.Object, spec, "spec"); err != nil {
		panic(err)
	}
	return u
}
