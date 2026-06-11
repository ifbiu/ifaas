//go:build e2e
// +build e2e

/*
Copyright 2026.
*/

package e2e

import (
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/rand"

	"github.com/ifbiu/ifaas/test/utils"
)

// Webhook rejection paths. Each "It" creates its own KnativeAdoption /
// Deployment fresh so failures stay isolated; we deliberately avoid
// Ordered here because the cases are independent admission decisions.
var _ = Describe("validating webhook rejections", func() {
	var ns string

	BeforeEach(func() {
		ns = withNamespace("ifaas-webhook")
	})

	It("rejects KnativeAdoption pointing at a missing Deployment", func() {
		cr := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "ifaas.ifbiu.com/v1alpha1",
			"kind":       "KnativeAdoption",
			"metadata":   map[string]any{"name": "ghost", "namespace": ns},
			"spec": map[string]any{
				"sourceRef": map[string]any{"name": "does-not-exist"},
			},
		}}
		_, err := clients.Dynamic.Resource(utils.GVRKnativeAdoption).Namespace(ns).
			Create(ctxBackground(), cr, metav1.CreateOptions{})
		Expect(err).To(HaveOccurred())
		Expect(strings.ToLower(err.Error())).To(ContainSubstring("does not exist"))
	})

	It("rejects KnativeAdoption when an HPA already targets the source Deployment", func() {
		appName := "hpa-" + strings.ToLower(rand.String(4))
		applyStubDeployment(ns, stubDeploymentOpts{name: appName})

		var minR int32 = 1
		hpa := &autoscalingv2.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: appName, Namespace: ns},
			Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
					APIVersion: "apps/v1", Kind: "Deployment", Name: appName,
				},
				MinReplicas: &minR,
				MaxReplicas: 5,
			},
		}
		_, err := clients.Typed.AutoscalingV2().HorizontalPodAutoscalers(ns).
			Create(ctxBackground(), hpa, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		cr := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "ifaas.ifbiu.com/v1alpha1",
			"kind":       "KnativeAdoption",
			"metadata":   map[string]any{"name": appName, "namespace": ns},
			"spec":       map[string]any{"sourceRef": map[string]any{"name": appName}},
		}}
		_, err = clients.Dynamic.Resource(utils.GVRKnativeAdoption).Namespace(ns).
			Create(ctxBackground(), cr, metav1.CreateOptions{})
		Expect(err).To(HaveOccurred())
		Expect(strings.ToLower(err.Error())).To(ContainSubstring("hpa"))
	})

	It("rejects mode=eventing without spec.eventing.broker", func() {
		appName := "evt-" + strings.ToLower(rand.String(4))
		applyStubDeployment(ns, stubDeploymentOpts{name: appName})

		cr := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "ifaas.ifbiu.com/v1alpha1",
			"kind":       "KnativeAdoption",
			"metadata":   map[string]any{"name": appName, "namespace": ns},
			"spec": map[string]any{
				"sourceRef": map[string]any{"name": appName},
				"mode":      "eventing",
			},
		}}
		_, err := clients.Dynamic.Resource(utils.GVRKnativeAdoption).Namespace(ns).
			Create(ctxBackground(), cr, metav1.CreateOptions{})
		Expect(err).To(HaveOccurred())
		Expect(strings.ToLower(err.Error())).To(ContainSubstring("broker"))
	})

	It("freezes spec.sourceRef on update", func() {
		first := "src-" + strings.ToLower(rand.String(4))
		second := "src-" + strings.ToLower(rand.String(4))
		applyStubDeployment(ns, stubDeploymentOpts{name: first})
		applyStubDeployment(ns, stubDeploymentOpts{name: second})

		cr := applyAdoption(ns, adoptionOpts{name: first, source: first})
		Expect(cr).NotTo(BeNil())

		// Mutate sourceRef.name to a *valid* second Deployment so the only
		// thing that should fail is the immutability rule.
		Eventually(func() error {
			fresh, err := clients.Dynamic.Resource(utils.GVRKnativeAdoption).Namespace(ns).
				Get(ctxBackground(), first, metav1Get())
			if err != nil {
				return err
			}
			Expect(unstructured.SetNestedField(fresh.Object, second, "spec", "sourceRef", "name")).To(Succeed())
			_, err = clients.Dynamic.Resource(utils.GVRKnativeAdoption).Namespace(ns).
				Update(ctxBackground(), fresh, metav1.UpdateOptions{})
			return err
		}, 30*time.Second, time.Second).Should(MatchError(ContainSubstring("immutable")),
			"spec.sourceRef must be rejected on update")
	})
})
