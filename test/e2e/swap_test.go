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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
)

// Verifies the ServiceSwapper path: a pre-existing core/v1.Service with
// the same name as the workload must be renamed (or otherwise yielded)
// when the operator publishes a KSvc, because Knative serving creates a
// Service of its own with that exact name.
//
// We do not assert on the exact swap technique (rename-with-prefix vs
// delete-and-recreate); we only require that:
//
//  1. the user's original selector survives somewhere with the same
//     pod targets, and
//  2. a KSvc with the workload name comes up.
//
// This keeps the test resilient to swap-policy refinements done in
// later milestones.
var _ = Describe("service-name swap", Ordered, func() {
	var (
		ns      string
		appName string
	)

	BeforeAll(func() {
		ns = withNamespace("ifaas-swap")
		appName = "swap-" + strings.ToLower(rand.String(4))
	})

	It("creates a user Service with the same name as the workload", func() {
		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: appName, Namespace: ns},
			Spec: corev1.ServiceSpec{
				Selector: map[string]string{"app": appName},
				Ports: []corev1.ServicePort{{
					Name: "http", Port: 80, TargetPort: intStrFromInt(8080),
				}},
			},
		}
		_, err := clients.Typed.CoreV1().Services(ns).Create(ctxBackground(), svc, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())
	})

	It("brings the workload Deployment under autopilot", func() {
		applyStubDeployment(ns, stubDeploymentOpts{
			name:           appName,
			autopilotLabel: true,
			allowEnv:       "false",
		})
	})

	It("converges to a KSvc owning the workload name", func() {
		eventuallyKSvcExists(ns, appName, 3*time.Minute)
	})

	It("scales the source Deployment to 0 once the swap is in place", func() {
		eventuallyReplicas(ns, appName, 0, 3*time.Minute)
	})

	It("keeps a Service object pointing at the user's pods (under any name)", func() {
		// We do not pin the post-swap name; we just require *some* Service
		// in the namespace whose selector still references the user's
		// label so traffic can keep landing on the workload pods.
		Eventually(func() bool {
			list, err := clients.Typed.CoreV1().Services(ns).List(ctxBackground(), metav1.ListOptions{})
			if err != nil {
				return false
			}
			for _, s := range list.Items {
				if s.Spec.Selector["app"] == appName {
					return true
				}
			}
			return false
		}, 90*time.Second, 2*time.Second).Should(BeTrue(),
			"a Service selecting the workload pods must remain after swap")
	})
})
