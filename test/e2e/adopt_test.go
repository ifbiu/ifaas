//go:build e2e
// +build e2e

/*
Copyright 2026.
*/

package e2e

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
	"strings"

	"github.com/ifbiu/ifaas/test/utils"
)

var _ = Describe("core adopt → quiesce → restore", Ordered, func() {
	const baseName = "stub"
	var (
		ns      string
		appName string
	)

	BeforeAll(func() {
		ns = withNamespace("ifaas-adopt")
		appName = baseName + "-" + strings.ToLower(rand.String(4))
	})

	It("creates a labelled Deployment that ifaas picks up", func() {
		applyStubDeployment(ns, stubDeploymentOpts{
			name:           appName,
			autopilotLabel: true,
			allowEnv:       "false",
		})
	})

	It("materialises a KSvc with the same name", func() {
		eventuallyKSvcExists(ns, appName, 2*time.Minute)
	})

	It("scales the source Deployment to 0 (quiesced)", func() {
		eventuallyReplicas(ns, appName, 0, 2*time.Minute)
	})

	It("publishes Adopted=True on the KnativeAdoption CR", func() {
		// The CR was auto-created by the DeploymentWatcher; name == sourceRef.name.
		eventuallyAdoptionCondition(ns, appName, "Adopted", "True", 2*time.Minute)
	})

	It("removes the KSvc when the CR is deleted, restoring the Deployment", func() {
		err := clients.Dynamic.Resource(utils.GVRKnativeAdoption).Namespace(ns).
			Delete(ctxBackground(), appName, metav1.DeleteOptions{})
		Expect(err == nil || apierrors.IsNotFound(err)).To(BeTrue(),
			"delete KnativeAdoption should succeed or be a no-op")

		By("KSvc should disappear")
		eventuallyKSvcGone(ns, appName, 2*time.Minute)

		By("Deployment should be scaled back to >=1 replica by the restore chain")
		Eventually(func() int32 {
			dep, err := clients.Typed.AppsV1().Deployments(ns).Get(ctxBackground(), appName, metav1Get())
			if err != nil {
				return -1
			}
			if dep.Spec.Replicas == nil {
				return 1
			}
			return *dep.Spec.Replicas
		}, 2*time.Minute, 2*time.Second).Should(BeNumerically(">=", 1),
			"deployment %s/%s should be restored to >=1 replica after CR delete", ns, appName)
	})
})
