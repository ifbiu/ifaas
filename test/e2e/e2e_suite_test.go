//go:build e2e
// +build e2e

/*
Copyright 2026.
Licensed under the Apache License, Version 2.0 (the "License");
*/

package e2e

import (
	"fmt"
	"os"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ifbiu/ifaas/test/utils"
)

// Suite-level state shared by every spec in the package. We intentionally
// keep this as plain package vars (rather than a fixture struct) so the
// Ginkgo trees can reach it without ceremony — the suite is single-run.
var (
	clients *utils.Clients

	// e2eImg is the workload image baked by the stub Dockerfile. It must
	// already be present in the cluster's container runtime; the Makefile
	// target `setup-test-e2e` is responsible for that.
	e2eImg string
)

// TestE2E is the entry point for `go test -tags=e2e ./test/e2e/...`.
//
// Cluster lifecycle, ifaas deployment and image loading are all driven by
// the Makefile (`setup-test-e2e`). This suite assumes the cluster is
// already serving Knative Serving + Eventing CRDs and that the ifaas
// controller-manager is running in `ifaas-system`. We deliberately do
// nothing destructive at the cluster scope — every spec works inside a
// disposable namespace it owns.
func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	_, _ = fmt.Fprintln(GinkgoWriter, "Starting ifaas e2e test suite")
	RunSpecs(t, "ifaas e2e suite")
}

var _ = BeforeSuite(func() {
	By("loading kubeconfig and constructing API clients")
	c, err := utils.NewClients()
	Expect(err).NotTo(HaveOccurred(), "kubeconfig must point at the e2e cluster")
	clients = c

	e2eImg = os.Getenv("E2E_STUB_IMG")
	if e2eImg == "" {
		e2eImg = "ifaas-scaledownz-stub:e2e"
	}

	By("verifying the ifaas controller-manager is Available")
	expectIfaasReady()
})

func expectIfaasReady() {
	dep, err := clients.Typed.AppsV1().Deployments("ifaas-system").
		Get(ctxBackground(), "ifaas-controller-manager", metav1Get())
	Expect(err).NotTo(HaveOccurred(), "ifaas controller-manager must already be deployed")
	Expect(dep.Status.AvailableReplicas).To(BeNumerically(">=", 1),
		"ifaas controller-manager has no Available replicas; run `make setup-test-e2e` first")
}
