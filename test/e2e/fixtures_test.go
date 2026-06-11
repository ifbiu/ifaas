//go:build e2e
// +build e2e

/*
Copyright 2026.
*/

package e2e

import (
	"context"
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/rand"

	"github.com/ifbiu/ifaas/test/utils"
)

// ----- context / metav1 helpers (kept in one place so callers stay terse) ---

func ctxBackground() context.Context { return context.Background() }
func metav1Get() metav1.GetOptions   { return metav1.GetOptions{} }

// ----- fixtures ------------------------------------------------------------

// withNamespace creates an ephemeral namespace for the enclosing Describe
// block, registers cleanup, and returns its name. We rely on Ginkgo's
// per-spec deferred cleanup so a failing spec still tears down what it
// owns — the next run starts clean.
func withNamespace(prefix string) string {
	name := fmt.Sprintf("%s-%s", prefix, strings.ToLower(rand.String(6)))
	By("creating namespace " + name)
	_, err := utils.CreateNamespace(ctxBackground(), clients.Typed, name)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() {
		_ = utils.DeleteNamespace(ctxBackground(), clients.Typed, name, 90*time.Second)
	})
	return name
}

// ----- workload Deployment -------------------------------------------------

type stubDeploymentOpts struct {
	name           string
	replicas       int32
	autopilotLabel bool
	primaryName    string
	allowEnv       string // "true"/"false"; empty leaves the env unset
}

// applyStubDeployment writes a Deployment running the e2e stub image so
// the operator has a real workload to adopt. Container name defaults to
// `app` (matches translator's primary-container default) but can be
// overridden when a spec needs to exercise the multi-container path.
func applyStubDeployment(ns string, o stubDeploymentOpts) *appsv1.Deployment {
	if o.replicas == 0 {
		o.replicas = 1
	}
	if o.primaryName == "" {
		o.primaryName = "app"
	}
	labels := map[string]string{"app": o.name}
	if o.autopilotLabel {
		labels["ifaas.ifbiu.com/knative-autopilot"] = "enabled"
	}
	envs := []corev1.EnvVar{}
	if o.allowEnv != "" {
		envs = append(envs, corev1.EnvVar{Name: "ALLOW_SCALEDOWN", Value: o.allowEnv})
	}
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: o.name, Namespace: ns, Labels: labels},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptrInt32(o.replicas),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": o.name}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": o.name}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:            o.primaryName,
						Image:           e2eImg,
						ImagePullPolicy: corev1.PullIfNotPresent,
						Env:             envs,
						Ports: []corev1.ContainerPort{
							{Name: "http", ContainerPort: 8080},
							{Name: "probe", ContainerPort: 8081},
						},
					}},
				},
			},
		},
	}
	out, err := clients.Typed.AppsV1().Deployments(ns).Create(ctxBackground(), dep, metav1.CreateOptions{})
	Expect(err).NotTo(HaveOccurred(), "create stub deployment")
	return out
}

func ptrInt32(v int32) *int32 { return &v }

// ----- KnativeAdoption CR --------------------------------------------------

type adoptionOpts struct {
	name     string
	source   string
	mode     string // "" → service (default); "eventing" → eventing
	primary  string
	minScale *int32
}

func applyAdoption(ns string, o adoptionOpts) *unstructured.Unstructured {
	spec := map[string]any{
		"sourceRef": map[string]any{"name": o.source},
		"probe": map[string]any{
			"path": "/scaledownz",
			"port": int64(8081),
		},
	}
	if o.mode != "" {
		spec["mode"] = o.mode
	}
	if o.primary != "" {
		spec["primaryContainer"] = o.primary
	}
	if o.minScale != nil {
		spec["minScale"] = int64(*o.minScale)
	}
	cr := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "ifaas.ifbiu.com/v1alpha1",
		"kind":       "KnativeAdoption",
		"metadata":   map[string]any{"name": o.name, "namespace": ns},
		"spec":       spec,
	}}
	out, err := clients.Dynamic.Resource(utils.GVRKnativeAdoption).
		Namespace(ns).Create(ctxBackground(), cr, metav1.CreateOptions{})
	Expect(err).NotTo(HaveOccurred(), "create KnativeAdoption")
	return out
}

// ----- assertion helpers ---------------------------------------------------

func eventuallyReplicas(ns, name string, want int32, within time.Duration) {
	Eventually(func() (int32, error) {
		dep, err := clients.Typed.AppsV1().Deployments(ns).Get(ctxBackground(), name, metav1Get())
		if err != nil {
			return -1, err
		}
		if dep.Spec.Replicas == nil {
			return 1, nil
		}
		return *dep.Spec.Replicas, nil
	}, within, 2*time.Second).Should(Equal(want),
		"deployment %s/%s replicas should converge to %d", ns, name, want)
}

func eventuallyKSvcExists(ns, name string, within time.Duration) *unstructured.Unstructured {
	var out *unstructured.Unstructured
	Eventually(func() error {
		got, err := clients.Dynamic.Resource(utils.GVRService).Namespace(ns).
			Get(ctxBackground(), name, metav1Get())
		if err != nil {
			return err
		}
		out = got
		return nil
	}, within, 2*time.Second).Should(Succeed(),
		"KSvc %s/%s should exist", ns, name)
	return out
}

func eventuallyKSvcGone(ns, name string, within time.Duration) {
	Eventually(func() bool {
		_, err := clients.Dynamic.Resource(utils.GVRService).Namespace(ns).
			Get(ctxBackground(), name, metav1Get())
		return apierrors.IsNotFound(err)
	}, within, 2*time.Second).Should(BeTrue(),
		"KSvc %s/%s should be gone", ns, name)
}

func eventuallyAdoptionCondition(ns, name, condType, status string, within time.Duration) {
	Eventually(func() string {
		cr, err := clients.Dynamic.Resource(utils.GVRKnativeAdoption).Namespace(ns).
			Get(ctxBackground(), name, metav1Get())
		if err != nil {
			return ""
		}
		conds, found, _ := unstructured.NestedSlice(cr.Object, "status", "conditions")
		if !found {
			return ""
		}
		for _, c := range conds {
			m, _ := c.(map[string]any)
			if t, _ := m["type"].(string); t == condType {
				if s, _ := m["status"].(string); s == status {
					return s
				}
			}
		}
		return ""
	}, within, 2*time.Second).Should(Equal(status),
		"adoption %s/%s condition %s should be %s", ns, name, condType, status)
}

// ----- silence unused-import vigilance for specs that pull a subset -------
var _ = schema.GroupVersionResource{}

func intStrFromInt(v int) intstr.IntOrString { return intstr.FromInt(v) }
