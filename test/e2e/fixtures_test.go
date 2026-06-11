//go:build e2e
// +build e2e

/*
Copyright 2026.
*/

package e2e

import (
	"context"
	"fmt"
	"io"
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
//
// On spec failure the cleanup hook also dumps a focused diagnostic of
// the namespace (KSvc/Revision/Configuration/Route, KnativeAdoption,
// Pod, Events) to GinkgoWriter so the next investigator does not need
// to chase a 180-second window with a parallel terminal.
func withNamespace(prefix string) string {
	name := fmt.Sprintf("%s-%s", prefix, strings.ToLower(rand.String(6)))
	By("creating namespace " + name)
	_, err := utils.CreateNamespace(ctxBackground(), clients.Typed, name)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() {
		if r := CurrentSpecReport(); r.Failed() {
			dumpDiagnostics(name)
		}
		_ = utils.DeleteNamespace(ctxBackground(), clients.Typed, name, 90*time.Second)
	})
	return name
}

// ----- failure diagnostics -------------------------------------------------

// dumpDiagnostics writes everything we'd want to know about why a spec
// stalled in `ns` to GinkgoWriter. It is deliberately defensive: any
// individual lookup may fail (CRD missing, API not registered, ns
// already gone) without aborting the rest of the dump.
func dumpDiagnostics(ns string) {
	w := GinkgoWriter
	fmt.Fprintf(w, "\n========== diagnostics for ns/%s ==========\n", ns)
	defer fmt.Fprintf(w, "========== end diagnostics ns/%s ==========\n\n", ns)

	dumpUnstructured(w, "KnativeAdoption", utils.GVRKnativeAdoption, ns)
	dumpAdoptionMeta(w, ns)
	dumpUnstructured(w, "KSvc", utils.GVRService, ns)
	dumpUnstructured(w, "Revision", utils.GVRRevision, ns)
	dumpUnstructured(w, "Configuration", schema.GroupVersionResource{
		Group: "serving.knative.dev", Version: "v1", Resource: "configurations"}, ns)
	dumpUnstructured(w, "Route", schema.GroupVersionResource{
		Group: "serving.knative.dev", Version: "v1", Resource: "routes"}, ns)

	if deps, err := clients.Typed.AppsV1().Deployments(ns).List(ctxBackground(), metav1.ListOptions{}); err == nil {
		for _, d := range deps.Items {
			fmt.Fprintf(w, "Deployment %s: replicas spec=%v status=%d available=%d\n",
				d.Name, d.Spec.Replicas, d.Status.Replicas, d.Status.AvailableReplicas)
		}
	}

	if pods, err := clients.Typed.CoreV1().Pods(ns).List(ctxBackground(), metav1.ListOptions{}); err == nil {
		for _, p := range pods.Items {
			reason := "-"
			if len(p.Status.ContainerStatuses) > 0 {
				if s := p.Status.ContainerStatuses[0].State.Waiting; s != nil {
					reason = s.Reason + ": " + s.Message
				}
			}
			fmt.Fprintf(w, "Pod %s phase=%s reason=%q\n", p.Name, p.Status.Phase, reason)
		}
	}

	if evs, err := clients.Typed.CoreV1().Events(ns).List(ctxBackground(),
		metav1.ListOptions{Limit: 25}); err == nil {
		for _, e := range evs.Items {
			fmt.Fprintf(w, "Event %s/%s [%s] %s: %s\n",
				e.InvolvedObject.Kind, e.InvolvedObject.Name, e.Type, e.Reason, e.Message)
		}
	}

	dumpControllerLogs(w)
}

// dumpAdoptionMeta surfaces the deletion-path metadata that conditions
// alone do not show: deletionTimestamp, finalizers, ownerReferences. A
// CR sitting at "Ready=True/Adopted" right after Delete usually means
// either the finalizer never fired or status writes were rolled back —
// these three fields decide which.
func dumpAdoptionMeta(w io.Writer, ns string) {
	list, err := clients.Dynamic.Resource(utils.GVRKnativeAdoption).Namespace(ns).
		List(ctxBackground(), metav1.ListOptions{})
	if err != nil {
		fmt.Fprintf(w, "KnativeAdoption meta list error: %v\n", err)
		return
	}
	for _, it := range list.Items {
		dt, _, _ := unstructured.NestedString(it.Object, "metadata", "deletionTimestamp")
		fins, _, _ := unstructured.NestedStringSlice(it.Object, "metadata", "finalizers")
		labels, _, _ := unstructured.NestedStringMap(it.Object, "metadata", "labels")
		fmt.Fprintf(w, "KnativeAdoption %s meta: deletionTimestamp=%q finalizers=%v labels=%v\n",
			it.GetName(), dt, fins, labels)
	}
}

// dumpControllerLogs pulls the last ~200 lines from every
// ifaas-controller-manager pod. The deletion-path bug surface is
// almost always visible there (handleDeletion error, status patch
// failure, restore webhook denial) and chasing it without these lines
// means re-running the suite blind.
func dumpControllerLogs(w io.Writer) {
	const sysNS = "ifaas-system"
	pods, err := clients.Typed.CoreV1().Pods(sysNS).List(ctxBackground(), metav1.ListOptions{
		LabelSelector: "control-plane=controller-manager",
	})
	if err != nil {
		fmt.Fprintf(w, "controller-manager pod list error: %v\n", err)
		return
	}
	tail := int64(200)
	for _, p := range pods.Items {
		fmt.Fprintf(w, "----- controller-manager logs (%s) -----\n", p.Name)
		req := clients.Typed.CoreV1().Pods(sysNS).GetLogs(p.Name, &corev1.PodLogOptions{
			Container: "manager",
			TailLines: &tail,
		})
		stream, err := req.Stream(ctxBackground())
		if err != nil {
			fmt.Fprintf(w, "  log stream error: %v\n", err)
			continue
		}
		_, _ = io.Copy(w, stream)
		_ = stream.Close()
		fmt.Fprintf(w, "----- end controller-manager logs (%s) -----\n", p.Name)
	}
}

// dumpUnstructured prints a one-line status summary for every object
// of `gvr` in `ns`. We only render conditions because that is the
// only field the operator actually waits on.
func dumpUnstructured(w io.Writer, kind string, gvr schema.GroupVersionResource, ns string) {
	list, err := clients.Dynamic.Resource(gvr).Namespace(ns).List(ctxBackground(), metav1.ListOptions{})
	if err != nil {
		fmt.Fprintf(w, "%s: list error: %v\n", kind, err)
		return
	}
	for _, it := range list.Items {
		conds, _, _ := unstructured.NestedSlice(it.Object, "status", "conditions")
		var parts []string
		for _, c := range conds {
			m, _ := c.(map[string]any)
			t, _ := m["type"].(string)
			s, _ := m["status"].(string)
			r, _ := m["reason"].(string)
			msg, _ := m["message"].(string)
			line := fmt.Sprintf("%s=%s", t, s)
			if r != "" {
				line += "/" + r
			}
			if msg != "" {
				if len(msg) > 120 {
					msg = msg[:120] + "…"
				}
				line += "(" + msg + ")"
			}
			parts = append(parts, line)
		}
		fmt.Fprintf(w, "%s %s: %s\n", kind, it.GetName(), strings.Join(parts, "; "))
	}
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
