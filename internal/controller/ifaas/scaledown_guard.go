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
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	ifaasv1alpha1 "github.com/ifbiu/ifaas/api/ifaas/v1alpha1"
	"github.com/ifbiu/ifaas/internal/flusher"
	"github.com/ifbiu/ifaas/internal/metrics"
	"github.com/ifbiu/ifaas/internal/scaledown"
)

// Defaults applied when spec.scaleDownProbe leaves a knob unset. They mirror
// the kubebuilder defaults declared on the CRD so the reconciler can be
// exercised in tests without having to populate every field.
const (
	defaultGuardPath               = "/scaledownz"
	defaultGuardIntervalSeconds    = int32(30)
	defaultGuardTimeoutSeconds     = int32(2)
	defaultGuardFailureThreshold   = int32(20)
	knativePodServiceLabelSelector = "serving.knative.dev/service"
)

// guardConfig resolves the effective probe knobs for a single CR. The defaults
// here are duplicated with the CRD-level defaults so that even hand-crafted
// CRs (or test fixtures that bypass defaulting) get sensible behaviour.
func guardConfig(a *ifaasv1alpha1.KnativeAdoption) (path string, interval, timeout time.Duration, failureThreshold int32) {
	path = a.Spec.ScaleDownProbe.Path
	if path == "" {
		path = defaultGuardPath
	}
	intervalSec := a.Spec.ScaleDownProbe.IntervalSeconds
	if intervalSec <= 0 {
		intervalSec = defaultGuardIntervalSeconds
	}
	timeoutSec := a.Spec.ScaleDownProbe.TimeoutSeconds
	if timeoutSec <= 0 {
		timeoutSec = defaultGuardTimeoutSeconds
	}
	failureThreshold = a.Spec.ScaleDownProbe.ConsecutiveFailureThreshold
	if failureThreshold <= 0 {
		failureThreshold = defaultGuardFailureThreshold
	}
	return path, time.Duration(intervalSec) * time.Second, time.Duration(timeoutSec) * time.Second, failureThreshold
}

// guardEnabled reports whether the /scaledownz guard should run for this CR.
//
// Per Q2 in impl-plan §0 the guard only matters when the user-declared
// baseline is 0; when minScale ≥ 1 there is nothing to gate against because
// the workload is pinned warm regardless.
func guardEnabled(a *ifaasv1alpha1.KnativeAdoption) bool {
	if a.Spec.Autoscaling.MinScale == nil {
		return true
	}
	return *a.Spec.Autoscaling.MinScale == 0
}

// runScaleDownGuard performs one /scaledownz round for the given CR. It
// updates `adoption.Status` in-place — never persists anything itself — so
// the caller's outer status patch can roll the change up with the rest of
// the reconcile pass.
//
// Side-effects on adoption.Status:
//   - LastScaleDownProbe (Time, Result, Message, ConsecutiveErrors)
//   - Conditions[ScaleDownAllowed]
//   - Conditions[Degraded] (only when error counter crosses the threshold)
//
// Returned interval is the requeue duration the caller should honour; the
// reconciler scales requeue intervals globally in `reconcileAdoption`.
func (r *KnativeAdoptionReconciler) runScaleDownGuard(ctx context.Context, adoption *ifaasv1alpha1.KnativeAdoption) (scaledown.Outcome, time.Duration) {
	log := logf.FromContext(ctx).WithValues("adoption", adoption.Name)
	path, interval, timeout, failureThreshold := guardConfig(adoption)

	if r.Prober == nil {
		// No prober means we are running in a unit/envtest configuration that
		// has not opted into the guard. Mirror that as condition=Unknown so
		// the absence is observable instead of silent.
		setCondition(adoption, ifaasv1alpha1.ConditionScaleDownAllowed,
			metav1.ConditionUnknown, ReasonGuardSkipped, "prober not configured")
		return scaledown.OutcomeNoPods, interval
	}

	pods, err := r.listKSvcPods(ctx, adoption.Namespace, adoption.Name)
	if err != nil {
		log.Error(err, "list ksvc pods failed")
		bumpProbeError(adoption, fmt.Sprintf("list pods: %v", err))
		setCondition(adoption, ifaasv1alpha1.ConditionScaleDownAllowed,
			metav1.ConditionUnknown, ReasonScaleDownProbeError, err.Error())
		maybeMarkDegraded(adoption, failureThreshold)
		metrics.ScaleDownProbeErrorsTotal.WithLabelValues(metrics.ProbeErrorReasonListPods).Inc()
		return scaledown.OutcomeBlock, interval
	}

	results := r.probeAll(ctx, adoption, pods, path, timeout)
	outcome := scaledown.Vote(results)
	ok, refused, errored := scaledown.Tally(results)
	msg := fmt.Sprintf("ok=%d refused=%d errored=%d (n=%d)", ok, refused, errored, len(results))

	now := metav1.Now()
	adoption.Status.LastScaleDownProbe = mergeProbeStatus(adoption.Status.LastScaleDownProbe,
		now, outcome, msg, errored > 0)

	switch outcome {
	case scaledown.OutcomeAllowZero:
		prevAllowed := isCondTrue(adoption, ifaasv1alpha1.ConditionScaleDownAllowed)
		setCondition(adoption, ifaasv1alpha1.ConditionScaleDownAllowed,
			metav1.ConditionTrue, ReasonScaleDownAllowed, msg)
		clearConditionIfReason(adoption, ifaasv1alpha1.ConditionDegraded, ReasonScaleDownProbeError)
		r.enqueueMinScale(adoption, 0, "scaledownz=true")
		if !prevAllowed {
			r.emitEvent(adoption, eventTypeNormal, EventReasonScaleDownAllowed, msg)
		}
	case scaledown.OutcomeBlock:
		prevBlocked := condReasonIs(adoption, ifaasv1alpha1.ConditionScaleDownAllowed, ReasonScaleDownBlocked)
		setCondition(adoption, ifaasv1alpha1.ConditionScaleDownAllowed,
			metav1.ConditionFalse, ReasonScaleDownBlocked, msg)
		r.enqueueMinScale(adoption, 1, ReasonScaleDownBlocked)
		metrics.ScaleDownBlockedTotal.WithLabelValues(adoption.Namespace, adoption.Name).Inc()
		if errored > 0 {
			metrics.ScaleDownProbeErrorsTotal.WithLabelValues(metrics.ProbeErrorReasonProberFault).Add(float64(errored))
		}
		if !prevBlocked {
			r.emitEvent(adoption, eventTypeNormal, EventReasonScaleDownBlocked, msg)
		}
	case scaledown.OutcomeNoPods:
		// pods=0 is the steady state we are gating *toward*: the KSvc has
		// already scaled to zero, so there is nothing left to refuse. Treat
		// it as allowed and proactively clear any stale ProbeError /
		// Degraded condition that may linger from before the workload
		// drained, otherwise the CR appears stuck on the previous failure
		// even though the system is healthy.
		prevAllowed := isCondTrue(adoption, ifaasv1alpha1.ConditionScaleDownAllowed)
		setCondition(adoption, ifaasv1alpha1.ConditionScaleDownAllowed,
			metav1.ConditionTrue, ReasonScaleDownAllowed, "ksvc already at zero; no pods to probe")
		clearConditionIfReason(adoption, ifaasv1alpha1.ConditionDegraded, ReasonScaleDownProbeError)
		r.enqueueMinScale(adoption, 0, "no-pods")
		if !prevAllowed {
			r.emitEvent(adoption, eventTypeNormal, EventReasonScaleDownAllowed, "ksvc already at zero")
		}
		log.V(1).Info("no pods to probe; treating as already-at-zero")
	}

	maybeMarkDegraded(adoption, failureThreshold)
	return outcome, interval
}

func (r *KnativeAdoptionReconciler) listKSvcPods(ctx context.Context, ns, ksvcName string) ([]corev1.Pod, error) {
	var list corev1.PodList
	if err := r.List(ctx, &list,
		client.InNamespace(ns),
		client.MatchingLabels{knativePodServiceLabelSelector: ksvcName},
	); err != nil {
		return nil, fmt.Errorf("list ksvc pods: %w", err)
	}
	out := make([]corev1.Pod, 0, len(list.Items))
	for i := range list.Items {
		p := &list.Items[i]
		if p.DeletionTimestamp != nil {
			continue
		}
		if p.Status.Phase != corev1.PodRunning {
			continue
		}
		out = append(out, *p)
	}
	return out, nil
}

// probeAll runs the prober against each pod in parallel. Probes are isolated
// per goroutine so a slow pod cannot delay the round any more than the
// configured timeout.
func (r *KnativeAdoptionReconciler) probeAll(ctx context.Context, a *ifaasv1alpha1.KnativeAdoption, pods []corev1.Pod, path string, timeout time.Duration) []scaledown.Result {
	if len(pods) == 0 {
		return nil
	}
	out := make([]scaledown.Result, len(pods))
	var wg sync.WaitGroup
	wg.Add(len(pods))
	for i := range pods {
		go func(i int) {
			defer wg.Done()
			pod := &pods[i]
			port := resolveProbePort(a, pod)
			start := time.Now()
			out[i] = r.Prober.Probe(ctx, pod.Namespace, pod.Name, port, path, timeout)
			metrics.ScaleDownProbeLatencySeconds.Observe(time.Since(start).Seconds())
		}(i)
	}
	wg.Wait()
	return out
}

// resolveProbePort picks the port the prober will hit. Priority:
//  1. spec.scaleDownProbe.port (explicit override)
//  2. first containerPort of the user-container (named "user-container" by
//     Knative; otherwise pod.Spec.Containers[0])
func resolveProbePort(a *ifaasv1alpha1.KnativeAdoption, pod *corev1.Pod) int32 {
	if a.Spec.ScaleDownProbe.Port != nil && *a.Spec.ScaleDownProbe.Port > 0 {
		return *a.Spec.ScaleDownProbe.Port
	}
	if len(pod.Spec.Containers) == 0 {
		return 0
	}
	c := pod.Spec.Containers[0]
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == "user-container" {
			c = pod.Spec.Containers[i]
			break
		}
	}
	if len(c.Ports) == 0 {
		return 0
	}
	return c.Ports[0].ContainerPort
}

// mergeProbeStatus rolls a fresh round into the running ProbeStatus. The
// ConsecutiveErrors counter is bumped on any errored round and reset to zero
// otherwise; the rest of the fields snapshot the current round verbatim.
func mergeProbeStatus(prev *ifaasv1alpha1.ProbeStatus, now metav1.Time, o scaledown.Outcome, msg string, hadError bool) *ifaasv1alpha1.ProbeStatus {
	out := &ifaasv1alpha1.ProbeStatus{
		Time:    now,
		Message: msg,
	}
	switch o {
	case scaledown.OutcomeAllowZero:
		out.Result = ifaasv1alpha1.ProbeResultTrue
	case scaledown.OutcomeBlock:
		out.Result = ifaasv1alpha1.ProbeResultFalse
	default:
		out.Result = ifaasv1alpha1.ProbeResultUnknown
	}
	if hadError && prev != nil {
		out.ConsecutiveErrors = prev.ConsecutiveErrors + 1
	} else if hadError {
		out.ConsecutiveErrors = 1
	} else {
		out.ConsecutiveErrors = 0
	}
	return out
}

func bumpProbeError(a *ifaasv1alpha1.KnativeAdoption, msg string) {
	prev := a.Status.LastScaleDownProbe
	count := int32(1)
	if prev != nil {
		count = prev.ConsecutiveErrors + 1
	}
	a.Status.LastScaleDownProbe = &ifaasv1alpha1.ProbeStatus{
		Time:              metav1.Now(),
		Result:            ifaasv1alpha1.ProbeResultUnknown,
		Message:           msg,
		ConsecutiveErrors: count,
	}
}

// maybeMarkDegraded raises the Degraded condition once consecutive probe
// errors hit the configured threshold; it does not clear Degraded on its own
// because Degraded is a sticky multi-source signal owned by §S10 and other
// reconcile branches.
func maybeMarkDegraded(a *ifaasv1alpha1.KnativeAdoption, threshold int32) {
	if a.Status.LastScaleDownProbe == nil {
		return
	}
	if a.Status.LastScaleDownProbe.ConsecutiveErrors < threshold {
		return
	}
	setCondition(a, ifaasv1alpha1.ConditionDegraded, metav1.ConditionTrue,
		ReasonScaleDownProbeError,
		fmt.Sprintf("consecutive /scaledownz failures = %d, threshold = %d",
			a.Status.LastScaleDownProbe.ConsecutiveErrors, threshold))
}

// guardRequeueAfter is the requeue interval the caller should honour after a
// successful guard pass.
func guardRequeueAfter(a *ifaasv1alpha1.KnativeAdoption) time.Duration {
	_, interval, _, _ := guardConfig(a)
	return interval
}

// enqueueMinScale hands a min-scale decision off to the namespace flusher.
// If no flusher is wired (unit tests, single-object S6 mode), the helper is
// a no-op and the trailing SSA pass in reconcileAdoption is solely
// responsible for landing the value on the KSvc.
func (r *KnativeAdoptionReconciler) enqueueMinScale(a *ifaasv1alpha1.KnativeAdoption, desired int32, reason string) {
	if r.Flusher == nil {
		return
	}
	_ = r.Flusher.Enqueue(flusher.Decision{
		Namespace:       a.Namespace,
		KSvcName:        a.Name,
		AdoptionName:    a.Name,
		DesiredMinScale: desired,
		Reason:          reason,
	})
}
