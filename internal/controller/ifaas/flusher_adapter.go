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
	"fmt"
	"strconv"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	kservingv1 "knative.dev/serving/pkg/apis/serving/v1"

	ifaasv1alpha1 "github.com/ifbiu/ifaas/api/ifaas/v1alpha1"
	"github.com/ifbiu/ifaas/internal/flusher"
	"github.com/ifbiu/ifaas/internal/translator"
)

// FlusherFieldOwner is the field manager the flusher uses when it PATCHes
// the min-scale annotation. It is intentionally distinct from FieldOwner
// (the SSA owner used by the reconciler for the full KSvc spec) so the
// two writers do not contest the same field path under SSA semantics.
const FlusherFieldOwner = "ifaas-autopilot-guard"

// KSvcMinScalePatcher implements flusher.Patcher against the live apiserver.
//
// The patcher is intentionally narrow: it reads the live KSvc, compares the
// current min-scale annotation against the desired value, and either skips
// (returning skipped=true, nil) or issues a JSON-merge patch scoped to the
// single annotation path under spec.template.metadata.annotations.
//
// Why JSON-merge instead of SSA:
//   - The reconciler's SSA pass already owns the entire spec.template;
//     re-applying via SSA from a different owner would generate a managed-
//     fields conflict on every flush.
//   - JSON-merge with a dedicated FieldManager is the canonical Knative
//     pattern for "narrow, high-frequency annotation updates" and leaves
//     the SSA owner's view of the spec untouched.
//
// On 404 (the KSvc has been deleted between the guard's decision and the
// flush) we return (skipped=true, nil) because the desired state is
// trivially met: there is no KSvc to scale.
type KSvcMinScalePatcher struct {
	Client client.Client
}

// Patch implements flusher.Patcher.
func (p *KSvcMinScalePatcher) Patch(ctx context.Context, d flusher.Decision) (bool, error) {
	live := &kservingv1.Service{}
	if err := p.Client.Get(ctx, types.NamespacedName{Namespace: d.Namespace, Name: d.KSvcName}, live); err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, fmt.Errorf("get ksvc: %w", err)
	}

	desired := strconv.FormatInt(int64(d.DesiredMinScale), 10)
	if cur := live.Spec.Template.GetAnnotations()[translator.AutoscalingMinScaleAnnotation]; cur == desired {
		return true, nil
	}

	patchBytes, err := buildMinScaleMergePatch(translator.AutoscalingMinScaleAnnotation, desired)
	if err != nil {
		return false, err
	}
	if err := p.Client.Patch(ctx, live, client.RawPatch(types.MergePatchType, patchBytes),
		client.FieldOwner(FlusherFieldOwner)); err != nil {
		return false, fmt.Errorf("patch ksvc min-scale: %w", err)
	}
	return false, nil
}

// buildMinScaleMergePatch produces a JSON-merge patch that touches only
// spec.template.metadata.annotations[<key>], leaving every other annotation
// (including ones owned by SSA) untouched.
func buildMinScaleMergePatch(annotationKey, value string) ([]byte, error) {
	payload := map[string]any{
		"spec": map[string]any{
			"template": map[string]any{
				"metadata": map[string]any{
					"annotations": map[string]string{
						annotationKey: value,
					},
				},
			},
		},
	}
	return json.Marshal(payload)
}

// NewFlusherFailureSink returns a flusher.FailureSink that surfaces the
// guard-flush failure on the originating KnativeAdoption:
//
//   - Conditions[Degraded]=True with reason GuardFlushFailed and the
//     consecutive-attempt count + last error in the message.
//   - A Warning Event mirrored on the CR and (best-effort) the source
//     Deployment.
//   - The error is also logged so the failure stays observable when the CR
//     has already been deleted between the flush and the sink.
//
// The sink only fires once a Decision crosses the consecutive-failure
// threshold (configured on flusher.Config.MaxFailures); per-attempt
// failures are accounted for by metrics.GuardFlushTotal{result="failed"}
// via the Observer.
func NewFlusherFailureSink(c client.Client, recorder record.EventRecorder) flusher.FailureSink {
	return func(ctx context.Context, d flusher.Decision, attempts int, lastErr error) {
		log := logf.FromContext(ctx).WithName("flusher").
			WithValues("namespace", d.Namespace, "ksvc", d.KSvcName, "adoption", d.AdoptionName)
		log.Error(lastErr, "flusher: consecutive patch failures crossed threshold",
			"attempts", attempts, "desiredMinScale", d.DesiredMinScale, "reason", d.Reason)

		// Best-effort: write Degraded + Event on the originating CR.
		// A missing CR (release path raced with the sink) is silently
		// dropped because the desired observable end-state has already
		// been reached.
		a := &ifaasv1alpha1.KnativeAdoption{}
		err := c.Get(ctx, types.NamespacedName{Namespace: d.Namespace, Name: d.AdoptionName}, a)
		if apierrors.IsNotFound(err) {
			return
		}
		if err != nil {
			log.Error(err, "flusher sink: get adoption")
			return
		}
		msg := fmt.Sprintf("guard flush failed %d times in a row: %v", attempts, lastErr)
		patch := client.MergeFrom(a.DeepCopy())
		setCondition(a, ifaasv1alpha1.ConditionDegraded, metav1.ConditionTrue, ReasonScaleDownProbeError, msg)
		_ = c.Status().Patch(ctx, a, patch)

		if recorder != nil {
			recorder.Event(a, "Warning", EventReasonGuardFlushFail, msg)
		}
	}
}
