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
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	kservingv1 "knative.dev/serving/pkg/apis/serving/v1"

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

// NewLogOnlyFailureSink returns a flusher.FailureSink that only logs once a
// decision crosses the consecutive-failure threshold.
//
// Rationale (S7 → S10 hand-off):
//   - Writing ConditionDegraded and recording an Event from the flusher
//     would force this module to depend on the v1alpha1 API and on the
//     event recorder, undoing the package boundary.
//   - S10 owns the full Conditions catalogue and metrics emission. The
//     reconciler will surface guard-flush failures there using
//     `status.lastScaleDownProbe.consecutiveErrors`, which the guard
//     already maintains.
//
// Until S10 lands, a log-only sink is enough: production failures still
// show up in operator logs, and tests use a custom sink.
func NewLogOnlyFailureSink() flusher.FailureSink {
	return func(ctx context.Context, d flusher.Decision, attempts int, lastErr error) {
		log := logf.FromContext(ctx).WithName("flusher").
			WithValues("namespace", d.Namespace, "ksvc", d.KSvcName, "adoption", d.AdoptionName)
		log.Error(lastErr, "flusher: consecutive patch failures crossed threshold",
			"attempts", attempts, "desiredMinScale", d.DesiredMinScale, "reason", d.Reason)
	}
}
