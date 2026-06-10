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

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kservingv1 "knative.dev/serving/pkg/apis/serving/v1"
)

// applyKnativeService submits the translator output via server-side apply.
// The KSvc carries an owner reference to the KnativeAdoption so cascade
// deletion handles the happy-path teardown (S9 wires up snapshot restore).
func applyKnativeService(ctx context.Context, c client.Client, scheme *runtime.Scheme, ksvc *kservingv1.Service) error {
	ksvc.TypeMeta.APIVersion = "serving.knative.dev/v1"
	ksvc.TypeMeta.Kind = "Service"
	if err := c.Patch(ctx, ksvc, client.Apply, client.FieldOwner(FieldOwner), client.ForceOwnership); err != nil {
		return fmt.Errorf("ssa knative service: %w", err)
	}
	return nil
}

// knativeServiceReady returns true when the KSvc has a Ready=True condition.
// The url is returned when populated by Knative regardless of readiness so the
// reconciler can surface it early as a Service is being provisioned.
func knativeServiceReady(ksvc *kservingv1.Service) (ready bool, url string) {
	if ksvc.Status.URL != nil {
		url = ksvc.Status.URL.String()
	}
	for _, c := range ksvc.Status.Conditions {
		if c.Type == kservingv1.ServiceConditionReady {
			ready = c.IsTrue()
			break
		}
	}
	return ready, url
}
