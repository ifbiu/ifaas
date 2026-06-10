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

package projector

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ifaasv1alpha1 "github.com/ifbiu/ifaas/api/ifaas/v1alpha1"
)

func dep(name string, ann map[string]string) *appsv1.Deployment {
	return &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: ann}}
}

func TestFromDeployment_DefaultsWhenAnnotationsEmpty(t *testing.T) {
	spec, warns := FromDeployment(dep("hello", nil))
	if len(warns) != 0 {
		t.Errorf("unexpected warnings: %v", warns)
	}
	if spec.SourceRef.Kind != ifaasv1alpha1.SourceKindDeployment || spec.SourceRef.Name != "hello" {
		t.Errorf("sourceRef mismatch: %#v", spec.SourceRef)
	}
	if spec.Mode != ifaasv1alpha1.ModeServing {
		t.Errorf("mode default should be serving, got %s", spec.Mode)
	}
	if spec.PrimaryContainer != "" {
		t.Errorf("primaryContainer should be empty, got %q", spec.PrimaryContainer)
	}
	if spec.Autoscaling.MinScale != nil || spec.Autoscaling.MaxScale != nil || spec.Autoscaling.TargetConcurrency != nil {
		t.Errorf("autoscaling should be empty, got %#v", spec.Autoscaling)
	}
	if spec.Eventing != nil {
		t.Errorf("eventing should be nil, got %#v", spec.Eventing)
	}
}

func TestFromDeployment_HappyPath(t *testing.T) {
	d := dep("hello", map[string]string{
		AnnoMode:                      "serving",
		AnnoPrimaryContainer:          "app",
		AnnoMinScale:                  "0",
		AnnoMaxScale:                  "20",
		AnnoTargetConcurrency:         "50",
		AnnoScaleDownPath:             "/zz",
		AnnoScaleDownPort:             "9000",
		AnnoScaleDownIntervalSeconds:  "15",
		AnnoScaleDownTimeoutSeconds:   "3",
		AnnoScaleDownFailureThreshold: "8",
	})

	spec, warns := FromDeployment(d)
	if len(warns) != 0 {
		t.Fatalf("unexpected warnings: %v", warns)
	}
	if spec.PrimaryContainer != "app" {
		t.Errorf("primaryContainer: got %q", spec.PrimaryContainer)
	}
	if got := *spec.Autoscaling.MinScale; got != 0 {
		t.Errorf("minScale: %d", got)
	}
	if got := *spec.Autoscaling.MaxScale; got != 20 {
		t.Errorf("maxScale: %d", got)
	}
	if got := *spec.Autoscaling.TargetConcurrency; got != 50 {
		t.Errorf("targetConcurrency: %d", got)
	}
	if spec.ScaleDownProbe.Path != "/zz" {
		t.Errorf("path: %s", spec.ScaleDownProbe.Path)
	}
	if got := *spec.ScaleDownProbe.Port; got != 9000 {
		t.Errorf("port: %d", got)
	}
	if spec.ScaleDownProbe.IntervalSeconds != 15 {
		t.Errorf("intervalSeconds: %d", spec.ScaleDownProbe.IntervalSeconds)
	}
	if spec.ScaleDownProbe.TimeoutSeconds != 3 {
		t.Errorf("timeoutSeconds: %d", spec.ScaleDownProbe.TimeoutSeconds)
	}
	if spec.ScaleDownProbe.ConsecutiveFailureThreshold != 8 {
		t.Errorf("threshold: %d", spec.ScaleDownProbe.ConsecutiveFailureThreshold)
	}
}

func TestFromDeployment_EventingMode(t *testing.T) {
	d := dep("hello", map[string]string{
		AnnoMode:           "eventing",
		AnnoEventingBroker: "default",
	})
	spec, warns := FromDeployment(d)
	if len(warns) != 0 {
		t.Errorf("unexpected warnings: %v", warns)
	}
	if spec.Mode != ifaasv1alpha1.ModeEventing {
		t.Errorf("mode: %s", spec.Mode)
	}
	if spec.Eventing == nil || spec.Eventing.Broker != "default" {
		t.Errorf("eventing: %#v", spec.Eventing)
	}
}

func TestFromDeployment_InvalidIntsBecomeWarnings(t *testing.T) {
	d := dep("hello", map[string]string{
		AnnoMinScale:                 "not-a-number",
		AnnoScaleDownIntervalSeconds: "abc",
	})
	spec, warns := FromDeployment(d)
	if spec.Autoscaling.MinScale != nil {
		t.Errorf("minScale should be unset on parse error, got %d", *spec.Autoscaling.MinScale)
	}
	if spec.ScaleDownProbe.IntervalSeconds != 0 {
		t.Errorf("intervalSeconds should be zero on parse error, got %d", spec.ScaleDownProbe.IntervalSeconds)
	}
	if len(warns) != 2 {
		t.Errorf("expected 2 warnings, got %d: %v", len(warns), warns)
	}
}

func TestFromDeployment_EmptyStringAnnotationsAreIgnored(t *testing.T) {
	d := dep("hello", map[string]string{
		AnnoMode:           "",
		AnnoMinScale:       "",
		AnnoEventingBroker: "",
	})
	spec, warns := FromDeployment(d)
	if len(warns) != 0 {
		t.Errorf("unexpected warnings: %v", warns)
	}
	if spec.Mode != ifaasv1alpha1.ModeServing {
		t.Errorf("mode should fall back to serving, got %s", spec.Mode)
	}
	if spec.Autoscaling.MinScale != nil {
		t.Errorf("minScale should remain unset")
	}
	if spec.Eventing != nil {
		t.Errorf("eventing should remain nil")
	}
}
