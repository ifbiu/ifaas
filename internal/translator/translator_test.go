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

package translator

import (
	"errors"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ifaasv1alpha1 "github.com/ifbiu/ifaas/api/ifaas/v1alpha1"
)

// --- fixtures -----------------------------------------------------------

func ptrInt32(v int32) *int32 { return &v }
func ptrInt64(v int64) *int64 { return &v }

func basicContainer(name string, port int32) corev1.Container {
	return corev1.Container{
		Name:  name,
		Image: "registry.example/" + name + ":v1",
		Ports: []corev1.ContainerPort{{ContainerPort: port}},
		Env:   []corev1.EnvVar{{Name: "FOO", Value: "bar"}},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
		},
	}
}

func newDeployment(name, ns string, containers []corev1.Container, mutators ...func(*appsv1.Deployment)) *appsv1.Deployment {
	d := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					ServiceAccountName: "default",
					Containers:         containers,
				},
			},
		},
	}
	for _, m := range mutators {
		m(d)
	}
	return d
}

func newAdoption(name, ns string, mutators ...func(*ifaasv1alpha1.KnativeAdoption)) *ifaasv1alpha1.KnativeAdoption {
	a := &ifaasv1alpha1.KnativeAdoption{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: ifaasv1alpha1.KnativeAdoptionSpec{
			SourceRef: ifaasv1alpha1.SourceRef{Kind: ifaasv1alpha1.SourceKindDeployment, Name: name},
			Mode:      ifaasv1alpha1.ModeServing,
		},
	}
	for _, m := range mutators {
		m(a)
	}
	return a
}

// --- happy path ---------------------------------------------------------

func TestTranslate_HappyPath_SingleContainer(t *testing.T) {
	dep := newDeployment("hello", "default", []corev1.Container{basicContainer("app", 8080)})
	a := newAdoption("hello", "default", func(a *ifaasv1alpha1.KnativeAdoption) {
		a.Spec.Autoscaling.MaxScale = ptrInt32(5)
		a.Spec.Autoscaling.TargetConcurrency = ptrInt32(20)
	})

	ksvc, err := Translate(dep, a)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ksvc.Name != "hello" || ksvc.Namespace != "default" {
		t.Fatalf("metadata mismatch: %s/%s", ksvc.Namespace, ksvc.Name)
	}
	if ksvc.Labels[ManagedByLabel] != ManagedByValue {
		t.Errorf("managed-by label missing: %v", ksvc.Labels)
	}
	if ksvc.Labels[OwnerLabel] != "hello" {
		t.Errorf("owner label mismatch: %v", ksvc.Labels)
	}
	containers := ksvc.Spec.Template.Spec.Containers
	if len(containers) != 1 || containers[0].Name != "app" {
		t.Fatalf("unexpected containers: %#v", containers)
	}
	if containers[0].Image != "registry.example/app:v1" {
		t.Errorf("image not preserved: %s", containers[0].Image)
	}
	if got := containers[0].Env; len(got) != 1 || got[0].Name != "FOO" {
		t.Errorf("env not preserved: %#v", got)
	}
	if got := containers[0].Resources.Requests.Cpu().String(); got != "100m" {
		t.Errorf("resources not preserved: cpu=%s", got)
	}
	if got := ksvc.Spec.Template.Annotations[AutoscalingMinScaleAnnotation]; got != "0" {
		t.Errorf("min-scale default not 0, got %q", got)
	}
	if got := ksvc.Spec.Template.Annotations[AutoscalingMaxScaleAnnotation]; got != "5" {
		t.Errorf("max-scale not transcribed: %q", got)
	}
	if got := ksvc.Spec.Template.Annotations[AutoscalingTargetAnnotation]; got != "20" {
		t.Errorf("target not transcribed: %q", got)
	}
}

// --- M1 omits PreStop / TerminationGracePeriodSeconds ---------------------
//
// Stock Knative does not declare these PodSpec fields in its KSvc CRD
// schema; SSA rejects the patch with "field not declared in schema" unless
// the operator at the cluster level enables the corresponding feature
// gates. Translator therefore ships them empty in M1, and the gate-aware
// opt-in surface is left for a follow-up (see plan §12).

func TestTranslate_NoPreStopInjectedByDefault(t *testing.T) {
	dep := newDeployment("hello", "default", []corev1.Container{basicContainer("app", 8080)})
	a := newAdoption("hello", "default")

	ksvc, err := Translate(dep, a)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c := ksvc.Spec.Template.Spec.Containers[0]; c.Lifecycle != nil {
		t.Fatalf("M1 must not write Lifecycle: %#v", c.Lifecycle)
	}
}

func TestTranslate_PreservesUserAuthoredLifecycle(t *testing.T) {
	cs := basicContainer("app", 8080)
	cs.Lifecycle = &corev1.Lifecycle{
		PostStart: &corev1.LifecycleHandler{Exec: &corev1.ExecAction{Command: []string{"echo"}}},
		PreStop:   &corev1.LifecycleHandler{Exec: &corev1.ExecAction{Command: []string{"old"}}},
	}
	dep := newDeployment("hello", "default", []corev1.Container{cs})
	a := newAdoption("hello", "default")

	ksvc, err := Translate(dep, a)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c := ksvc.Spec.Template.Spec.Containers[0]
	if c.Lifecycle == nil || c.Lifecycle.PostStart == nil || c.Lifecycle.PostStart.Exec == nil {
		t.Errorf("user PostStart should be preserved verbatim: %#v", c.Lifecycle)
	}
	if c.Lifecycle.PreStop == nil || c.Lifecycle.PreStop.Exec == nil {
		t.Errorf("user PreStop should be preserved verbatim: %#v", c.Lifecycle.PreStop)
	}
}

func TestTranslate_NoTerminationGracePeriodSet(t *testing.T) {
	dep := newDeployment("hello", "default", []corev1.Container{basicContainer("app", 8080)}, func(d *appsv1.Deployment) {
		d.Spec.Template.Spec.TerminationGracePeriodSeconds = ptrInt64(120)
	})
	a := newAdoption("hello", "default")

	ksvc, err := Translate(dep, a)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := ksvc.Spec.Template.Spec.TerminationGracePeriodSeconds; got != nil {
		t.Fatalf("M1 must leave TerminationGracePeriodSeconds unset, got %d", *got)
	}
}

// --- container selection -----------------------------------------------

func TestTranslate_MultiContainer(t *testing.T) {
	tests := []struct {
		name     string
		primary  string
		wantErr  error
		wantPick string
	}{
		{"no primary spec -> ambiguous", "", ErrAmbiguousPrimary, ""},
		{"primary not found", "ghost", ErrPrimaryNotFound, ""},
		{"primary picks side car", "worker", nil, "worker"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dep := newDeployment("hello", "default", []corev1.Container{
				basicContainer("app", 8080),
				basicContainer("worker", 9090),
			})
			a := newAdoption("hello", "default", func(a *ifaasv1alpha1.KnativeAdoption) {
				a.Spec.PrimaryContainer = tt.primary
			})
			ksvc, err := Translate(dep, a)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("want err %v, got %v", tt.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ksvc.Spec.Template.Spec.Containers[0].Name != tt.wantPick {
				t.Errorf("picked container want=%s got=%s", tt.wantPick, ksvc.Spec.Template.Spec.Containers[0].Name)
			}
		})
	}
}

func TestTranslate_NoContainer(t *testing.T) {
	dep := newDeployment("hello", "default", nil)
	a := newAdoption("hello", "default")
	if _, err := Translate(dep, a); !errors.Is(err, ErrNoContainer) {
		t.Fatalf("want %v, got %v", ErrNoContainer, err)
	}
}

// --- pod-level rejections ----------------------------------------------

func TestTranslate_HostFieldsRejected(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*appsv1.Deployment)
		wantErr error
	}{
		{"hostNetwork", func(d *appsv1.Deployment) { d.Spec.Template.Spec.HostNetwork = true }, ErrHostNetwork},
		{"hostPID", func(d *appsv1.Deployment) { d.Spec.Template.Spec.HostPID = true }, ErrHostPID},
		{"hostIPC", func(d *appsv1.Deployment) { d.Spec.Template.Spec.HostIPC = true }, ErrHostIPC},
		{"hostPort", func(d *appsv1.Deployment) { d.Spec.Template.Spec.Containers[0].Ports[0].HostPort = 30080 }, ErrHostPort},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dep := newDeployment("hello", "default", []corev1.Container{basicContainer("app", 8080)}, tt.mutate)
			a := newAdoption("hello", "default")
			_, err := Translate(dep, a)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("want err %v, got %v", tt.wantErr, err)
			}
		})
	}
}

func TestTranslate_PortRejection(t *testing.T) {
	tests := []struct {
		name    string
		ports   []corev1.ContainerPort
		wantErr error
	}{
		{"no ports", nil, ErrNoContainerPort},
		{"udp", []corev1.ContainerPort{{ContainerPort: 8080, Protocol: corev1.ProtocolUDP}}, ErrUnsupportedProtocol},
		{"sctp", []corev1.ContainerPort{{ContainerPort: 8080, Protocol: corev1.ProtocolSCTP}}, ErrUnsupportedProtocol},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := basicContainer("app", 8080)
			c.Ports = tt.ports
			dep := newDeployment("hello", "default", []corev1.Container{c})
			a := newAdoption("hello", "default")
			_, err := Translate(dep, a)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("want %v, got %v", tt.wantErr, err)
			}
		})
	}
}

// Knative serving validates the revision template's container ports
// against a closed name set ("", "h2c", "http1") and rejects arbitrary
// user names like "http". Translator must scrub Name rather than
// faithfully copying user input — otherwise the KSvc never lands.
// HostPort is rejected upstream by validatePodSpec, so it never reaches
// the sanitiser; HostIP is silently cleared as a defensive nicety.
func TestTranslate_PortSanitisation(t *testing.T) {
	c := basicContainer("app", 8080)
	c.Ports = []corev1.ContainerPort{{
		Name:          "http",
		ContainerPort: 8080,
		HostIP:        "127.0.0.1",
		Protocol:      corev1.ProtocolTCP,
	}}
	dep := newDeployment("hello", "default", []corev1.Container{c})
	a := newAdoption("hello", "default")

	ksvc, err := Translate(dep, a)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := ksvc.Spec.Template.Spec.Containers[0].Ports
	if len(got) != 1 {
		t.Fatalf("want 1 port, got %d", len(got))
	}
	if got[0].Name != "" {
		t.Fatalf("port name must be cleared, got %q", got[0].Name)
	}
	if got[0].HostIP != "" {
		t.Fatalf("HostIP must be cleared, got %q", got[0].HostIP)
	}
	if got[0].ContainerPort != 8080 {
		t.Fatalf("ContainerPort must survive, got %d", got[0].ContainerPort)
	}
}

// --- volumes / passthrough ---------------------------------------------

func TestTranslate_VolumesPassthrough(t *testing.T) {
	dep := newDeployment("hello", "default", []corev1.Container{basicContainer("app", 8080)}, func(d *appsv1.Deployment) {
		d.Spec.Template.Spec.Volumes = []corev1.Volume{
			{Name: "cm", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "app-cfg"}}}},
			{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data-pvc"}}},
		}
		d.Spec.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{
			{Name: "cm", MountPath: "/etc/app"},
			{Name: "data", MountPath: "/var/data"},
		}
	})
	a := newAdoption("hello", "default")

	ksvc, err := Translate(dep, a)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	vols := ksvc.Spec.Template.Spec.Volumes
	if len(vols) != 2 || vols[0].Name != "cm" || vols[1].Name != "data" {
		t.Fatalf("volumes not passthrough: %#v", vols)
	}
	if vols[1].PersistentVolumeClaim == nil || vols[1].PersistentVolumeClaim.ClaimName != "data-pvc" {
		t.Errorf("PVC volume lost: %#v", vols[1])
	}
	mounts := ksvc.Spec.Template.Spec.Containers[0].VolumeMounts
	if len(mounts) != 2 {
		t.Errorf("volume mounts dropped: %#v", mounts)
	}
}

func TestTranslate_PodLevelPassthrough(t *testing.T) {
	dep := newDeployment("hello", "default", []corev1.Container{basicContainer("app", 8080)}, func(d *appsv1.Deployment) {
		d.Spec.Template.Spec.ServiceAccountName = "team-a"
		d.Spec.Template.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "regcred"}}
		d.Spec.Template.Spec.NodeSelector = map[string]string{"zone": "a"}
		d.Spec.Template.Spec.Tolerations = []corev1.Toleration{{Key: "spot", Operator: corev1.TolerationOpExists}}
	})
	a := newAdoption("hello", "default")

	ksvc, err := Translate(dep, a)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ps := ksvc.Spec.Template.Spec
	if ps.ServiceAccountName != "team-a" {
		t.Errorf("serviceAccount not passthrough: %s", ps.ServiceAccountName)
	}
	if len(ps.ImagePullSecrets) != 1 || ps.ImagePullSecrets[0].Name != "regcred" {
		t.Errorf("imagePullSecrets not passthrough: %#v", ps.ImagePullSecrets)
	}
	if ps.NodeSelector["zone"] != "a" {
		t.Errorf("nodeSelector not passthrough: %#v", ps.NodeSelector)
	}
	if len(ps.Tolerations) != 1 || ps.Tolerations[0].Key != "spot" {
		t.Errorf("tolerations not passthrough: %#v", ps.Tolerations)
	}
}

// --- effective min-scale (S6 contract) ---------------------------------

func TestEffectiveMinScale(t *testing.T) {
	probeFalse := &ifaasv1alpha1.ProbeStatus{Result: ifaasv1alpha1.ProbeResultFalse}
	probeTrue := &ifaasv1alpha1.ProbeStatus{Result: ifaasv1alpha1.ProbeResultTrue}
	probeUnknown := &ifaasv1alpha1.ProbeStatus{Result: ifaasv1alpha1.ProbeResultUnknown}

	tests := []struct {
		name  string
		spec  *int32
		probe *ifaasv1alpha1.ProbeStatus
		want  int32
	}{
		{"spec nil, no probe -> 0", nil, nil, 0},
		{"spec 0, no probe -> 0", ptrInt32(0), nil, 0},
		{"spec 0, probe true -> 0", ptrInt32(0), probeTrue, 0},
		{"spec 0, probe unknown -> 0", ptrInt32(0), probeUnknown, 0},
		{"spec 0, probe false -> 1 (guard pin)", ptrInt32(0), probeFalse, 1},
		{"spec 2, probe false -> 2 (spec wins above 0)", ptrInt32(2), probeFalse, 2},
		{"spec 3, probe true -> 3", ptrInt32(3), probeTrue, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := newAdoption("hello", "default", func(a *ifaasv1alpha1.KnativeAdoption) {
				a.Spec.Autoscaling.MinScale = tt.spec
				a.Status.LastScaleDownProbe = tt.probe
			})
			if got := EffectiveMinScale(a); got != tt.want {
				t.Errorf("EffectiveMinScale = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestTranslate_MinScaleFollowsProbe(t *testing.T) {
	dep := newDeployment("hello", "default", []corev1.Container{basicContainer("app", 8080)})
	a := newAdoption("hello", "default", func(a *ifaasv1alpha1.KnativeAdoption) {
		a.Status.LastScaleDownProbe = &ifaasv1alpha1.ProbeStatus{Result: ifaasv1alpha1.ProbeResultFalse}
	})
	ksvc, err := Translate(dep, a)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := ksvc.Spec.Template.Annotations[AutoscalingMinScaleAnnotation]; got != "1" {
		t.Errorf("guard-pinned min-scale should be 1, got %q", got)
	}
}
