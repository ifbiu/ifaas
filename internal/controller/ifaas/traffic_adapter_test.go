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
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ifaasv1alpha1 "github.com/ifbiu/ifaas/api/ifaas/v1alpha1"
)

func TestTrafficEnabled(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   *ifaasv1alpha1.KnativeAdoption
		want bool
	}{
		{name: "nil adoption", in: nil, want: false},
		{name: "nil traffic", in: &ifaasv1alpha1.KnativeAdoption{}, want: false},
		{
			name: "traffic disabled",
			in: &ifaasv1alpha1.KnativeAdoption{
				Spec: ifaasv1alpha1.KnativeAdoptionSpec{
					Traffic: &ifaasv1alpha1.Traffic{Enabled: false},
				},
			},
			want: false,
		},
		{
			name: "traffic enabled",
			in: &ifaasv1alpha1.KnativeAdoption{
				Spec: ifaasv1alpha1.KnativeAdoptionSpec{
					Traffic: &ifaasv1alpha1.Traffic{Enabled: true},
				},
			},
			want: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := trafficEnabled(tc.in); got != tc.want {
				t.Fatalf("trafficEnabled() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestReconcileTrafficNoopWhenTrafficDisabled(t *testing.T) {
	t.Parallel()

	rec := &KnativeAdoptionReconciler{}
	cases := []struct {
		name     string
		adoption *ifaasv1alpha1.KnativeAdoption
	}{
		{
			name:     "nil traffic",
			adoption: &ifaasv1alpha1.KnativeAdoption{},
		},
		{
			name: "traffic disabled",
			adoption: &ifaasv1alpha1.KnativeAdoption{
				Spec: ifaasv1alpha1.KnativeAdoptionSpec{
					Traffic: &ifaasv1alpha1.Traffic{
						Enabled: false,
						Mode:    ifaasv1alpha1.TrafficModeInplace,
					},
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := tc.adoption.DeepCopy()

			err := rec.reconcileTraffic(context.Background(), tc.adoption)
			if err != nil {
				t.Fatalf("reconcileTraffic() error = %v", err)
			}
			if !reflect.DeepEqual(before.Spec, tc.adoption.Spec) {
				t.Fatalf("reconcileTraffic() mutated spec: before=%+v after=%+v", before.Spec, tc.adoption.Spec)
			}
			if !reflect.DeepEqual(before.Status, tc.adoption.Status) {
				t.Fatalf("reconcileTraffic() mutated status: before=%+v after=%+v", before.Status, tc.adoption.Status)
			}
		})
	}
}

func TestResolveTrafficTargetServicePortPrefersLiveServicePort(t *testing.T) {
	t.Parallel()

	ns := "platform-a"
	specPort := int32(5555)
	livePort := int32(80)

	rec := newTrafficTestReconciler(t, makeServiceWithPort("gops-ckbackup", ns, livePort))
	adoption := makeTrafficAdoption("gops-ckbackup", ns, &specPort, []string{"managed-vs"})

	got, err := rec.resolveTrafficTargetServicePort(context.Background(), adoption)
	if err != nil {
		t.Fatalf("resolveTrafficTargetServicePort() error = %v", err)
	}
	if got == nil {
		t.Fatalf("resolveTrafficTargetServicePort() returned nil, want %d", livePort)
	}
	if *got != livePort {
		t.Fatalf("resolveTrafficTargetServicePort() = %d, want %d", *got, livePort)
	}
}

func TestResolveTrafficTargetServicePortFallsBackToSpecPortWhenServiceMissing(t *testing.T) {
	t.Parallel()

	ns := "platform-a"
	specPort := int32(5555)

	rec := newTrafficTestReconciler(t)
	adoption := makeTrafficAdoption("gops-ckbackup", ns, &specPort, []string{"managed-vs"})

	got, err := rec.resolveTrafficTargetServicePort(context.Background(), adoption)
	if err != nil {
		t.Fatalf("resolveTrafficTargetServicePort() error = %v", err)
	}
	if got == nil {
		t.Fatalf("resolveTrafficTargetServicePort() returned nil, want %d", specPort)
	}
	if *got != specPort {
		t.Fatalf("resolveTrafficTargetServicePort() = %d, want %d", *got, specPort)
	}
}

func TestReconcileTrafficVirtualServiceRewritesDestinationPortToLiveService(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ns := "platform-a"
	specPort := int32(5555)
	livePort := int32(80)

	managed := makeVirtualService(
		"managed-vs",
		ns,
		[]interface{}{
			map[string]interface{}{
				"route": []interface{}{
					makeRoute("gops-ckbackup.platform-a.svc.cluster.local", 5555, "stable"),
					makeRoute("gops-ckbackup.platform-a.svc.cluster.local", 6666, "keep-mismatch"),
					makeRoute("legacy.platform-a.svc.cluster.local", 5555, "legacy"),
				},
			},
		},
	)

	rec := newTrafficTestReconciler(t, makeServiceWithPort("gops-ckbackup", ns, livePort), managed)
	adoption := makeTrafficAdoption("gops-ckbackup", ns, &specPort, []string{"managed-vs"})

	if err := rec.reconcileTraffic(ctx, adoption); err != nil {
		t.Fatalf("reconcileTraffic() error = %v", err)
	}

	gotManaged := getVirtualService(t, rec.Client, ns, "managed-vs")
	if hasSubset(gotManaged, 0, 0) {
		t.Fatalf("managed VS target destination subset should be removed")
	}
	if port, ok := routePortNumber(gotManaged, 0, 0); !ok || port != livePort {
		t.Fatalf("managed VS target destination port = %d (ok=%v), want %d", port, ok, livePort)
	}

	if !hasSubset(gotManaged, 0, 1) {
		t.Fatalf("managed VS target host with non-managed port subset should stay")
	}
	if port, ok := routePortNumber(gotManaged, 0, 1); !ok || port != 6666 {
		t.Fatalf("managed VS non-managed port route = %d (ok=%v), want 6666", port, ok)
	}

	if !hasSubset(gotManaged, 0, 2) {
		t.Fatalf("managed VS non-target host subset should stay")
	}
	if port, ok := routePortNumber(gotManaged, 0, 2); !ok || port != 5555 {
		t.Fatalf("managed VS non-target host route port = %d (ok=%v), want 5555", port, ok)
	}
}

func TestReconcileTrafficVirtualServiceSubsetPruneAndScope(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ns := "platform-a"
	servicePort := int32(5555)

	managed := makeVirtualService(
		"managed-vs",
		ns,
		[]interface{}{
			map[string]interface{}{
				"route": []interface{}{
					makeRoute("gops-ckbackup.platform-a.svc.cluster.local", 5555, "stable"),
					makeRoute("legacy.platform-a.svc.cluster.local", 5555, "legacy"),
					makeRoute("gops-ckbackup.platform-a.svc.cluster.local", 6666, "port-mismatch"),
				},
			},
		},
	)
	unmanaged := makeVirtualService(
		"unmanaged-vs",
		ns,
		[]interface{}{
			map[string]interface{}{
				"route": []interface{}{
					makeRoute("gops-ckbackup.platform-a.svc.cluster.local", 5555, "keep-unmanaged"),
				},
			},
		},
	)
	unmanagedSpecBefore := runtime.DeepCopyJSONValue(unmanaged.Object["spec"])

	rec := newTrafficTestReconciler(t, managed, unmanaged)
	adoption := makeTrafficAdoption("gops-ckbackup", ns, &servicePort, []string{"managed-vs"})

	if err := rec.reconcileTraffic(ctx, adoption); err != nil {
		t.Fatalf("reconcileTraffic() error = %v", err)
	}

	gotManaged := getVirtualService(t, rec.Client, ns, "managed-vs")
	if hasSubset(gotManaged, 0, 0) {
		t.Fatalf("managed VS target destination subset should be removed")
	}
	if !hasSubset(gotManaged, 0, 1) {
		t.Fatalf("managed VS non-target destination subset should stay")
	}
	if !hasSubset(gotManaged, 0, 2) {
		t.Fatalf("managed VS target host with non-matching port should stay")
	}
	assertHostsGatewaysPreserved(t, gotManaged)

	gotUnmanaged := getVirtualService(t, rec.Client, ns, "unmanaged-vs")
	if !reflect.DeepEqual(gotUnmanaged.Object["spec"], unmanagedSpecBefore) {
		t.Fatalf("unmanaged VS spec changed: before=%v after=%v", unmanagedSpecBefore, gotUnmanaged.Object["spec"])
	}
}

func TestReconcileTrafficVirtualServiceIdempotent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ns := "platform-a"
	servicePort := int32(5555)

	managed := makeVirtualService(
		"managed-vs",
		ns,
		[]interface{}{
			map[string]interface{}{
				"route": []interface{}{
					makeRoute("gops-ckbackup.platform-a.svc.cluster.local", 5555, "stable"),
				},
			},
		},
	)

	rec := newTrafficTestReconciler(t, managed)
	adoption := makeTrafficAdoption("gops-ckbackup", ns, &servicePort, []string{"managed-vs"})

	if err := rec.reconcileTraffic(ctx, adoption); err != nil {
		t.Fatalf("first reconcileTraffic() error = %v", err)
	}
	first := getVirtualService(t, rec.Client, ns, "managed-vs")
	if hasSubset(first, 0, 0) {
		t.Fatalf("first reconcile should remove target subset")
	}
	firstSpec := runtime.DeepCopyJSONValue(first.Object["spec"])

	if err := rec.reconcileTraffic(ctx, adoption); err != nil {
		t.Fatalf("second reconcileTraffic() error = %v", err)
	}
	second := getVirtualService(t, rec.Client, ns, "managed-vs")
	if !reflect.DeepEqual(second.Object["spec"], firstSpec) {
		t.Fatalf("second reconcile changed spec: first=%v second=%v", firstSpec, second.Object["spec"])
	}
}

func TestReconcileTrafficDestinationRuleSubsetConvergeAndScope(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ns := "platform-a"

	managed := makeDestinationRule(
		"managed-dr",
		ns,
		"gops-ckbackup.platform-a.svc.cluster.local",
		[]interface{}{
			makeSubset("stable"),
			makeSubset("canary"),
		},
	)
	managedForeignHost := makeDestinationRule(
		"managed-foreign-dr",
		ns,
		"legacy.platform-a.svc.cluster.local",
		[]interface{}{makeSubset("legacy")},
	)
	unmanaged := makeDestinationRule(
		"unmanaged-dr",
		ns,
		"gops-ckbackup.platform-a.svc.cluster.local",
		[]interface{}{makeSubset("keep-unmanaged")},
	)
	foreignBefore := runtime.DeepCopyJSONValue(managedForeignHost.Object["spec"])
	unmanagedBefore := runtime.DeepCopyJSONValue(unmanaged.Object["spec"])

	rec := newTrafficTestReconciler(t, managed, managedForeignHost, unmanaged)
	adoption := makeTrafficAdoptionWithIstioRefs(
		"gops-ckbackup",
		ns,
		nil,
		nil,
		[]string{"managed-dr", "managed-foreign-dr"},
	)

	if err := rec.reconcileTraffic(ctx, adoption); err != nil {
		t.Fatalf("reconcileTraffic() error = %v", err)
	}

	gotManaged := getDestinationRule(t, rec.Client, ns, "managed-dr")
	if destinationRuleHasSubsets(t, gotManaged) {
		t.Fatalf("managed DR subsets should be removed for target service")
	}

	gotForeign := getDestinationRule(t, rec.Client, ns, "managed-foreign-dr")
	if !reflect.DeepEqual(gotForeign.Object["spec"], foreignBefore) {
		t.Fatalf("managed non-target DR spec changed: before=%v after=%v", foreignBefore, gotForeign.Object["spec"])
	}

	gotUnmanaged := getDestinationRule(t, rec.Client, ns, "unmanaged-dr")
	if !reflect.DeepEqual(gotUnmanaged.Object["spec"], unmanagedBefore) {
		t.Fatalf("unmanaged DR spec changed: before=%v after=%v", unmanagedBefore, gotUnmanaged.Object["spec"])
	}
}

func TestReconcileTrafficDestinationRuleIdempotent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ns := "platform-a"

	managed := makeDestinationRule(
		"managed-dr",
		ns,
		"gops-ckbackup.platform-a.svc.cluster.local",
		[]interface{}{
			makeSubset("stable"),
			makeSubset("canary"),
		},
	)

	rec := newTrafficTestReconciler(t, managed)
	adoption := makeTrafficAdoptionWithIstioRefs(
		"gops-ckbackup",
		ns,
		nil,
		nil,
		[]string{"managed-dr"},
	)

	if err := rec.reconcileTraffic(ctx, adoption); err != nil {
		t.Fatalf("first reconcileTraffic() error = %v", err)
	}
	first := getDestinationRule(t, rec.Client, ns, "managed-dr")
	if destinationRuleHasSubsets(t, first) {
		t.Fatalf("first reconcile should remove DR subsets")
	}
	firstSpec := runtime.DeepCopyJSONValue(first.Object["spec"])

	if err := rec.reconcileTraffic(ctx, adoption); err != nil {
		t.Fatalf("second reconcileTraffic() error = %v", err)
	}
	second := getDestinationRule(t, rec.Client, ns, "managed-dr")
	if !reflect.DeepEqual(second.Object["spec"], firstSpec) {
		t.Fatalf("second reconcile changed DR spec: first=%v second=%v", firstSpec, second.Object["spec"])
	}
}

func TestReconcileTrafficObservedRoutesSuccessAndIdempotentMessage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ns := "platform-a"
	servicePort := int32(5555)

	managedVS := makeVirtualService(
		"managed-vs",
		ns,
		[]interface{}{
			map[string]interface{}{
				"route": []interface{}{
					makeRoute("gops-ckbackup.platform-a.svc.cluster.local", 5555, "stable"),
				},
			},
		},
	)
	managedDR := makeDestinationRule(
		"managed-dr",
		ns,
		"gops-ckbackup.platform-a.svc.cluster.local",
		[]interface{}{makeSubset("stable")},
	)

	rec := newTrafficTestReconciler(t, managedVS, managedDR)
	adoption := makeTrafficAdoptionWithIstioRefs(
		"gops-ckbackup",
		ns,
		&servicePort,
		[]string{"managed-vs"},
		[]string{"managed-dr"},
	)

	if err := rec.reconcileTraffic(ctx, adoption); err != nil {
		t.Fatalf("first reconcileTraffic() error = %v", err)
	}
	if adoption.Status.Traffic == nil {
		t.Fatalf("observed traffic status not written")
	}
	if len(adoption.Status.Traffic.ObservedRoutes) != 2 {
		t.Fatalf("observedRoutes len = %d, want 2", len(adoption.Status.Traffic.ObservedRoutes))
	}
	assertObservedRoute(t, adoption.Status.Traffic.ObservedRoutes[0], "VirtualService", "managed-vs", true, "subset removed from target destinations")
	assertObservedRoute(t, adoption.Status.Traffic.ObservedRoutes[1], "DestinationRule", "managed-dr", true, "subsets removed")

	if err := rec.reconcileTraffic(ctx, adoption); err != nil {
		t.Fatalf("second reconcileTraffic() error = %v", err)
	}
	if adoption.Status.Traffic == nil || len(adoption.Status.Traffic.ObservedRoutes) != 2 {
		t.Fatalf("observedRoutes should remain complete after second reconcile")
	}
	assertObservedRoute(t, adoption.Status.Traffic.ObservedRoutes[0], "VirtualService", "managed-vs", true, "already converged")
	assertObservedRoute(t, adoption.Status.Traffic.ObservedRoutes[1], "DestinationRule", "managed-dr", true, "already converged")
}

func TestReconcileTrafficObservedRoutesFailurePersistsMessage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ns := "platform-a"

	rec := newTrafficTestReconciler(t)
	adoption := makeTrafficAdoptionWithIstioRefs(
		"gops-ckbackup",
		ns,
		nil,
		[]string{""},
		nil,
	)

	err := rec.reconcileTraffic(ctx, adoption)
	if err == nil {
		t.Fatalf("reconcileTraffic() expected error but got nil")
	}
	wantErr := "traffic.istio.virtualServiceRefs.name must not be empty"
	if err.Error() != wantErr {
		t.Fatalf("reconcileTraffic() error = %q, want %q", err.Error(), wantErr)
	}
	if adoption.Status.Traffic == nil {
		t.Fatalf("observed traffic status not written on failure")
	}
	if len(adoption.Status.Traffic.ObservedRoutes) != 1 {
		t.Fatalf("observedRoutes len = %d, want 1", len(adoption.Status.Traffic.ObservedRoutes))
	}
	assertObservedRoute(t, adoption.Status.Traffic.ObservedRoutes[0], "VirtualService", "", false, wantErr)
}

func TestReconcileTrafficObservedRoutesUnsupportedMode(t *testing.T) {
	t.Parallel()

	rec := newTrafficTestReconciler(t)
	adoption := makeTrafficAdoptionWithIstioRefs("gops-ckbackup", "platform-a", nil, nil, nil)
	adoption.Spec.Traffic.Mode = ifaasv1alpha1.TrafficMode("shadow")

	err := rec.reconcileTraffic(context.Background(), adoption)
	if err == nil {
		t.Fatalf("reconcileTraffic() expected error but got nil")
	}
	wantErr := "unsupported traffic mode \"shadow\""
	if err.Error() != wantErr {
		t.Fatalf("reconcileTraffic() error = %q, want %q", err.Error(), wantErr)
	}
	if adoption.Status.Traffic == nil || len(adoption.Status.Traffic.ObservedRoutes) != 1 {
		t.Fatalf("unsupported mode should write one observed route")
	}
	assertObservedRoute(t, adoption.Status.Traffic.ObservedRoutes[0], "Traffic", "mode", false, wantErr)
}

func TestReconcileTrafficSnapshotCapturesOriginalSpecAndDoesNotOverwrite(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ns := "platform-a"
	servicePort := int32(5555)

	managedVS := makeVirtualService(
		"managed-vs",
		ns,
		[]interface{}{
			map[string]interface{}{
				"route": []interface{}{
					makeRoute("gops-ckbackup.platform-a.svc.cluster.local", 5555, "stable"),
				},
			},
		},
	)
	managedDR := makeDestinationRule(
		"managed-dr",
		ns,
		"gops-ckbackup.platform-a.svc.cluster.local",
		[]interface{}{makeSubset("stable")},
	)

	rec := newTrafficTestReconciler(t, managedVS, managedDR)
	adoption := makeTrafficAdoptionWithIstioRefs(
		"gops-ckbackup",
		ns,
		&servicePort,
		[]string{"managed-vs"},
		[]string{"managed-dr"},
	)

	if err := rec.reconcileTraffic(ctx, adoption); err != nil {
		t.Fatalf("first reconcileTraffic() error = %v", err)
	}
	if adoption.Status.SourceSnapshot == nil || adoption.Status.SourceSnapshot.Traffic == nil {
		t.Fatalf("traffic snapshot not written")
	}
	trafficSnap := adoption.Status.SourceSnapshot.Traffic
	if len(trafficSnap.VirtualServices) != 1 || len(trafficSnap.DestinationRules) != 1 {
		t.Fatalf("snapshot entries unexpected: vs=%d dr=%d", len(trafficSnap.VirtualServices), len(trafficSnap.DestinationRules))
	}

	vsSnap := trafficSnap.VirtualServices[0]
	drSnap := trafficSnap.DestinationRules[0]
	assertSnapshotContainsSubset(t, vsSnap.Spec.Raw, true)
	assertSnapshotContainsDRSubsets(t, drSnap.Spec.Raw, true)

	firstVSSnapRaw := append([]byte(nil), vsSnap.Spec.Raw...)
	firstDRSnapRaw := append([]byte(nil), drSnap.Spec.Raw...)

	liveVS := getVirtualService(t, rec.Client, ns, "managed-vs")
	if err := unstructured.SetNestedSlice(liveVS.Object, []interface{}{
		map[string]interface{}{
			"route": []interface{}{
				makeRoute("gops-ckbackup.platform-a.svc.cluster.local", 5555, "rewrite-attempt"),
			},
		},
	}, "spec", "http"); err != nil {
		t.Fatalf("set live VS spec for overwrite check: %v", err)
	}
	if err := rec.Update(ctx, liveVS); err != nil {
		t.Fatalf("update live VS for overwrite check: %v", err)
	}

	liveDR := getDestinationRule(t, rec.Client, ns, "managed-dr")
	if err := unstructured.SetNestedSlice(liveDR.Object, []interface{}{makeSubset("rewrite-attempt")}, "spec", "subsets"); err != nil {
		t.Fatalf("set live DR subsets for overwrite check: %v", err)
	}
	if err := rec.Update(ctx, liveDR); err != nil {
		t.Fatalf("update live DR for overwrite check: %v", err)
	}

	if err := rec.reconcileTraffic(ctx, adoption); err != nil {
		t.Fatalf("second reconcileTraffic() error = %v", err)
	}
	trafficSnap = adoption.Status.SourceSnapshot.Traffic
	if !bytes.Equal(trafficSnap.VirtualServices[0].Spec.Raw, firstVSSnapRaw) {
		t.Fatalf("virtualservice snapshot should not be overwritten")
	}
	if !bytes.Equal(trafficSnap.DestinationRules[0].Spec.Raw, firstDRSnapRaw) {
		t.Fatalf("destinationrule snapshot should not be overwritten")
	}
}

func TestStatusEqualDetectsTrafficSnapshotChange(t *testing.T) {
	t.Parallel()

	base := makeTrafficAdoptionWithIstioRefs("gops-ckbackup", "platform-a", nil, []string{"managed-vs"}, []string{"managed-dr"})
	base.Status.SourceSnapshot = &ifaasv1alpha1.SourceSnapshot{
		Traffic: &ifaasv1alpha1.TrafficSnapshot{
			VirtualServices: []ifaasv1alpha1.TrafficObjectSnapshot{{
				Name: "managed-vs",
				Spec: runtime.RawExtension{Raw: []byte(`{"hosts":["a.example"],"http":[]}`)},
			}},
			DestinationRules: []ifaasv1alpha1.TrafficObjectSnapshot{{
				Name: "managed-dr",
				Spec: runtime.RawExtension{Raw: []byte(`{"host":"a.example"}`)},
			}},
		},
	}

	same := base.DeepCopy()
	if !statusEqual(base, same) {
		t.Fatalf("statusEqual should return true for identical snapshots")
	}

	changed := base.DeepCopy()
	changed.Status.SourceSnapshot.Traffic.VirtualServices[0].Spec.Raw = []byte(`{"hosts":["b.example"],"http":[]}`)
	if statusEqual(base, changed) {
		t.Fatalf("statusEqual should detect snapshot raw changes")
	}
}

func newTrafficTestReconciler(t *testing.T, objs ...client.Object) *KnativeAdoptionReconciler {
	t.Helper()
	s := newTrafficTestScheme(t)
	cl := fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()
	return &KnativeAdoptionReconciler{Client: cl, Scheme: s}
}

func newTrafficTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := ifaasv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("add ifaas scheme: %v", err)
	}
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("add corev1 scheme: %v", err)
	}
	s.AddKnownTypeWithName(virtualServiceGVK, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group:   virtualServiceGVK.Group,
		Version: virtualServiceGVK.Version,
		Kind:    "VirtualServiceList",
	}, &unstructured.UnstructuredList{})
	s.AddKnownTypeWithName(destinationRuleGVK, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group:   destinationRuleGVK.Group,
		Version: destinationRuleGVK.Version,
		Kind:    "DestinationRuleList",
	}, &unstructured.UnstructuredList{})
	return s
}

func makeTrafficAdoption(name, namespace string, servicePort *int32, refs []string) *ifaasv1alpha1.KnativeAdoption {
	return makeTrafficAdoptionWithIstioRefs(name, namespace, servicePort, refs, nil)
}

func makeTrafficAdoptionWithIstioRefs(
	name,
	namespace string,
	servicePort *int32,
	virtualServiceRefs,
	destinationRuleRefs []string,
) *ifaasv1alpha1.KnativeAdoption {
	vsRefs := make([]ifaasv1alpha1.NamespacedObjectRef, 0, len(virtualServiceRefs))
	for _, item := range virtualServiceRefs {
		vsRefs = append(vsRefs, ifaasv1alpha1.NamespacedObjectRef{Name: item})
	}
	drRefs := make([]ifaasv1alpha1.NamespacedObjectRef, 0, len(destinationRuleRefs))
	for _, item := range destinationRuleRefs {
		drRefs = append(drRefs, ifaasv1alpha1.NamespacedObjectRef{Name: item})
	}
	return &ifaasv1alpha1.KnativeAdoption{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: ifaasv1alpha1.KnativeAdoptionSpec{
			SourceRef: ifaasv1alpha1.SourceRef{Kind: ifaasv1alpha1.SourceKindDeployment, Name: name},
			Traffic: &ifaasv1alpha1.Traffic{
				Enabled:     true,
				Mode:        ifaasv1alpha1.TrafficModeInplace,
				ServicePort: servicePort,
				Istio: &ifaasv1alpha1.IstioTraffic{
					VirtualServiceRefs:   vsRefs,
					DestinationRuleRefs: drRefs,
				},
			},
		},
	}
}

func makeVirtualService(name, namespace string, http []interface{}) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": virtualServiceGVK.GroupVersion().String(),
		"kind":       virtualServiceGVK.Kind,
		"metadata": map[string]interface{}{
			"name":      name,
			"namespace": namespace,
		},
		"spec": map[string]interface{}{
			"hosts": []interface{}{"gops-ckbackup-test.diezhi.net"},
			"gateways": []interface{}{
				"netops/common-inbound-gateway",
			},
			"http": http,
		},
	}}
}

func makeDestinationRule(name, namespace, host string, subsets []interface{}) *unstructured.Unstructured {
	spec := map[string]interface{}{
		"host": host,
		"trafficPolicy": map[string]interface{}{
			"loadBalancer": map[string]interface{}{"simple": "ROUND_ROBIN"},
		},
	}
	if subsets != nil {
		spec["subsets"] = subsets
	}
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": destinationRuleGVK.GroupVersion().String(),
		"kind":       destinationRuleGVK.Kind,
		"metadata": map[string]interface{}{
			"name":      name,
			"namespace": namespace,
		},
		"spec": spec,
	}}
}

func makeSubset(name string) map[string]interface{} {
	return map[string]interface{}{
		"name": name,
		"labels": map[string]interface{}{
			"app": "gops-ckbackup",
		},
	}
}

func makeRoute(host string, port int64, subset string) map[string]interface{} {
	destination := map[string]interface{}{
		"host": host,
		"port": map[string]interface{}{"number": port},
	}
	if subset != "" {
		destination["subset"] = subset
	}
	return map[string]interface{}{
		"destination": destination,
		"weight":      int64(100),
	}
}

func makeServiceWithPort(name, namespace string, port int32) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Name:     "http",
					Port:     port,
					Protocol: corev1.ProtocolTCP,
				},
			},
		},
	}
}

func getVirtualService(t *testing.T, cl client.Client, namespace, name string) *unstructured.Unstructured {
	t.Helper()
	vs := &unstructured.Unstructured{}
	vs.SetGroupVersionKind(virtualServiceGVK)
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, vs); err != nil {
		t.Fatalf("get VirtualService %s/%s: %v", namespace, name, err)
	}
	return vs
}

func getDestinationRule(t *testing.T, cl client.Client, namespace, name string) *unstructured.Unstructured {
	t.Helper()
	dr := &unstructured.Unstructured{}
	dr.SetGroupVersionKind(destinationRuleGVK)
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, dr); err != nil {
		t.Fatalf("get DestinationRule %s/%s: %v", namespace, name, err)
	}
	return dr
}

func destinationRuleHasSubsets(t *testing.T, dr *unstructured.Unstructured) bool {
	t.Helper()
	_, found, err := unstructured.NestedFieldNoCopy(dr.Object, "spec", "subsets")
	if err != nil {
		t.Fatalf("read DestinationRule subsets: %v", err)
	}
	return found
}

func hasSubset(vs *unstructured.Unstructured, httpIdx, routeIdx int) bool {
	destination, ok := routeDestination(vs, httpIdx, routeIdx)
	if !ok {
		return false
	}
	_, exists := destination["subset"]
	return exists
}

func routePortNumber(vs *unstructured.Unstructured, httpIdx, routeIdx int) (int32, bool) {
	destination, ok := routeDestination(vs, httpIdx, routeIdx)
	if !ok {
		return 0, false
	}
	portBlock, ok := destination["port"].(map[string]interface{})
	if !ok {
		return 0, false
	}
	return nestedInt32(portBlock, "number")
}

func routeDestination(vs *unstructured.Unstructured, httpIdx, routeIdx int) (map[string]interface{}, bool) {
	https, ok := vs.Object["spec"].(map[string]interface{})["http"].([]interface{})
	if !ok || httpIdx < 0 || httpIdx >= len(https) {
		return nil, false
	}
	httpRoute, ok := https[httpIdx].(map[string]interface{})
	if !ok {
		return nil, false
	}
	routes, ok := httpRoute["route"].([]interface{})
	if !ok || routeIdx < 0 || routeIdx >= len(routes) {
		return nil, false
	}
	route, ok := routes[routeIdx].(map[string]interface{})
	if !ok {
		return nil, false
	}
	destination, ok := route["destination"].(map[string]interface{})
	if !ok {
		return nil, false
	}
	return destination, true
}

func assertSnapshotContainsSubset(t *testing.T, raw []byte, wantSubset bool) {
	t.Helper()
	spec := decodeSnapshotSpec(t, raw)
	https, ok := spec["http"].([]interface{})
	if !ok || len(https) == 0 {
		t.Fatalf("snapshot spec.http missing or empty")
	}
	httpRoute, ok := https[0].(map[string]interface{})
	if !ok {
		t.Fatalf("snapshot first http route malformed")
	}
	routes, ok := httpRoute["route"].([]interface{})
	if !ok || len(routes) == 0 {
		t.Fatalf("snapshot route missing or empty")
	}
	route, ok := routes[0].(map[string]interface{})
	if !ok {
		t.Fatalf("snapshot first route malformed")
	}
	destination, ok := route["destination"].(map[string]interface{})
	if !ok {
		t.Fatalf("snapshot destination missing")
	}
	_, hasSubset := destination["subset"]
	if hasSubset != wantSubset {
		t.Fatalf("snapshot destination subset = %v, want %v", hasSubset, wantSubset)
	}
}

func assertSnapshotContainsDRSubsets(t *testing.T, raw []byte, wantSubsets bool) {
	t.Helper()
	spec := decodeSnapshotSpec(t, raw)
	_, hasSubsets := spec["subsets"]
	if hasSubsets != wantSubsets {
		t.Fatalf("snapshot destinationrule subsets = %v, want %v", hasSubsets, wantSubsets)
	}
}

func decodeSnapshotSpec(t *testing.T, raw []byte) map[string]interface{} {
	t.Helper()
	out := map[string]interface{}{}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode snapshot raw spec: %v", err)
	}
	return out
}

func assertObservedRoute(t *testing.T, got ifaasv1alpha1.ObservedRoute, wantType, wantName string, wantReady bool, wantMessage string) {
	t.Helper()
	if got.Type != wantType {
		t.Fatalf("observed route type = %q, want %q", got.Type, wantType)
	}
	if got.Name != wantName {
		t.Fatalf("observed route name = %q, want %q", got.Name, wantName)
	}
	if got.Ready == nil {
		t.Fatalf("observed route ready is nil")
	}
	if *got.Ready != wantReady {
		t.Fatalf("observed route ready = %v, want %v", *got.Ready, wantReady)
	}
	if got.Message != wantMessage {
		t.Fatalf("observed route message = %q, want %q", got.Message, wantMessage)
	}
}

func assertHostsGatewaysPreserved(t *testing.T, vs *unstructured.Unstructured) {
	t.Helper()
	hosts, _, err := unstructured.NestedStringSlice(vs.Object, "spec", "hosts")
	if err != nil {
		t.Fatalf("read hosts: %v", err)
	}
	if !reflect.DeepEqual(hosts, []string{"gops-ckbackup-test.diezhi.net"}) {
		t.Fatalf("hosts changed: %v", hosts)
	}
	gateways, _, err := unstructured.NestedStringSlice(vs.Object, "spec", "gateways")
	if err != nil {
		t.Fatalf("read gateways: %v", err)
	}
	if !reflect.DeepEqual(gateways, []string{"netops/common-inbound-gateway"}) {
		t.Fatalf("gateways changed: %v", gateways)
	}
}
