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
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ifaasv1alpha1 "github.com/ifbiu/ifaas/api/ifaas/v1alpha1"
)

var virtualServiceGVK = schema.GroupVersionKind{
	Group:   "networking.istio.io",
	Version: "v1alpha3",
	Kind:    "VirtualService",
}

var destinationRuleGVK = schema.GroupVersionKind{
	Group:   "networking.istio.io",
	Version: "v1alpha3",
	Kind:    "DestinationRule",
}

// reconcileTraffic is the single entry point for traffic adaptation.
//
// M1-08 keeps the adapter minimal and in-place, and reports per-object status:
//   - only objects explicitly declared in spec.traffic.istio refs;
//   - VirtualService: strip destination.subset only on routes targeting the
//     adopted Service;
//   - DestinationRule: drop spec.subsets only when the DR host targets the
//     adopted Service;
//   - status.traffic.observedRoutes records one entry per declared object.
func (r *KnativeAdoptionReconciler) reconcileTraffic(ctx context.Context, adoption *ifaasv1alpha1.KnativeAdoption) error {
	if !trafficEnabled(adoption) {
		return nil
	}
	if trafficMode(adoption) != ifaasv1alpha1.TrafficModeInplace {
		err := fmt.Errorf("unsupported traffic mode %q", adoption.Spec.Traffic.Mode)
		setObservedRoutes(adoption, []ifaasv1alpha1.ObservedRoute{newObservedRoute("Traffic", "mode", false, err.Error())})
		return err
	}
	targetServicePort, err := r.resolveTrafficTargetServicePort(ctx, adoption)
	if err != nil {
		setObservedRoutes(adoption, []ifaasv1alpha1.ObservedRoute{newObservedRoute("Traffic", "servicePort", false, err.Error())})
		return err
	}

	observed := make([]ifaasv1alpha1.ObservedRoute, 0, len(virtualServiceRefs(adoption))+len(destinationRuleRefs(adoption)))
	var firstErr error

	for _, ref := range virtualServiceRefs(adoption) {
		if ref.Name == "" {
			err := fmt.Errorf("traffic.istio.virtualServiceRefs.name must not be empty")
			observed = append(observed, newObservedRoute("VirtualService", "", false, err.Error()))
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		changed, err := r.reconcileVirtualService(ctx, adoption, ref.Name, targetServicePort)
		if err != nil {
			observed = append(observed, newObservedRoute("VirtualService", ref.Name, false, err.Error()))
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		msg := "already converged"
		if changed {
			msg = "subset removed from target destinations"
		}
		observed = append(observed, newObservedRoute("VirtualService", ref.Name, true, msg))
	}

	for _, ref := range destinationRuleRefs(adoption) {
		if ref.Name == "" {
			err := fmt.Errorf("traffic.istio.destinationRuleRefs.name must not be empty")
			observed = append(observed, newObservedRoute("DestinationRule", "", false, err.Error()))
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		changed, err := r.reconcileDestinationRule(ctx, adoption, ref.Name)
		if err != nil {
			observed = append(observed, newObservedRoute("DestinationRule", ref.Name, false, err.Error()))
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		msg := "already converged"
		if changed {
			msg = "subsets removed"
		}
		observed = append(observed, newObservedRoute("DestinationRule", ref.Name, true, msg))
	}

	setObservedRoutes(adoption, observed)
	if firstErr != nil {
		return firstErr
	}
	return nil
}

func setObservedRoutes(adoption *ifaasv1alpha1.KnativeAdoption, routes []ifaasv1alpha1.ObservedRoute) {
	if adoption == nil {
		return
	}
	if len(routes) == 0 {
		adoption.Status.Traffic = nil
		return
	}
	copied := make([]ifaasv1alpha1.ObservedRoute, 0, len(routes))
	for _, route := range routes {
		cloned := route
		if route.Ready != nil {
			cloned.Ready = boolPtr(*route.Ready)
		}
		copied = append(copied, cloned)
	}
	adoption.Status.Traffic = &ifaasv1alpha1.TrafficStatus{ObservedRoutes: copied}
}

func newObservedRoute(kind, name string, ready bool, msg string) ifaasv1alpha1.ObservedRoute {
	return ifaasv1alpha1.ObservedRoute{
		Type:    kind,
		Name:    name,
		Ready:   boolPtr(ready),
		Message: msg,
	}
}

func boolPtr(v bool) *bool {
	b := v
	return &b
}

func trafficEnabled(adoption *ifaasv1alpha1.KnativeAdoption) bool {
	if adoption == nil || adoption.Spec.Traffic == nil {
		return false
	}
	return adoption.Spec.Traffic.Enabled
}

func trafficMode(adoption *ifaasv1alpha1.KnativeAdoption) ifaasv1alpha1.TrafficMode {
	if adoption == nil || adoption.Spec.Traffic == nil || adoption.Spec.Traffic.Mode == "" {
		return ifaasv1alpha1.TrafficModeInplace
	}
	return adoption.Spec.Traffic.Mode
}

func virtualServiceRefs(adoption *ifaasv1alpha1.KnativeAdoption) []ifaasv1alpha1.NamespacedObjectRef {
	if adoption == nil || adoption.Spec.Traffic == nil || adoption.Spec.Traffic.Istio == nil {
		return nil
	}
	return adoption.Spec.Traffic.Istio.VirtualServiceRefs
}

func destinationRuleRefs(adoption *ifaasv1alpha1.KnativeAdoption) []ifaasv1alpha1.NamespacedObjectRef {
	if adoption == nil || adoption.Spec.Traffic == nil || adoption.Spec.Traffic.Istio == nil {
		return nil
	}
	return adoption.Spec.Traffic.Istio.DestinationRuleRefs
}

func specTrafficServicePort(adoption *ifaasv1alpha1.KnativeAdoption) *int32 {
	if adoption == nil || adoption.Spec.Traffic == nil || adoption.Spec.Traffic.ServicePort == nil {
		return nil
	}
	v := *adoption.Spec.Traffic.ServicePort
	return &v
}

func firstServicePort(svc *corev1.Service) *int32 {
	if svc == nil || len(svc.Spec.Ports) == 0 {
		return nil
	}
	port := svc.Spec.Ports[0].Port
	if port <= 0 {
		return nil
	}
	v := port
	return &v
}

func (r *KnativeAdoptionReconciler) resolveTrafficTargetServicePort(ctx context.Context, adoption *ifaasv1alpha1.KnativeAdoption) (*int32, error) {
	fallback := specTrafficServicePort(adoption)
	if adoption == nil {
		return fallback, nil
	}

	svc := &corev1.Service{}
	key := types.NamespacedName{Namespace: adoption.Namespace, Name: sourceName(adoption)}
	if err := r.Get(ctx, key, svc); err != nil {
		if apierrors.IsNotFound(err) {
			return fallback, nil
		}
		return nil, fmt.Errorf("get target Service %s/%s: %w", adoption.Namespace, sourceName(adoption), err)
	}
	if port := firstServicePort(svc); port != nil {
		return port, nil
	}
	return fallback, nil
}

func snapshotVirtualServiceOnce(adoption *ifaasv1alpha1.KnativeAdoption, name string, vs *unstructured.Unstructured) error {
	if adoption == nil || name == "" {
		return nil
	}
	traffic := ensureTrafficSnapshot(adoption)
	if hasTrafficObjectSnapshot(traffic.VirtualServices, name) {
		return nil
	}
	spec, err := rawSpecSnapshot(vs)
	if err != nil {
		return err
	}
	traffic.VirtualServices = append(traffic.VirtualServices, ifaasv1alpha1.TrafficObjectSnapshot{
		Name: name,
		Spec: spec,
	})
	return nil
}

func snapshotDestinationRuleOnce(adoption *ifaasv1alpha1.KnativeAdoption, name string, dr *unstructured.Unstructured) error {
	if adoption == nil || name == "" {
		return nil
	}
	traffic := ensureTrafficSnapshot(adoption)
	if hasTrafficObjectSnapshot(traffic.DestinationRules, name) {
		return nil
	}
	spec, err := rawSpecSnapshot(dr)
	if err != nil {
		return err
	}
	traffic.DestinationRules = append(traffic.DestinationRules, ifaasv1alpha1.TrafficObjectSnapshot{
		Name: name,
		Spec: spec,
	})
	return nil
}

func ensureTrafficSnapshot(adoption *ifaasv1alpha1.KnativeAdoption) *ifaasv1alpha1.TrafficSnapshot {
	if adoption.Status.SourceSnapshot == nil {
		adoption.Status.SourceSnapshot = &ifaasv1alpha1.SourceSnapshot{}
	}
	if adoption.Status.SourceSnapshot.Traffic == nil {
		adoption.Status.SourceSnapshot.Traffic = &ifaasv1alpha1.TrafficSnapshot{}
	}
	return adoption.Status.SourceSnapshot.Traffic
}

func hasTrafficObjectSnapshot(items []ifaasv1alpha1.TrafficObjectSnapshot, name string) bool {
	for i := range items {
		if items[i].Name == name {
			return true
		}
	}
	return false
}

func rawSpecSnapshot(obj *unstructured.Unstructured) (runtime.RawExtension, error) {
	spec, found, err := unstructured.NestedFieldNoCopy(obj.Object, "spec")
	if err != nil {
		return runtime.RawExtension{}, fmt.Errorf("read spec: %w", err)
	}
	if !found {
		return runtime.RawExtension{}, nil
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		return runtime.RawExtension{}, fmt.Errorf("marshal spec: %w", err)
	}
	return runtime.RawExtension{Raw: raw}, nil
}

func (r *KnativeAdoptionReconciler) reconcileVirtualService(ctx context.Context, adoption *ifaasv1alpha1.KnativeAdoption, name string, targetServicePort *int32) (bool, error) {
	vs := &unstructured.Unstructured{}
	vs.SetGroupVersionKind(virtualServiceGVK)
	key := types.NamespacedName{Namespace: adoption.Namespace, Name: name}
	if err := r.Get(ctx, key, vs); err != nil {
		return false, fmt.Errorf("get VirtualService %s/%s: %w", adoption.Namespace, name, err)
	}
	if err := snapshotVirtualServiceOnce(adoption, name, vs); err != nil {
		return false, fmt.Errorf("snapshot VirtualService %s/%s: %w", adoption.Namespace, name, err)
	}

	before := vs.DeepCopy()
	changed, err := stripSubsetFromTargetDestinations(
		vs,
		sourceName(adoption),
		adoption.Namespace,
		specTrafficServicePort(adoption),
		targetServicePort,
	)
	if err != nil {
		return false, fmt.Errorf("reconcile VirtualService %s/%s: %w", adoption.Namespace, name, err)
	}
	if !changed {
		return false, nil
	}
	if err := r.Patch(ctx, vs, client.MergeFrom(before)); err != nil {
		return false, fmt.Errorf("patch VirtualService %s/%s: %w", adoption.Namespace, name, err)
	}
	return true, nil
}

func (r *KnativeAdoptionReconciler) reconcileDestinationRule(ctx context.Context, adoption *ifaasv1alpha1.KnativeAdoption, name string) (bool, error) {
	dr := &unstructured.Unstructured{}
	dr.SetGroupVersionKind(destinationRuleGVK)
	key := types.NamespacedName{Namespace: adoption.Namespace, Name: name}
	if err := r.Get(ctx, key, dr); err != nil {
		return false, fmt.Errorf("get DestinationRule %s/%s: %w", adoption.Namespace, name, err)
	}
	if err := snapshotDestinationRuleOnce(adoption, name, dr); err != nil {
		return false, fmt.Errorf("snapshot DestinationRule %s/%s: %w", adoption.Namespace, name, err)
	}

	before := dr.DeepCopy()
	changed, err := stripTargetServiceSubsets(dr, sourceName(adoption), adoption.Namespace)
	if err != nil {
		return false, fmt.Errorf("reconcile DestinationRule %s/%s: %w", adoption.Namespace, name, err)
	}
	if !changed {
		return false, nil
	}
	if err := r.Patch(ctx, dr, client.MergeFrom(before)); err != nil {
		return false, fmt.Errorf("patch DestinationRule %s/%s: %w", adoption.Namespace, name, err)
	}
	return true, nil
}

func stripTargetServiceSubsets(dr *unstructured.Unstructured, serviceName, namespace string) (bool, error) {
	host, found, err := unstructured.NestedString(dr.Object, "spec", "host")
	if err != nil {
		return false, fmt.Errorf("read spec.host: %w", err)
	}
	if !found || !serviceHostMatches(host, serviceName, namespace) {
		return false, nil
	}
	if _, found, err := unstructured.NestedFieldNoCopy(dr.Object, "spec", "subsets"); err != nil {
		return false, fmt.Errorf("read spec.subsets: %w", err)
	} else if !found {
		return false, nil
	}
	unstructured.RemoveNestedField(dr.Object, "spec", "subsets")
	return true, nil
}

func stripSubsetFromTargetDestinations(
	vs *unstructured.Unstructured,
	serviceName,
	namespace string,
	servicePort *int32,
	targetServicePort *int32,
) (bool, error) {
	https, found, err := unstructured.NestedSlice(vs.Object, "spec", "http")
	if err != nil {
		return false, fmt.Errorf("read spec.http: %w", err)
	}
	if !found || len(https) == 0 {
		return false, nil
	}

	managedPorts := trafficManagedPortSet(servicePort, targetServicePort)
	changed := false
	for i := range https {
		httpRoute, ok := https[i].(map[string]interface{})
		if !ok {
			continue
		}
		routes, ok := httpRoute["route"].([]interface{})
		if !ok || len(routes) == 0 {
			continue
		}

		routeChanged := false
		for j := range routes {
			route, ok := routes[j].(map[string]interface{})
			if !ok {
				continue
			}
			destination, ok := route["destination"].(map[string]interface{})
			if !ok {
				continue
			}
			if !destinationTargetsService(destination, serviceName, namespace) {
				continue
			}
			if !destinationMatchesManagedPort(destination, managedPorts) {
				continue
			}

			destinationChanged := false
			if targetServicePort != nil && ensureDestinationPort(destination, *targetServicePort) {
				destinationChanged = true
			}
			if _, exists := destination["subset"]; exists {
				delete(destination, "subset")
				destinationChanged = true
			}
			if !destinationChanged {
				continue
			}

			route["destination"] = destination
			routes[j] = route
			routeChanged = true
		}

		if routeChanged {
			httpRoute["route"] = routes
			https[i] = httpRoute
			changed = true
		}
	}

	if !changed {
		return false, nil
	}
	if err := unstructured.SetNestedSlice(vs.Object, https, "spec", "http"); err != nil {
		return false, fmt.Errorf("write spec.http: %w", err)
	}
	return true, nil
}

func trafficManagedPortSet(ports ...*int32) map[int32]struct{} {
	set := make(map[int32]struct{})
	for _, port := range ports {
		if port == nil || *port <= 0 {
			continue
		}
		set[*port] = struct{}{}
	}
	if len(set) == 0 {
		return nil
	}
	return set
}

func destinationMatchesManagedPort(destination map[string]interface{}, managedPorts map[int32]struct{}) bool {
	if len(managedPorts) == 0 {
		return true
	}
	portBlock, ok := destination["port"].(map[string]interface{})
	if !ok {
		return false
	}
	port, found := nestedInt32(portBlock, "number")
	if !found {
		return false
	}
	_, ok = managedPorts[port]
	return ok
}

func ensureDestinationPort(destination map[string]interface{}, servicePort int32) bool {
	portBlock, ok := destination["port"].(map[string]interface{})
	if !ok || portBlock == nil {
		destination["port"] = map[string]interface{}{"number": int64(servicePort)}
		return true
	}
	port, found := nestedInt32(portBlock, "number")
	if found && port == servicePort {
		return false
	}
	portBlock["number"] = int64(servicePort)
	destination["port"] = portBlock
	return true
}

func destinationTargetsService(destination map[string]interface{}, serviceName, namespace string) bool {
	host, _ := destination["host"].(string)
	return serviceHostMatches(host, serviceName, namespace)
}

func nestedInt32(obj map[string]interface{}, fields ...string) (int32, bool) {
	n, found, err := unstructured.NestedInt64(obj, fields...)
	if err != nil || !found {
		return 0, false
	}
	return int32(n), true
}

func serviceHostMatches(host, serviceName, namespace string) bool {
	h := normalizeHost(host)
	svc := normalizeHost(serviceName)
	ns := normalizeHost(namespace)
	if h == "" || svc == "" || ns == "" {
		return false
	}
	if h == svc || h == svc+"."+ns || h == svc+"."+ns+".svc" {
		return true
	}
	return strings.HasPrefix(h, svc+"."+ns+".svc.")
}

func normalizeHost(in string) string {
	out := strings.TrimSpace(in)
	out = strings.TrimSuffix(out, ".")
	return strings.ToLower(out)
}
