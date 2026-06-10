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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	ifaasv1alpha1 "github.com/ifbiu/ifaas/api/ifaas/v1alpha1"
)

// KnativeAdoptionReconciler reconciles a KnativeAdoption object
type KnativeAdoptionReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=ifaas.ifbiu.com,resources=knativeadoptions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ifaas.ifbiu.com,resources=knativeadoptions/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ifaas.ifbiu.com,resources=knativeadoptions/finalizers,verbs=update
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=pods/proxy,verbs=get

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the KnativeAdoption object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.24.1/pkg/reconcile
func (r *KnativeAdoptionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	log.Info("reconcile triggered", "namespace", req.Namespace, "name", req.Name)

	var adoption ifaasv1alpha1.KnativeAdoption
	if err := r.Get(ctx, req.NamespacedName, &adoption); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("resource not found, assume deleted", "namespace", req.Namespace, "name", req.Name)
			return ctrl.Result{}, nil
		}
		log.Error(err, "failed to get KnativeAdoption", "namespace", req.Namespace, "name", req.Name)
		return ctrl.Result{}, err
	}

	log.Info("fetched KnativeAdoption",
		"namespace", adoption.Namespace,
		"name", adoption.Name,
		"generation", adoption.Generation,
		"resourceVersion", adoption.ResourceVersion,
		"conditions", len(adoption.Status.Conditions),
	)

	// TODO(user): your logic here

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *KnativeAdoptionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&ifaasv1alpha1.KnativeAdoption{}).
		Watches(
			&corev1.Pod{},
			handler.EnqueueRequestsFromMapFunc(r.mapPodToKnativeAdoptions),
		).
		Named("ifaas-knativeadoption").
		Complete(r)
}

// mapPodToKnativeAdoptions fans out a Pod event to reconcile requests for every
// KnativeAdoption in the same namespace. If no KnativeAdoption exists, the
// Reconcile function is not called, so we still log the raw pod event here for
// visibility.
func (r *KnativeAdoptionReconciler) mapPodToKnativeAdoptions(ctx context.Context, obj client.Object) []reconcile.Request {
	log := logf.FromContext(ctx).WithName("pod-watch")

	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil
	}

	log.Info("pod event observed",
		"namespace", pod.Namespace,
		"name", pod.Name,
		"phase", pod.Status.Phase,
	)

	var list ifaasv1alpha1.KnativeAdoptionList
	if err := r.List(ctx, &list, client.InNamespace(pod.Namespace)); err != nil {
		log.Error(err, "failed to list KnativeAdoption for pod fan-out",
			"namespace", pod.Namespace, "pod", pod.Name)
		return nil
	}

	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, adoption := range list.Items {
		requests = append(requests, reconcile.Request{
			NamespacedName: client.ObjectKey{
				Namespace: adoption.Namespace,
				Name:      adoption.Name,
			},
		})
	}
	log.Info("fan-out to KnativeAdoption", "pod", pod.Name, "count", len(requests))
	return requests
}
