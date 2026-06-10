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

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	ifaasv1alpha1 "github.com/ifbiu/ifaas/api/ifaas/v1alpha1"
	"github.com/ifbiu/ifaas/internal/projector"
)

// DeploymentWatcher reconciles labeled Deployments into KnativeAdoption CRs.
//
// State transitions, expressed in terms of the source Deployment:
//
//	(no label)              → ensure no watcher-owned CR exists with this name.
//	(label=enabled)         → SSA a watcher-owned CR projecting Deployment
//	                          annotations onto KnativeAdoption.Spec.
//	(deleted)               → ensure no watcher-owned CR remains.
//
// A CR is "watcher-owned" iff it carries label LabelManagedByWatcher=true. The
// watcher never touches a CR without that label, so a user-authored CR with the
// same name is left for the adoption reconciler to consume directly.
type DeploymentWatcher struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch
// +kubebuilder:rbac:groups=ifaas.ifbiu.com,resources=knativeadoptions,verbs=get;list;watch;create;update;patch;delete

// Reconcile is the entry point. Three branches map to the three transitions
// listed above.
func (r *DeploymentWatcher) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx).WithValues("namespace", req.Namespace, "name", req.Name)

	var dep appsv1.Deployment
	err := r.Get(ctx, req.NamespacedName, &dep)
	switch {
	case apierrors.IsNotFound(err):
		return ctrl.Result{}, r.deleteIfWatcherOwned(ctx, req.NamespacedName)
	case err != nil:
		return ctrl.Result{}, err
	}

	if !isAdoptionLabelEnabled(&dep) {
		log.V(1).Info("deployment label not enabled; ensuring no watcher-owned adoption")
		return ctrl.Result{}, r.deleteIfWatcherOwned(ctx, req.NamespacedName)
	}

	return ctrl.Result{}, r.applyAdoption(ctx, &dep)
}

func isAdoptionLabelEnabled(dep *appsv1.Deployment) bool {
	return dep.GetLabels()[LabelEnabled] == LabelEnabledValue
}

func (r *DeploymentWatcher) deleteIfWatcherOwned(ctx context.Context, key types.NamespacedName) error {
	existing := &ifaasv1alpha1.KnativeAdoption{}
	if err := r.Get(ctx, key, existing); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if existing.GetLabels()[LabelManagedByWatcher] != LabelManagedByWatcherValue {
		// User-authored CR; the watcher must not interfere with it.
		return nil
	}
	if err := r.Delete(ctx, existing); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete watcher-owned adoption: %w", err)
	}
	return nil
}

func (r *DeploymentWatcher) applyAdoption(ctx context.Context, dep *appsv1.Deployment) error {
	log := logf.FromContext(ctx)

	spec, warnings := projector.FromDeployment(dep)
	for _, w := range warnings {
		log.Info("annotation projection warning", "warning", w)
	}

	// Detect a pre-existing user-authored CR. If it lacks the watcher label,
	// we step out completely; the user owns the configuration loop.
	existing := &ifaasv1alpha1.KnativeAdoption{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: dep.Namespace, Name: dep.Name}, existing); err == nil {
		if existing.GetLabels()[LabelManagedByWatcher] != LabelManagedByWatcherValue {
			log.V(1).Info("user-authored adoption already exists; watcher stays out of the way")
			return nil
		}
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	desired := &ifaasv1alpha1.KnativeAdoption{
		TypeMeta: metav1.TypeMeta{
			APIVersion: ifaasv1alpha1.GroupVersion.String(),
			Kind:       "KnativeAdoption",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      dep.Name,
			Namespace: dep.Namespace,
			Labels: map[string]string{
				LabelManagedByWatcher: LabelManagedByWatcherValue,
				LabelEnabled:          LabelEnabledValue,
			},
		},
		Spec: spec,
	}

	if err := r.Patch(ctx, desired, client.Apply, client.FieldOwner(FieldOwnerWatcher), client.ForceOwnership); err != nil {
		return fmt.Errorf("ssa watcher-owned adoption: %w", err)
	}
	return nil
}

// SetupWithManager wires the watcher into the manager. There is only one
// primary source (Deployment); KnativeAdoption events do not feed back into
// this controller because the adoption reconciler in S3 already watches them.
func (r *DeploymentWatcher) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1.Deployment{}).
		Named("ifaas-deployment-watcher").
		Complete(r)
}
