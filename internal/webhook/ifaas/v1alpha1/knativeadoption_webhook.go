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

package v1alpha1

import (
	"context"

	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	ifaasv1alpha1 "github.com/ifbiu/ifaas/api/ifaas/v1alpha1"
)

// log is for logging in this package.
//
//nolint:unused
var knativeadoptionlog = logf.Log.WithName("knativeadoption-resource")

// SetupKnativeAdoptionWebhookWithManager registers the webhook for KnativeAdoption in the manager.
func SetupKnativeAdoptionWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &ifaasv1alpha1.KnativeAdoption{}).
		WithValidator(&KnativeAdoptionCustomValidator{}).
		WithDefaulter(&KnativeAdoptionCustomDefaulter{}).
		Complete()
}

// TODO(user): EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!

// +kubebuilder:webhook:path=/mutate-ifaas-ifbiu-com-v1alpha1-knativeadoption,mutating=true,failurePolicy=fail,sideEffects=None,groups=ifaas.ifbiu.com,resources=knativeadoptions,verbs=create;update,versions=v1alpha1,name=mknativeadoption-v1alpha1.kb.io,admissionReviewVersions=v1

// KnativeAdoptionCustomDefaulter struct is responsible for setting default values on the custom resource of the
// Kind KnativeAdoption when those are created or updated.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as it is used only for temporary operations and does not need to be deeply copied.
type KnativeAdoptionCustomDefaulter struct {
	// TODO(user): Add more fields as needed for defaulting
}

// Default implements webhook.CustomDefaulter so a webhook will be registered for the Kind KnativeAdoption.
func (d *KnativeAdoptionCustomDefaulter) Default(_ context.Context, obj *ifaasv1alpha1.KnativeAdoption) error {
	knativeadoptionlog.Info("Defaulting for KnativeAdoption", "name", obj.GetName())

	// TODO(user): fill in your defaulting logic.

	return nil
}

// TODO(user): change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
// NOTE: If you want to customise the 'path', use the flags '--defaulting-path' or '--validation-path'.
// +kubebuilder:webhook:path=/validate-ifaas-ifbiu-com-v1alpha1-knativeadoption,mutating=false,failurePolicy=fail,sideEffects=None,groups=ifaas.ifbiu.com,resources=knativeadoptions,verbs=create;update,versions=v1alpha1,name=vknativeadoption-v1alpha1.kb.io,admissionReviewVersions=v1

// KnativeAdoptionCustomValidator struct is responsible for validating the KnativeAdoption resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type KnativeAdoptionCustomValidator struct {
	// TODO(user): Add more fields as needed for validation
}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type KnativeAdoption.
func (v *KnativeAdoptionCustomValidator) ValidateCreate(_ context.Context, obj *ifaasv1alpha1.KnativeAdoption) (admission.Warnings, error) {
	knativeadoptionlog.Info("Validation for KnativeAdoption upon creation", "name", obj.GetName())

	// TODO(user): fill in your validation logic upon object creation.

	return nil, nil
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type KnativeAdoption.
func (v *KnativeAdoptionCustomValidator) ValidateUpdate(_ context.Context, oldObj, newObj *ifaasv1alpha1.KnativeAdoption) (admission.Warnings, error) {
	knativeadoptionlog.Info("Validation for KnativeAdoption upon update", "name", newObj.GetName())

	// TODO(user): fill in your validation logic upon object update.

	return nil, nil
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type KnativeAdoption.
func (v *KnativeAdoptionCustomValidator) ValidateDelete(_ context.Context, obj *ifaasv1alpha1.KnativeAdoption) (admission.Warnings, error) {
	knativeadoptionlog.Info("Validation for KnativeAdoption upon deletion", "name", obj.GetName())

	// TODO(user): fill in your validation logic upon object deletion.

	return nil, nil
}
