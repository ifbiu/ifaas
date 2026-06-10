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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	kservingv1 "knative.dev/serving/pkg/apis/serving/v1"

	"github.com/ifbiu/ifaas/internal/flusher"
	"github.com/ifbiu/ifaas/internal/translator"
)

var _ = Describe("KSvcMinScalePatcher", func() {
	const ns = "default"

	var (
		ictx    context.Context
		patcher *KSvcMinScalePatcher
	)

	BeforeEach(func() {
		ictx = context.Background()
		patcher = &KSvcMinScalePatcher{Client: k8sClient}
	})

	AfterEach(func() {
		// Best-effort cleanup; suite-level teardown removes the rest.
		_ = k8sClient.Delete(ictx, &kservingv1.Service{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "patcher-target"},
		})
	})

	It("skips when the KSvc is missing", func() {
		skipped, err := patcher.Patch(ictx, flusher.Decision{
			Namespace: ns, KSvcName: "does-not-exist", DesiredMinScale: 1,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(skipped).To(BeTrue())
	})

	It("skips when the desired value already matches the live annotation", func() {
		Expect(k8sClient.Create(ictx, &kservingv1.Service{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "patcher-target"},
			Spec: kservingv1.ServiceSpec{
				ConfigurationSpec: kservingv1.ConfigurationSpec{
					Template: kservingv1.RevisionTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{
								translator.AutoscalingMinScaleAnnotation: "1",
							},
						},
					},
				},
			},
		})).To(Succeed())

		skipped, err := patcher.Patch(ictx, flusher.Decision{
			Namespace: ns, KSvcName: "patcher-target", DesiredMinScale: 1,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(skipped).To(BeTrue())
	})

	It("patches the annotation when the desired value differs", func() {
		Expect(k8sClient.Create(ictx, &kservingv1.Service{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "patcher-target"},
			Spec: kservingv1.ServiceSpec{
				ConfigurationSpec: kservingv1.ConfigurationSpec{
					Template: kservingv1.RevisionTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{
								translator.AutoscalingMinScaleAnnotation: "0",
								"sample-keep-me":                         "yes",
							},
						},
					},
				},
			},
		})).To(Succeed())

		skipped, err := patcher.Patch(ictx, flusher.Decision{
			Namespace: ns, KSvcName: "patcher-target", DesiredMinScale: 1,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(skipped).To(BeFalse())

		got := &kservingv1.Service{}
		Expect(k8sClient.Get(ictx, types.NamespacedName{Namespace: ns, Name: "patcher-target"}, got)).To(Succeed())
		Expect(got.Spec.Template.GetAnnotations()).To(HaveKeyWithValue(translator.AutoscalingMinScaleAnnotation, "1"))
		Expect(got.Spec.Template.GetAnnotations()).To(HaveKeyWithValue("sample-keep-me", "yes"))
	})
})
