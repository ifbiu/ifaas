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

import "errors"

// Sentinel errors returned by Translate. Callers compare with errors.Is.
// The reconciler maps these to KnativeAdoption Conditions
// (TranslationDegraded, ServiceAdoptionRefused, etc.).
var (
	ErrNoContainer         = errors.New("translator: deployment has no containers")
	ErrAmbiguousPrimary    = errors.New("translator: deployment has >1 container but spec.primaryContainer is empty")
	ErrPrimaryNotFound     = errors.New("translator: spec.primaryContainer does not match any container in deployment")
	ErrNoContainerPort     = errors.New("translator: primary container has no ports")
	ErrUnsupportedProtocol = errors.New("translator: primary container port protocol must be TCP")
	ErrHostNetwork         = errors.New("translator: hostNetwork is rejected by Knative")
	ErrHostPID             = errors.New("translator: hostPID is rejected by Knative")
	ErrHostIPC             = errors.New("translator: hostIPC is rejected by Knative")
	ErrHostPort            = errors.New("translator: hostPort is rejected by Knative")
)
