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

// Reason constants populated into KnativeAdoption.status.conditions[].reason.
// Kept here so the test package and the reconciler refer to the same strings.
const (
	ReasonAdopted                = "Adopted"
	ReasonSourceMissing          = "SourceMissing"
	ReasonTranslationFailed      = "TranslationFailed"
	ReasonServiceApplyFailed     = "ServiceApplyFailed"
	ReasonKnativeServiceNotReady = "KnativeServiceNotReady"
	ReasonKnativeServiceReady    = "KnativeServiceReady"
	ReasonSourceQuiesced         = "SourceQuiesced"
	ReasonSourceScaleFailed      = "SourceScaleFailed"
	ReasonReconciling            = "Reconciling"

	// ServiceSwapper reasons.
	ReasonServiceSwapping       = "ServiceSwapping"
	ReasonServiceSwapTakenOver  = "ServiceSwapTakenOver"
	ReasonServiceSwapTypeRefuse = "ServiceTypeNotSupported"
	ReasonServiceSwapOwnedByOS  = "ServiceOwnedByExternal"
	ReasonServiceSwapDeleteFail = "ServiceDeleteFailed"

	// ScaleDownGuard reasons.
	ReasonScaleDownAllowed    = "ScaleDownAllowed"
	ReasonScaleDownBlocked    = "ScaleDownBlocked"
	ReasonScaleDownNoPods     = "ScaleDownNoPods"
	ReasonScaleDownProbeError = "ScaleDownProbeError"
	ReasonGuardSkipped        = "GuardSkipped"
)

// FieldOwner used for server-side apply of operator-owned KSvc objects.
const FieldOwner = "ifaas-autopilot"

// FinalizerRestoreSourceService keeps the KnativeAdoption alive until the
// ServiceSwapper teardown path (S9) can rebuild the pre-adoption Service from
// status.sourceSnapshot.service.
const FinalizerRestoreSourceService = "ifaas.ifbiu.com/restore-source-service"

// AnnoServiceManagedBy marks a Service the operator already adopted in a prior
// reconcile loop, so the swapper does not snapshot/delete it twice.
const (
	AnnoServiceManagedBy      = "ifaas.ifbiu.com/managed-by"
	AnnoServiceManagedByValue = "knative-autopilot"
)
