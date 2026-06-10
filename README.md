# ifaas

[![Repo](https://img.shields.io/badge/github-ifbiu%2Fifaas-1f6feb?logo=github)](https://github.com/ifbiu/ifaas)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](#license)

Kubernetes operator that turns ordinary `Deployment`-based workloads into
[Knative Serving](https://knative.dev/) revisions with cooperative
scale-to-zero, without forcing application teams to rewrite their manifests.

- 项目地址：<https://github.com/ifbiu/ifaas>
- 维护者：Candide
- Go module：`github.com/ifbiu/ifaas`
- API group / Kind：`ifaas.ifbiu.com/v1alpha1` · `KnativeAdoption`

## Description

`ifaas` (the **i**fbiu **F**unction-**as**-a-**S**ervice operator) automates the
adoption of existing applications into a serverless runtime:

1. A user labels a `Deployment` with `ifaas.ifbiu.com/knative-autopilot=enabled`,
   or hand-writes a `KnativeAdoption` CR.
2. The operator translates the `Deployment` (image, env, ports, volumes,
   resources, scheduling) into an equivalent `knative.dev/Service`.
3. The original `Deployment` is scaled to zero and kept as a ledger so the
   adoption is fully reversible.
4. A per-pod `/scaledownz` HTTP probe gates scale-to-zero: every active pod
   must vote `true` before the operator allows `min-scale=0`. Any pod voting
   `false` (or failing to answer) pins `min-scale=1`.
5. The CR's `status.conditions` surface the live state — `Adopted`,
   `ServiceAdopted`, `ScaleDownAllowed`, `SourceQuiesced`, `Degraded`,
   `Ready` — so platform teams can drive dashboards and alerts off a single
   object.

Design and implementation plan live under `../docs/`:

- `docs/knative-autopilot-design.md` — decisions archive
- `docs/knative-autopilot-impl-plan.md` — milestone-by-milestone plan (S1-S11)

## Status

M1 is being built milestone-by-milestone. Current progress:

| # | Milestone | State |
|---|---|---|
| S1 | `KnativeAdoption` types + sample CR | done |
| S2 | Translator (`Deployment` → `KService`) pure functions | done |
| S3 | `KnativeAdoptionReconciler` MVP (apply KSvc + scale source to 0) | done |
| S4 | `DeploymentWatcher` (label → CR auto-adopt) | done |
| S5 | `ServiceSwapper` (same-name `core/Service` takeover + finalizer) | done |
| S6 | `ScaleDownGuard` (single-CR `/scaledownz` voting) | done |
| S7 | Namespace flusher (debounced `min-scale` PATCH coalescing) | pending |
| S8 | Validating webhook (`KnativeAdoption` + `Deployment`) | pending |
| S9 | Restore chain (replicas + `Service` spec rollback finalizers) | pending |
| S10 | Metrics + events + full Conditions catalogue | pending |
| S11 | e2e (`kind` + Knative Serving) | pending |

## Getting Started

### Prerequisites

- Go 1.24.6+
- Docker 17.03+ (or any OCI builder)
- `kubectl` 1.11.3+
- A Kubernetes cluster (1.21+ recommended) with [Knative Serving](https://knative.dev/docs/install/) installed
- `kubebuilder` v4.x (only required for code generation / scaffolding)

### Build & deploy

**Build and push the controller image:**

```sh
make docker-build docker-push IMG=<some-registry>/ifaas:tag
```

The image must be reachable from the target cluster; configure registry pull
secrets accordingly.

**Install the CRDs:**

```sh
make install
```

**Deploy the controller:**

```sh
make deploy IMG=<some-registry>/ifaas:tag
```

> If you hit RBAC errors, you need cluster-admin (the controller installs a
> `ClusterRole` for `pods/proxy`, `serving.knative.dev/services` etc.).

**Apply a sample `KnativeAdoption`:**

```sh
kubectl apply -k config/samples/
```

> The sample under `config/samples/ifaas_v1alpha1_knativeadoption.yaml`
> assumes a `Deployment` of the same name already exists in the namespace.

### Tearing down

```sh
kubectl delete -k config/samples/
make uninstall    # remove CRDs
make undeploy     # remove the controller
```

### Local development

```sh
make manifests generate   # (re)generate CRDs and DeepCopy
make fmt vet              # standard hygiene
make test                 # envtest-backed unit + controller tests
make run                  # run the controller against the current kubecontext
```

`make test` provisions envtest binaries under `bin/k8s/<version>` and pulls
in the hand-written Knative `Service` CRD from `test/crds/knative/` so the
reconciler's SSA path can be exercised without a real Knative installation.

## Project distribution

### Bundled YAML installer

```sh
make build-installer IMG=<some-registry>/ifaas:tag
```

The bundle is written to `dist/install.yaml`. Users can install with:

```sh
kubectl apply -f https://raw.githubusercontent.com/ifbiu/ifaas/<tag-or-branch>/dist/install.yaml
```

### Helm chart (optional)

```sh
kubebuilder edit --plugins=helm/v2-alpha
```

The generated chart lives under `dist/chart`. Re-run the command after
controller-side changes to keep the chart in sync; pass `--force` and
re-merge `dist/chart/values.yaml` if you have local customisations.

## Contributing

External contributions are welcome. Practical guidelines:

1. Open an issue first for anything beyond a focused bug-fix; the design
   archive in `docs/knative-autopilot-design.md` exists to surface trade-offs
   before code lands.
2. Stay inside the milestone slicing in `docs/knative-autopilot-impl-plan.md`.
   New behaviour belongs in its designated step (or a new step appended to
   the plan), not opportunistically dropped into an unrelated PR.
3. Every code change must come with `make manifests generate fmt vet test`
   green; controller behaviour must come with envtest coverage in
   `internal/controller/ifaas/`.
4. Pure functions (translator, vote logic, projector) live under `internal/`
   without controller-runtime dependencies and are exercised by table-driven
   unit tests — keep them that way.
5. Conditions, reasons, finalizers and annotations are public API surface.
   Document additions in `docs/` and update the Conditions catalogue in S10
   when you introduce new ones.

Run `make help` for the full target list.

More on the Kubebuilder layout:
[Kubebuilder Documentation](https://book.kubebuilder.io/introduction.html).

## License

Copyright 2026 Candide.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.