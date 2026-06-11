# Image URL to use all building/pushing image targets
IMG ?= controller:latest
# YEAR defines the year value used for substituting the YEAR placeholder in the boilerplate header.
YEAR ?= $(shell date +%Y)

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# CONTAINER_TOOL defines the container tool to be used for building images.
# Be aware that the target commands are only tested with Docker which is
# scaffolded by default. However, you might want to replace it to use other
# tools. (i.e. podman)
CONTAINER_TOOL ?= docker

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk command is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: manifests
manifests: controller-gen ## Generate WebhookConfiguration, ClusterRole and CustomResourceDefinition objects.
	"$(CONTROLLER_GEN)" rbac:roleName=manager-role crd webhook paths="./..." output:crd:artifacts:config=config/crd/bases

.PHONY: generate
generate: controller-gen ## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	"$(CONTROLLER_GEN)" object:headerFile="hack/boilerplate.go.txt",year=$(YEAR) paths="./..."

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: test
test: manifests generate fmt vet setup-envtest ## Run tests.
	KUBEBUILDER_ASSETS="$(shell "$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path)" go test $$(go list ./... | grep -v /e2e) -coverprofile cover.out

# E2E runtime knobs:
# - E2E_RUNTIME=k3s   (default) assumes a pre-existing k3s cluster with Knative Serving/Eventing
#                     and cert-manager installed. Image is loaded into k3s containerd via `k3s ctr`.
# - E2E_RUNTIME=kind  legacy path: spins up a kind cluster and `kind load` the image.
# - E2E_RUNTIME=external skips any cluster/image setup; assumes ifaas already deployed.
E2E_RUNTIME      ?= k3s
KIND_CLUSTER     ?= ifaas-test-e2e
K3S_CTR          ?= sudo k3s ctr
E2E_IMG          ?= ifaas:e2e
E2E_STUB_IMG     ?= ifaas-scaledownz-stub:e2e
E2E_NAMESPACE    ?= ifaas-e2e

.PHONY: setup-test-e2e
setup-test-e2e: ## Prepare cluster + images for e2e according to E2E_RUNTIME.
	@case "$(E2E_RUNTIME)" in \
		kind) \
			command -v $(KIND) >/dev/null 2>&1 || { echo "kind not installed"; exit 1; }; \
			case "$$($(KIND) get clusters)" in *"$(KIND_CLUSTER)"*) echo "kind cluster ready";; \
			*) $(KIND) create cluster --name $(KIND_CLUSTER);; esac; \
			$(MAKE) docker-build IMG=$(E2E_IMG); \
			$(MAKE) stub-build  IMG=$(E2E_STUB_IMG); \
			$(KIND) load docker-image $(E2E_IMG)      --name $(KIND_CLUSTER); \
			$(KIND) load docker-image $(E2E_STUB_IMG) --name $(KIND_CLUSTER); \
			$(MAKE) kind-knative-skip-tag-resolving ;; \
		k3s) \
			$(MAKE) docker-build IMG=$(E2E_IMG); \
			$(MAKE) stub-build   IMG=$(E2E_STUB_IMG); \
			$(MAKE) k3s-load     IMG=$(E2E_IMG); \
			$(MAKE) k3s-load     IMG=$(E2E_STUB_IMG) ;; \
		external) \
			echo "E2E_RUNTIME=external: rebuilding ifaas controller image; stub left to user"; \
			$(MAKE) docker-build IMG=$(E2E_IMG); \
			$(MAKE) k3s-load     IMG=$(E2E_IMG) ;; \
		*) echo "unknown E2E_RUNTIME=$(E2E_RUNTIME)"; exit 1 ;; \
	esac
	$(MAKE) install
	$(MAKE) deploy IMG=$(E2E_IMG)
	@echo "force-rolling controller-manager so the new image is picked up (same tag, IfNotPresent)"
	"$(KUBECTL)" -n ifaas-system rollout restart deploy/ifaas-controller-manager
	"$(KUBECTL)" -n ifaas-system rollout status deploy/ifaas-controller-manager --timeout=180s

.PHONY: stub-build
stub-build: ## Build the e2e workload stub image (scaledownz probe + business port).
	CGO_ENABLED=0 GOOS=linux go build -o test/e2e/stub/scaledownz-stub ./test/e2e/stub
	$(CONTAINER_TOOL) build -f test/e2e/stub/Dockerfile -t $(IMG) test/e2e/stub
	rm -f test/e2e/stub/scaledownz-stub

.PHONY: k3s-load
k3s-load: ## Import IMG into k3s built-in containerd (namespace k8s.io).
	@command -v docker >/dev/null 2>&1 || { echo "docker not installed"; exit 1; }
	@tmp=$$(mktemp -t ifaas-img-XXXX.tar); \
		docker save "$(IMG)" -o "$$tmp"; \
		$(K3S_CTR) -n k8s.io images import "$$tmp"; \
		rm -f "$$tmp"

.PHONY: kind-knative-skip-tag-resolving
kind-knative-skip-tag-resolving: ## Tell Knative to skip digest resolution for tags loaded via `kind load`.
	@if ! "$(KUBECTL)" get ns knative-serving >/dev/null 2>&1; then \
		echo "knative-serving namespace not found; install Knative Serving on the kind cluster first."; \
		exit 1; \
	fi
	"$(KUBECTL)" -n knative-serving patch cm config-deployment --type=merge \
		-p '{"data":{"registries-skipping-tag-resolving":"kind.local,ko.local,dev.local,index.docker.io"}}'
	"$(KUBECTL)" -n knative-serving rollout restart deploy/controller
	"$(KUBECTL)" -n knative-serving rollout status  deploy/controller --timeout=120s

.PHONY: test-e2e
test-e2e: manifests generate fmt vet setup-test-e2e ## Run the e2e tests against a real cluster. Pass GINKGO_FLAGS='-ginkgo.focus="..."' to narrow the run.
	E2E_RUNTIME=$(E2E_RUNTIME) E2E_NAMESPACE=$(E2E_NAMESPACE) E2E_IMG=$(E2E_IMG) E2E_STUB_IMG=$(E2E_STUB_IMG) \
		go test -tags=e2e ./test/e2e/... -v -ginkgo.v -timeout=20m $(GINKGO_FLAGS)
	@echo "[hint] run 'make cleanup-test-e2e' to drop the e2e namespace and ifaas deployment"

.PHONY: cleanup-test-e2e
cleanup-test-e2e: ## Tear down e2e leftovers (namespace + ifaas deploy). kind cluster is also removed when E2E_RUNTIME=kind.
	-"$(KUBECTL)" delete ns $(E2E_NAMESPACE) --ignore-not-found --wait=false
	-$(MAKE) undeploy ignore-not-found=true
	-$(MAKE) uninstall ignore-not-found=true
	@if [ "$(E2E_RUNTIME)" = "kind" ]; then $(KIND) delete cluster --name $(KIND_CLUSTER); fi

.PHONY: lint
lint: golangci-lint ## Run golangci-lint linter
	"$(GOLANGCI_LINT)" run

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes
	"$(GOLANGCI_LINT)" run --fix

.PHONY: lint-config
lint-config: golangci-lint ## Verify golangci-lint linter configuration
	"$(GOLANGCI_LINT)" config verify

##@ Build

.PHONY: build
build: manifests generate fmt vet ## Build manager binary.
	go build -o bin/manager cmd/main.go

.PHONY: run
run: manifests generate fmt vet ## Run a controller from your host.
	go run ./cmd/main.go

# If you wish to build the manager image targeting other platforms you can use the --platform flag.
# (i.e. docker build --platform linux/arm64). However, you must enable docker buildKit for it.
# More info: https://docs.docker.com/develop/develop-images/build_enhancements/
.PHONY: docker-build
docker-build: ## Build docker image with the manager.
	$(CONTAINER_TOOL) build -t ${IMG} .

.PHONY: docker-push
docker-push: ## Push docker image with the manager.
	$(CONTAINER_TOOL) push ${IMG}

# PLATFORMS defines the target platforms for the manager image be built to provide support to multiple
# architectures. (i.e. make docker-buildx IMG=myregistry/mypoperator:0.0.1). To use this option you need to:
# - be able to use docker buildx. More info: https://docs.docker.com/build/buildx/
# - have enabled BuildKit. More info: https://docs.docker.com/develop/develop-images/build_enhancements/
# - be able to push the image to your registry (i.e. if you do not set a valid value via IMG=<myregistry/image:<tag>> then the export will fail)
# To adequately provide solutions that are compatible with multiple platforms, you should consider using this option.
PLATFORMS ?= linux/arm64,linux/amd64,linux/s390x,linux/ppc64le
.PHONY: docker-buildx
docker-buildx: ## Build and push docker image for the manager for cross-platform support
	# copy existing Dockerfile and insert --platform=${BUILDPLATFORM} into Dockerfile.cross, and preserve the original Dockerfile
	sed -e '1 s/\(^FROM\)/FROM --platform=\$$\{BUILDPLATFORM\}/; t' -e ' 1,// s//FROM --platform=\$$\{BUILDPLATFORM\}/' Dockerfile > Dockerfile.cross
	- $(CONTAINER_TOOL) buildx create --name ifaas-builder
	$(CONTAINER_TOOL) buildx use ifaas-builder
	- $(CONTAINER_TOOL) buildx build --push --platform=$(PLATFORMS) --tag ${IMG} -f Dockerfile.cross .
	- $(CONTAINER_TOOL) buildx rm ifaas-builder
	rm Dockerfile.cross

.PHONY: build-installer
build-installer: manifests generate kustomize ## Generate a consolidated YAML with CRDs and deployment.
	mkdir -p dist
	cd config/manager && "$(KUSTOMIZE)" edit set image controller=${IMG}
	"$(KUSTOMIZE)" build config/default > dist/install.yaml

##@ Deployment

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: install
install: manifests kustomize ## Install CRDs into the K8s cluster specified in ~/.kube/config.
	@out="$$( "$(KUSTOMIZE)" build config/crd 2>/dev/null || true )"; \
	if [ -n "$$out" ]; then echo "$$out" | "$(KUBECTL)" apply -f -; else echo "No CRDs to install; skipping."; fi

.PHONY: uninstall
uninstall: manifests kustomize ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	@out="$$( "$(KUSTOMIZE)" build config/crd 2>/dev/null || true )"; \
	if [ -n "$$out" ]; then echo "$$out" | "$(KUBECTL)" delete --ignore-not-found=$(ignore-not-found) -f -; else echo "No CRDs to delete; skipping."; fi

.PHONY: deploy
deploy: manifests kustomize ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	cd config/manager && "$(KUSTOMIZE)" edit set image controller=${IMG}
	"$(KUSTOMIZE)" build config/default | "$(KUBECTL)" apply -f -

.PHONY: undeploy
undeploy: kustomize ## Undeploy controller from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	"$(KUSTOMIZE)" build config/default | "$(KUBECTL)" delete --ignore-not-found=$(ignore-not-found) -f -

##@ Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p "$(LOCALBIN)"

## Tool Binaries
KUBECTL ?= kubectl
KIND ?= kind
KUSTOMIZE ?= $(LOCALBIN)/kustomize
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT = $(LOCALBIN)/golangci-lint

## Tool Versions
KUSTOMIZE_VERSION ?= v5.8.1
CONTROLLER_TOOLS_VERSION ?= v0.21.0

#ENVTEST_VERSION pins the setup-envtest CLI. setup-envtest is published from
#its own go module under sigs.k8s.io/controller-runtime/tools/setup-envtest
#and tagged independently from controller-runtime; deriving it from the
#controller-runtime version in go.mod yields a tag that does not exist as a
#submodule release. Use a known-good branch tag here and override via
#environment variable when chasing a newer release.
ENVTEST_VERSION ?= release-0.21

#ENVTEST_K8S_VERSION is the version of Kubernetes to use for setting up ENVTEST binaries (i.e. 1.31)
ENVTEST_K8S_VERSION ?= $(shell v='$(call gomodver,k8s.io/api)'; \
  [ -n "$$v" ] || { echo "Set ENVTEST_K8S_VERSION manually (k8s.io/api replace has no tag)" >&2; exit 1; }; \
  printf '%s\n' "$$v" | sed -E 's/^v?[0-9]+\.([0-9]+).*/1.\1/')

GOLANGCI_LINT_VERSION ?= v2.12.2
.PHONY: kustomize
kustomize: $(KUSTOMIZE) ## Download kustomize locally if necessary.
$(KUSTOMIZE): $(LOCALBIN)
	$(call go-install-tool,$(KUSTOMIZE),sigs.k8s.io/kustomize/kustomize/v5,$(KUSTOMIZE_VERSION))

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(LOCALBIN)
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen,$(CONTROLLER_TOOLS_VERSION))

.PHONY: setup-envtest
setup-envtest: envtest ## Download the binaries required for ENVTEST in the local bin directory.
	@echo "Setting up envtest binaries for Kubernetes version $(ENVTEST_K8S_VERSION)..."
	@"$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path || { \
		echo "Error: Failed to set up envtest binaries for version $(ENVTEST_K8S_VERSION)."; \
		exit 1; \
	}

.PHONY: envtest
envtest: $(ENVTEST) ## Download setup-envtest locally if necessary.
$(ENVTEST): $(LOCALBIN)
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))
	@test -f .custom-gcl.yml && { \
		echo "Building custom golangci-lint with plugins..." && \
		$(GOLANGCI_LINT) custom --destination $(LOCALBIN) --name golangci-lint-custom && \
		mv -f $(LOCALBIN)/golangci-lint-custom $(GOLANGCI_LINT); \
	} || true

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f "$(1)-$(3)" ] && [ "$$(readlink -- "$(1)" 2>/dev/null)" = "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f "$(1)" ;\
GOBIN="$(LOCALBIN)" go install $${package} ;\
mv "$(LOCALBIN)/$$(basename "$(1)")" "$(1)-$(3)" ;\
} ;\
ln -sf "$$(realpath "$(1)-$(3)")" "$(1)"
endef

define gomodver
$(shell go list -m -f '{{if .Replace}}{{.Replace.Version}}{{else}}{{.Version}}{{end}}' $(1) 2>/dev/null)
endef
