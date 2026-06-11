/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package utils contains test-only helpers shared by the e2e suite.
//
// Scope is intentionally narrow: it knows how to (1) build typed and
// dynamic clients against the kubeconfig the developer/CI is already
// using, (2) create per-spec ephemeral namespaces and tear them down,
// and (3) reach Knative Serving objects via unstructured GVRs without
// dragging the full knative.dev module tree into go.mod. Anything more
// opinionated lives next to the test that needs it.
package utils

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

// Knative Serving GVRs reused by the e2e specs.
var (
	GVRService = schema.GroupVersionResource{
		Group:    "serving.knative.dev",
		Version:  "v1",
		Resource: "services",
	}
	GVRRevision = schema.GroupVersionResource{
		Group:    "serving.knative.dev",
		Version:  "v1",
		Resource: "revisions",
	}
	GVRTrigger = schema.GroupVersionResource{
		Group:    "eventing.knative.dev",
		Version:  "v1",
		Resource: "triggers",
	}
)

// ifaas CR GVR (kept here so the e2e suite avoids importing the api/
// package, which pulls in scheme registration noise).
var GVRKnativeAdoption = schema.GroupVersionResource{
	Group:    "ifaas.ifbiu.com",
	Version:  "v1alpha1",
	Resource: "knativeadoptions",
}

// Clients bundles the three flavours of client the e2e tests reach for.
type Clients struct {
	Cfg     *rest.Config
	Typed   *kubernetes.Clientset
	Dynamic dynamic.Interface
	Ctrl    client.Client
}

// NewClients resolves a kubeconfig (KUBECONFIG > $HOME/.kube/config >
// in-cluster) and returns the bundle. Any failure here is fatal for the
// suite; the caller is expected to assert against this in BeforeSuite.
func NewClients() (*Clients, error) {
	cfg, err := loadConfig()
	if err != nil {
		return nil, fmt.Errorf("load rest.Config: %w", err)
	}
	typed, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("new typed client: %w", err)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("new dynamic client: %w", err)
	}
	c, err := client.New(cfg, client.Options{})
	if err != nil {
		return nil, fmt.Errorf("new ctrl client: %w", err)
	}
	return &Clients{Cfg: cfg, Typed: typed, Dynamic: dyn, Ctrl: c}, nil
}

func loadConfig() (*rest.Config, error) {
	// Honour what controller-runtime would pick: KUBECONFIG env, then the
	// usual locations. Falls back to in-cluster credentials when present.
	if cfg, err := config.GetConfig(); err == nil {
		return cfg, nil
	}
	if home, err := os.UserHomeDir(); err == nil {
		path := filepath.Join(home, ".kube", "config")
		return clientcmd.BuildConfigFromFlags("", path)
	}
	return rest.InClusterConfig()
}

// CreateNamespace materialises an empty namespace and returns it. Names
// are caller-supplied because Ginkgo specs derive them from spec text;
// the helper is idempotent against AlreadyExists so retried suites do
// not flake on leftover state.
func CreateNamespace(ctx context.Context, t *kubernetes.Clientset, name string) (*corev1.Namespace, error) {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	out, err := t.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return nil, err
	}
	if apierrors.IsAlreadyExists(err) {
		return t.CoreV1().Namespaces().Get(ctx, name, metav1.GetOptions{})
	}
	return out, nil
}

// DeleteNamespace removes the namespace and waits for it to disappear,
// up to timeout. Hard failures are returned; "not found" counts as
// success because that is the post-condition we care about.
func DeleteNamespace(ctx context.Context, t *kubernetes.Clientset, name string, timeout time.Duration) error {
	if err := t.CoreV1().Namespaces().Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, err := t.CoreV1().Namespaces().Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("namespace %q not deleted within %s", name, timeout)
}
