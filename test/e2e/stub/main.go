// Package main is the e2e stub that doubles as the workload image.
//
// It serves two HTTP endpoints on different ports so a single image can
// represent both the business application *and* the /scaledownz probe
// the operator relies on:
//
//   - 8080 /         → 200 "ok\n" (the request the user's traffic hits)
//   - 8081 /scaledownz → JSON {"allow":bool} with bool driven by the
//     ALLOW_SCALEDOWN env var (default: "false")
//
// The probe endpoint listens on a separate port so the Knative Service
// keeps a single user-facing port (8080) while the operator continues to
// reach the probe via pods/proxy on 8081, matching the design in
// docs/knative-autopilot-design.md §ScaleDownGuard.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
)

func main() {
	bizPort := envDefault("BIZ_PORT", "8080")
	probePort := envDefault("PROBE_PORT", "8081")

	go runBiz(bizPort)
	runProbe(probePort)
}

func runBiz(port string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintln(w, "ok")
	})
	log.Printf("biz: listening on :%s", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatalf("biz listen: %v", err)
	}
}

func runProbe(port string) {
	var hits atomic.Uint64

	mux := http.NewServeMux()
	mux.HandleFunc("/scaledownz", func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		allow := readAllow()
		w.Header().Set("Content-Type", "application/json")
		body, _ := json.Marshal(map[string]any{
			"allow": allow,
			"hits":  hits.Load(),
		})
		_, _ = w.Write(body)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintln(w, "ok")
	})
	log.Printf("probe: listening on :%s", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatalf("probe listen: %v", err)
	}
}

func readAllow() bool {
	// e2e tests flip /etc/scaledownz/allow via a ConfigMap mount when the
	// ALLOW_FILE env points at it; falling back to ALLOW_SCALEDOWN keeps
	// container-level overrides cheap when no ConfigMap is wired up.
	if path := os.Getenv("ALLOW_FILE"); path != "" {
		if b, err := os.ReadFile(path); err == nil {
			return parseBool(string(b))
		}
	}
	return parseBool(os.Getenv("ALLOW_SCALEDOWN"))
}

func parseBool(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

func envDefault(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
