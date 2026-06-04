package main

import (
	"os"
	"testing"
	"time"
)

func TestBoolEnvDefaultOn(t *testing.T) {
	t.Setenv("CHECK_TEST_ENABLED", "")
	if !boolEnvDefaultOn("CHECK_TEST_ENABLED") {
		t.Fatal("empty env should default to enabled")
	}

	t.Setenv("CHECK_TEST_ENABLED", "false")
	if boolEnvDefaultOn("CHECK_TEST_ENABLED") {
		t.Fatal("false should disable the check")
	}

	t.Setenv("CHECK_TEST_ENABLED", "disabled")
	if boolEnvDefaultOn("CHECK_TEST_ENABLED") {
		t.Fatal("disabled should disable the check")
	}

	t.Setenv("CHECK_TEST_ENABLED", "1")
	if !boolEnvDefaultOn("CHECK_TEST_ENABLED") {
		t.Fatal("1 should enable the check")
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	clearConfigEnv(t)
	cfg := LoadConfig()

	if cfg.Namespace != "default" {
		t.Fatalf("namespace = %q, want default", cfg.Namespace)
	}
	if cfg.PollInterval != 60*time.Second {
		t.Fatalf("poll interval = %s, want 60s", cfg.PollInterval)
	}
	if cfg.LogScanLines != 100 {
		t.Fatalf("log scan lines = %d, want 100", cfg.LogScanLines)
	}
	if !cfg.Checks.Pods || !cfg.Checks.Events || !cfg.Checks.ResourceUsage {
		t.Fatal("core checks should default to enabled")
	}
	if cfg.FindingsTopic != "k8s.namespace.findings" {
		t.Fatalf("findings topic = %q, want generic k8s.namespace.findings default", cfg.FindingsTopic)
	}
	if cfg.RedpandaBroker != "localhost:9092" {
		t.Fatalf("redpanda broker = %q, want generic localhost:9092 default", cfg.RedpandaBroker)
	}
	if cfg.PublishTimeout != 10*time.Second {
		t.Fatalf("publish timeout = %s, want 10s", cfg.PublishTimeout)
	}
	if cfg.CheckTimeout != 30*time.Second {
		t.Fatalf("check timeout = %s, want 30s", cfg.CheckTimeout)
	}
}

func TestLoadConfigCheckTimeout(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("CHECK_TIMEOUT", "250ms")

	cfg := LoadConfig()

	if cfg.CheckTimeout != 250*time.Millisecond {
		t.Fatalf("check timeout = %s, want 250ms", cfg.CheckTimeout)
	}
}

func TestResourceBackendDisabledDisablesUsageCheck(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("RESOURCE_USAGE_BACKEND", "disabled")
	cfg := LoadConfig()
	if cfg.Checks.ResourceUsage {
		t.Fatal("disabled resource backend should disable resource usage checks")
	}
}

func clearConfigEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"WATCH_NAMESPACE",
		"POD_NAMESPACE",
		"POLL_INTERVAL",
		"LOG_SCAN_LINES",
		"CHECK_PODS_ENABLED",
		"CHECK_EVENTS_ENABLED",
		"CHECK_RESOURCE_USAGE_ENABLED",
		"RESOURCE_USAGE_BACKEND",
		"REDPANDA_BROKER",
		"FINDINGS_TOPIC",
		"PUBLISH_TIMEOUT",
		"CHECK_TIMEOUT",
		"STATE_CONFIGMAP_NAME",
		"STATE_WRITE_INTERVAL",
		"RESOLVED_FINDING_RETENTION",
		"AI_TRIAGE_ENABLED",
		"AI_MIN_SCORE",
		"AI_COOLDOWN",
		"AI_COOLDOWN_INCIDENT",
		"MEMORY_WARNING_PERCENT",
		"MEMORY_CRITICAL_PERCENT",
		"RESTART_WARNING_COUNT",
		"RESTART_WINDOW",
		"EVENT_LOOKBACK",
		"SUPPRESS_CONFIGMAP",
		"CHECK_LOGS_ENABLED",
		"CHECK_WORKLOADS_ENABLED",
		"CHECK_SCALING_ENABLED",
		"CHECK_RESOURCE_SPECS_ENABLED",
		"CHECK_CUSTOM_RESOURCES_ENABLED",
		"CHECK_SERVICES_ENABLED",
		"CHECK_PVCS_ENABLED",
	} {
		t.Setenv(key, "")
	}
	_ = os.Unsetenv("WATCH_NAMESPACE")
	_ = os.Unsetenv("POD_NAMESPACE")
}
