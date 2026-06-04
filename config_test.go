package main

import (
	"os"
	"strings"
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
	if cfg.CustomResourceDiscoveryTTL != 10*time.Minute {
		t.Fatalf("custom resource discovery ttl = %s, want 10m", cfg.CustomResourceDiscoveryTTL)
	}
	if cfg.ObservationRetention != 24*time.Hour {
		t.Fatalf("observation retention = %s, want 24h", cfg.ObservationRetention)
	}
	if cfg.MaxObservations != 5000 {
		t.Fatalf("max observations = %d, want 5000", cfg.MaxObservations)
	}
	if cfg.MaxFindings != 2000 {
		t.Fatalf("max findings = %d, want 2000", cfg.MaxFindings)
	}
	if cfg.MaxStateBytes != 900*1024 {
		t.Fatalf("max state bytes = %d, want %d", cfg.MaxStateBytes, 900*1024)
	}
	if len(cfg.CustomResourceAllowlist) != 0 || len(cfg.CustomResourceExcludelist) != 0 {
		t.Fatalf("custom resource lists should default empty, got allow=%v exclude=%v", cfg.CustomResourceAllowlist, cfg.CustomResourceExcludelist)
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

func TestLoadConfigCustomResourceControls(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("CUSTOM_RESOURCE_ALLOWLIST", "source.toolkit.fluxcd.io, helm.toolkit.fluxcd.io/helmreleases ")
	t.Setenv("CUSTOM_RESOURCE_EXCLUDELIST", " noisy.example.com/widgets ")
	t.Setenv("CUSTOM_RESOURCE_DISCOVERY_TTL", "2m")

	cfg := LoadConfig()
	if cfg.CustomResourceDiscoveryTTL != 2*time.Minute {
		t.Fatalf("custom resource discovery ttl = %s, want 2m", cfg.CustomResourceDiscoveryTTL)
	}
	if got, want := strings.Join(cfg.CustomResourceAllowlist, ","), "source.toolkit.fluxcd.io,helm.toolkit.fluxcd.io/helmreleases"; got != want {
		t.Fatalf("allowlist = %q, want %q", got, want)
	}
	if got, want := strings.Join(cfg.CustomResourceExcludelist, ","), "noisy.example.com/widgets"; got != want {
		t.Fatalf("excludelist = %q, want %q", got, want)
	}
}

func TestLoadConfigCustomResourceGroupAliases(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("CUSTOM_RESOURCE_GROUPS", "source.toolkit.fluxcd.io")
	t.Setenv("CUSTOM_RESOURCE_EXCLUDE_GROUPS", "events.k8s.io")

	cfg := LoadConfig()
	if got, want := strings.Join(cfg.CustomResourceAllowlist, ","), "source.toolkit.fluxcd.io"; got != want {
		t.Fatalf("allowlist alias = %q, want %q", got, want)
	}
	if got, want := strings.Join(cfg.CustomResourceExcludelist, ","), "events.k8s.io"; got != want {
		t.Fatalf("excludelist alias = %q, want %q", got, want)
	}
}

func TestLoadConfigDownstreamTriageDefaults(t *testing.T) {
	clearConfigEnv(t)
	cfg := LoadConfig()
	if !cfg.DownstreamTriageEnabled {
		t.Fatal("downstream triage should default to enabled")
	}
	if cfg.DownstreamMinScore != 60 {
		t.Fatalf("downstream min score = %d, want 60", cfg.DownstreamMinScore)
	}
	if cfg.DownstreamCooldown != 30*time.Minute {
		t.Fatalf("downstream cooldown = %s, want 30m", cfg.DownstreamCooldown)
	}
	if cfg.DownstreamCooldownIncident != 10*time.Minute {
		t.Fatalf("downstream cooldown incident = %s, want 10m", cfg.DownstreamCooldownIncident)
	}
}

func TestLoadConfigDownstreamTriageExplicitEnv(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("DOWNSTREAM_TRIAGE_ENABLED", "false")
	t.Setenv("DOWNSTREAM_MIN_SCORE", "75")
	t.Setenv("DOWNSTREAM_COOLDOWN", "45m")
	t.Setenv("DOWNSTREAM_COOLDOWN_INCIDENT", "5m")

	cfg := LoadConfig()
	if cfg.DownstreamTriageEnabled {
		t.Fatal("expected DOWNSTREAM_TRIAGE_ENABLED=false to disable triage")
	}
	if cfg.DownstreamMinScore != 75 {
		t.Fatalf("downstream min score = %d, want 75", cfg.DownstreamMinScore)
	}
	if cfg.DownstreamCooldown != 45*time.Minute {
		t.Fatalf("downstream cooldown = %s, want 45m", cfg.DownstreamCooldown)
	}
	if cfg.DownstreamCooldownIncident != 5*time.Minute {
		t.Fatalf("downstream cooldown incident = %s, want 5m", cfg.DownstreamCooldownIncident)
	}
}

func TestLoadConfigDownstreamTriageDeprecatedAlias(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("AI_TRIAGE_ENABLED", "false")
	t.Setenv("AI_MIN_SCORE", "42")
	t.Setenv("AI_COOLDOWN", "15m")
	t.Setenv("AI_COOLDOWN_INCIDENT", "3m")

	cfg := LoadConfig()
	if cfg.DownstreamTriageEnabled {
		t.Fatal("expected AI_TRIAGE_ENABLED=false to disable downstream triage via deprecated alias")
	}
	if cfg.DownstreamMinScore != 42 {
		t.Fatalf("downstream min score from deprecated alias = %d, want 42", cfg.DownstreamMinScore)
	}
	if cfg.DownstreamCooldown != 15*time.Minute {
		t.Fatalf("downstream cooldown from deprecated alias = %s, want 15m", cfg.DownstreamCooldown)
	}
	if cfg.DownstreamCooldownIncident != 3*time.Minute {
		t.Fatalf("downstream cooldown incident from deprecated alias = %s, want 3m", cfg.DownstreamCooldownIncident)
	}
}

func TestLoadConfigDownstreamTriageNewOverridesDeprecated(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("DOWNSTREAM_TRIAGE_ENABLED", "true")
	t.Setenv("AI_TRIAGE_ENABLED", "false")
	t.Setenv("DOWNSTREAM_MIN_SCORE", "88")
	t.Setenv("AI_MIN_SCORE", "11")

	cfg := LoadConfig()
	if !cfg.DownstreamTriageEnabled {
		t.Fatal("expected new env var to win over deprecated alias")
	}
	if cfg.DownstreamMinScore != 88 {
		t.Fatalf("downstream min score = %d, want 88 (new env should win over deprecated)", cfg.DownstreamMinScore)
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
		"OBSERVATION_RETENTION",
		"MAX_OBSERVATIONS",
		"MAX_FINDINGS",
		"MAX_STATE_BYTES",
		"DOWNSTREAM_TRIAGE_ENABLED",
		"DOWNSTREAM_MIN_SCORE",
		"DOWNSTREAM_COOLDOWN",
		"DOWNSTREAM_COOLDOWN_INCIDENT",
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
		"CUSTOM_RESOURCE_ALLOWLIST",
		"CUSTOM_RESOURCE_GROUPS",
		"CUSTOM_RESOURCE_EXCLUDELIST",
		"CUSTOM_RESOURCE_EXCLUDE_GROUPS",
		"CUSTOM_RESOURCE_DISCOVERY_TTL",
	} {
		t.Setenv(key, "")
	}
	_ = os.Unsetenv("WATCH_NAMESPACE")
	_ = os.Unsetenv("POD_NAMESPACE")
}
