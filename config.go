package main

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type CheckConfig struct {
	Pods           bool
	Events         bool
	Logs           bool
	Workloads      bool
	Scaling        bool
	ResourceUsage  bool
	ResourceSpecs  bool
	CustomResource bool
	Services       bool
	PVCs           bool
}

type Config struct {
	Namespace                    string
	PollInterval                 time.Duration
	MetricsPort                  string
	RedpandaBroker               string
	FindingsTopic                string
	PublishTimeout               time.Duration
	CheckTimeout                 time.Duration
	StateConfigMapName           string
	StateWriteInterval           time.Duration
	ResolvedFindingRetention     time.Duration
	ObservationRetention         time.Duration
	MaxObservations              int
	MaxFindings                  int
	MaxStateBytes                int
	DownstreamTriageEnabled      bool
	DownstreamMinScore           int
	DownstreamCooldown           time.Duration
	DownstreamCooldownIncident   time.Duration
	LogScanLines                 int
	ResourceUsageBackend         string
	MemoryWarningPercent         int
	MemoryCriticalPercent        int
	RestartWarningCount          int32
	RestartWindow                time.Duration
	EventLookback                time.Duration
	SuppressConfigMapName        string
	CustomResourceAllowlist      []string
	CustomResourceExcludelist    []string
	CustomResourceDiscoveryTTL   time.Duration
	// HealthMaxPollGap is the maximum allowed duration between completed polls
	// before /healthz returns 503. When zero, the default formula is used:
	// max(3 * PollInterval, PollInterval + CheckTimeout).
	HealthMaxPollGap             time.Duration
	Checks                       CheckConfig
}

func LoadConfig() Config {
	resourceBackend := env("RESOURCE_USAGE_BACKEND", "metrics-server")
	resourceUsageEnabled := boolEnvDefaultOn("CHECK_RESOURCE_USAGE_ENABLED")
	if strings.EqualFold(resourceBackend, "disabled") {
		resourceUsageEnabled = false
	}

	return Config{
		Namespace:                  namespace(),
		PollInterval:               durationEnv("POLL_INTERVAL", 60*time.Second),
		MetricsPort:                env("METRICS_PORT", "8080"),
		RedpandaBroker:             env("REDPANDA_BROKER", "localhost:9092"),
		FindingsTopic:              env("FINDINGS_TOPIC", "k8s.namespace.findings"),
		PublishTimeout:             durationEnv("PUBLISH_TIMEOUT", 10*time.Second),
		CheckTimeout:               durationEnv("CHECK_TIMEOUT", 30*time.Second),
		StateConfigMapName:         env("STATE_CONFIGMAP_NAME", "autonomous-monitor-state"),
		StateWriteInterval:         durationEnv("STATE_WRITE_INTERVAL", 60*time.Second),
		ResolvedFindingRetention:   durationEnv("RESOLVED_FINDING_RETENTION", 24*time.Hour),
		ObservationRetention:       durationEnv("OBSERVATION_RETENTION", 24*time.Hour),
		MaxObservations:            intEnv("MAX_OBSERVATIONS", 5000),
		MaxFindings:                intEnv("MAX_FINDINGS", 2000),
		MaxStateBytes:              intEnv("MAX_STATE_BYTES", 900*1024),
		DownstreamTriageEnabled:    downstreamTriageEnabled(),
		DownstreamMinScore:         downstreamMinScore(),
		DownstreamCooldown:         durationEnv("DOWNSTREAM_COOLDOWN", durationEnv("AI_COOLDOWN", 30*time.Minute)),
		DownstreamCooldownIncident: durationEnv("DOWNSTREAM_COOLDOWN_INCIDENT", durationEnv("AI_COOLDOWN_INCIDENT", 10*time.Minute)),
		SuppressConfigMapName:      env("SUPPRESS_CONFIGMAP", ""),
		LogScanLines:               intEnv("LOG_SCAN_LINES", 100),
		ResourceUsageBackend:       resourceBackend,
		MemoryWarningPercent:       intEnv("MEMORY_WARNING_PERCENT", 80),
		MemoryCriticalPercent:      intEnv("MEMORY_CRITICAL_PERCENT", 90),
		RestartWarningCount:        clampInt32(intEnv("RESTART_WARNING_COUNT", 3), 1, 1000),
		RestartWindow:              durationEnv("RESTART_WINDOW", 10*time.Minute),
		EventLookback:              durationEnv("EVENT_LOOKBACK", 30*time.Minute),
		CustomResourceAllowlist:    listEnv("CUSTOM_RESOURCE_ALLOWLIST", "CUSTOM_RESOURCE_GROUPS"),
		CustomResourceExcludelist:  listEnv("CUSTOM_RESOURCE_EXCLUDELIST", "CUSTOM_RESOURCE_EXCLUDE_GROUPS"),
		CustomResourceDiscoveryTTL: durationEnv("CUSTOM_RESOURCE_DISCOVERY_TTL", 10*time.Minute),
		HealthMaxPollGap:           durationEnv("HEALTH_MAX_POLL_GAP", 0),
		Checks: CheckConfig{
			Pods:           boolEnvDefaultOn("CHECK_PODS_ENABLED"),
			Events:         boolEnvDefaultOn("CHECK_EVENTS_ENABLED"),
			Logs:           boolEnvDefaultOn("CHECK_LOGS_ENABLED"),
			Workloads:      boolEnvDefaultOn("CHECK_WORKLOADS_ENABLED"),
			Scaling:        boolEnvDefaultOn("CHECK_SCALING_ENABLED"),
			ResourceUsage:  resourceUsageEnabled,
			ResourceSpecs:  boolEnvDefaultOn("CHECK_RESOURCE_SPECS_ENABLED"),
			CustomResource: boolEnvDefaultOn("CHECK_CUSTOM_RESOURCES_ENABLED"),
			Services:       boolEnvDefaultOn("CHECK_SERVICES_ENABLED"),
			PVCs:           boolEnvDefaultOn("CHECK_PVCS_ENABLED"),
		},
	}
}

// downstreamTriageEnabled reads DOWNSTREAM_TRIAGE_ENABLED and falls back to the
// deprecated AI_TRIAGE_ENABLED alias. The monitor never calls a model itself —
// it sets ai_triage_required on findings so a downstream consumer can dispatch
// triage. The env var name was renamed to reflect that. The legacy name still
// works to keep existing deployments working, with a one-shot deprecation log
// emitted on first read.
func downstreamTriageEnabled() bool {
	if _, ok := lookupEnv("DOWNSTREAM_TRIAGE_ENABLED"); ok {
		return boolEnvDefaultOn("DOWNSTREAM_TRIAGE_ENABLED")
	}
	if _, ok := lookupEnv("AI_TRIAGE_ENABLED"); ok {
		warnDeprecated("AI_TRIAGE_ENABLED", "DOWNSTREAM_TRIAGE_ENABLED")
		return boolEnvDefaultOn("AI_TRIAGE_ENABLED")
	}
	return true
}

// downstreamMinScore mirrors downstreamTriageEnabled for the score threshold.
func downstreamMinScore() int {
	if _, ok := lookupEnv("DOWNSTREAM_MIN_SCORE"); ok {
		return intEnv("DOWNSTREAM_MIN_SCORE", 60)
	}
	if _, ok := lookupEnv("AI_MIN_SCORE"); ok {
		warnDeprecated("AI_MIN_SCORE", "DOWNSTREAM_MIN_SCORE")
		return intEnv("AI_MIN_SCORE", 60)
	}
	return 60
}

// lookupEnv returns (value, true) if the env var is set to a non-empty
// string, mirroring the existing env() / intEnv() / durationEnv() helpers.
// Setting an env var to "" is treated identically to leaving it unset, so
// the codebase's "empty means default" convention is preserved.
func lookupEnv(key string) (string, bool) {
	v := os.Getenv(key)
	if v == "" {
		return "", false
	}
	return v, true
}

func warnDeprecated(oldName, newName string) {
	log.Printf("WARN: %s is deprecated and will be removed in v1.0.0; use %s instead", oldName, newName)
}

func namespace() string {
	if v := os.Getenv("WATCH_NAMESPACE"); v != "" {
		return v
	}
	if v := os.Getenv("POD_NAMESPACE"); v != "" {
		return v
	}
	return "default"
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func boolEnvDefaultOn(key string) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	switch v {
	case "", "1", "true", "yes", "on", "enabled":
		return true
	case "0", "false", "no", "off", "disabled":
		return false
	default:
		return true
	}
}

func intEnv(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func durationEnv(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

func listEnv(keys ...string) []string {
	for _, key := range keys {
		if v := os.Getenv(key); v != "" {
			parts := strings.Split(v, ",")
			out := make([]string, 0, len(parts))
			for _, part := range parts {
				item := strings.ToLower(strings.TrimSpace(part))
				if item != "" {
					out = append(out, item)
				}
			}
			return out
		}
	}
	return nil
}

func clampInt32(v, lo, hi int) int32 {
	switch {
	case v < lo:
		return int32(lo) //nolint:gosec // bounded by caller-provided lo
	case v > hi:
		return int32(hi) //nolint:gosec // bounded by caller-provided hi
	default:
		return int32(v) //nolint:gosec // bounded by lo..hi range, fits in int32
	}
}
