package main

import (
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
	Namespace                  string
	PollInterval               time.Duration
	MetricsPort                string
	RedpandaBroker             string
	FindingsTopic              string
	PublishTimeout             time.Duration
	CheckTimeout               time.Duration
	StateConfigMapName         string
	StateWriteInterval         time.Duration
	ResolvedFindingRetention   time.Duration
	AITriageEnabled            bool
	AIMinScore                 int
	AICooldown                 time.Duration
	AICooldownIncident         time.Duration
	LogScanLines               int
	ResourceUsageBackend       string
	MemoryWarningPercent       int
	MemoryCriticalPercent      int
	RestartWarningCount        int32
	RestartWindow              time.Duration
	EventLookback              time.Duration
	SuppressConfigMapName      string
	CustomResourceAllowlist    []string
	CustomResourceExcludelist  []string
	CustomResourceDiscoveryTTL time.Duration
	Checks                     CheckConfig
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
		AITriageEnabled:            boolEnvDefaultOn("AI_TRIAGE_ENABLED"),
		AIMinScore:                 intEnv("AI_MIN_SCORE", 60),
		AICooldown:                 durationEnv("AI_COOLDOWN", 30*time.Minute),
		AICooldownIncident:         durationEnv("AI_COOLDOWN_INCIDENT", 10*time.Minute),
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
