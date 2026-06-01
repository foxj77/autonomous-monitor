package main

import (
	"crypto/sha256"
	"fmt"
	"time"
)

const (
	sourceName  = "autonomous-monitor"
	payloadType = "namespace_finding"
)

type Finding struct {
	Source                       string     `json:"source"`
	PayloadType                  string     `json:"payload_type"`
	ID                           string     `json:"id"`
	Namespace                    string     `json:"namespace"`
	Kind                         string     `json:"kind"`
	Name                         string     `json:"name"`
	Severity                     string     `json:"severity"`
	Classification               string     `json:"classification"`
	Score                        int        `json:"score"`
	Check                        string     `json:"check"`
	Reason                       string     `json:"reason"`
	Status                       string     `json:"status"`
	FirstSeen                    time.Time  `json:"first_seen"`
	LastSeen                     time.Time  `json:"last_seen"`
	Evidence                     []string   `json:"evidence"`
	Prediction                   string     `json:"prediction,omitempty"`
	MatchingKubernetesEventFound bool       `json:"matching_kubernetes_event_found"`
	AITriageRequired             bool       `json:"ai_triage_required"`
	CooldownUntil                *time.Time `json:"cooldown_until,omitempty"`
}

func NewFinding(namespace, kind, name, check, reason string, score int, evidence []string) Finding {
	return Finding{
		Source:         sourceName,
		PayloadType:    payloadType,
		ID:             findingID(namespace, kind, name, check, reason),
		Namespace:      namespace,
		Kind:           kind,
		Name:           name,
		Severity:       severityForScore(score),
		Classification: classificationForScore(score),
		Score:          clampScore(score),
		Check:          check,
		Reason:         reason,
		Status:         "ongoing",
		Evidence:       evidence,
	}
}

func findingID(namespace, kind, name, check, reason string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%s|%s|%s", namespace, kind, name, check, reason)))
	return fmt.Sprintf("%x", sum)
}

func classificationForScore(score int) string {
	score = clampScore(score)
	switch {
	case score >= 80:
		return "incident-likely"
	case score >= 60:
		return "degraded"
	case score >= 40:
		return "suspicious"
	case score >= 20:
		return "watch"
	default:
		return "healthy"
	}
}

func severityForScore(score int) string {
	score = clampScore(score)
	switch {
	case score >= 80:
		return "critical"
	case score >= 60:
		return "high"
	case score >= 40:
		return "medium"
	default:
		return "low"
	}
}

func clampScore(score int) int {
	switch {
	case score < 0:
		return 0
	case score > 100:
		return 100
	default:
		return score
	}
}
