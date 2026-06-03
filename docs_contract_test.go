package main

import (
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
)

func TestReadmeDocumentsImplementedMetrics(t *testing.T) {
	readme := readTestFile(t, "README.md")
	metricsSource := readTestFile(t, "metrics.go")

	documented := sortedMatches(regexp.MustCompile(`autonomous_monitor_[a-z0-9_]+`).FindAllString(readmeSection(readme, "## Metrics", "##"), -1))
	implemented := sortedSubmatches(regexp.MustCompile(`Name:\s+"(autonomous_monitor_[^"]+)"`).FindAllStringSubmatch(metricsSource, -1), 1)

	assertSameStrings(t, documented, implemented, "README metrics")
}

func TestReadmeDocumentsImplementedConfigVariables(t *testing.T) {
	readme := readTestFile(t, "README.md")
	configSource := readTestFile(t, "config.go")

	documented := documentedConfigVars(readme)
	implemented := sortedSubmatches(regexp.MustCompile(`"([A-Z][A-Z0-9_]+)"`).FindAllStringSubmatch(configSource, -1), 1)

	assertSameStrings(t, documented, implemented, "README config variables")
}

func readTestFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

func readmeSection(readme, startHeading, nextHeadingPrefix string) string {
	start := strings.Index(readme, startHeading)
	if start == -1 {
		return ""
	}
	rest := readme[start+len(startHeading):]
	next := strings.Index(rest, "\n"+nextHeadingPrefix)
	if next == -1 {
		return rest
	}
	return rest[:next]
}

func documentedConfigVars(readme string) []string {
	section := readmeSection(readme, "## Configuration", "##")
	matches := regexp.MustCompile("`([A-Z][A-Z0-9_]+)`").FindAllStringSubmatch(section, -1)
	return sortedSubmatches(matches, 1)
}

func sortedMatches(matches []string) []string {
	set := map[string]struct{}{}
	for _, match := range matches {
		set[match] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for match := range set {
		out = append(out, match)
	}
	sort.Strings(out)
	return out
}

func sortedSubmatches(matches [][]string, idx int) []string {
	values := make([]string, 0, len(matches))
	for _, match := range matches {
		if len(match) > idx {
			values = append(values, match[idx])
		}
	}
	return sortedMatches(values)
}

func assertSameStrings(t *testing.T, got, want []string, label string) {
	t.Helper()
	missing := difference(want, got)
	extra := difference(got, want)
	if len(missing) > 0 || len(extra) > 0 {
		t.Fatalf("%s drifted\nmissing from docs: %v\nnot implemented: %v", label, missing, extra)
	}
}

func difference(a, b []string) []string {
	bSet := map[string]struct{}{}
	for _, item := range b {
		bSet[item] = struct{}{}
	}
	var diff []string
	for _, item := range a {
		if _, ok := bSet[item]; !ok {
			diff = append(diff, item)
		}
	}
	return diff
}
