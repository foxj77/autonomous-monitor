package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// ── PollHealth unit tests ─────────────────────────────────────────────────────

func TestPollHealth_LiveBeforeFirstPoll(t *testing.T) {
	h := NewPollHealth(3 * time.Minute)
	// No poll has completed — process is still starting; must report live.
	if !h.isLive(time.Now()) {
		t.Fatal("expected isLive=true before first poll (startup grace window)")
	}
}

func TestPollHealth_NotReadyBeforeFirstPoll(t *testing.T) {
	h := NewPollHealth(3 * time.Minute)
	if h.isReady() {
		t.Fatal("expected isReady=false before first poll")
	}
}

func TestPollHealth_ReadyAfterFirstPoll(t *testing.T) {
	h := NewPollHealth(3 * time.Minute)
	h.MarkPollComplete()
	if !h.isReady() {
		t.Fatal("expected isReady=true after first poll")
	}
}

func TestPollHealth_LiveAfterRecentPoll(t *testing.T) {
	maxGap := 3 * time.Minute
	h := NewPollHealth(maxGap)
	h.MarkPollComplete()
	// Query immediately — well within the gap.
	if !h.isLive(time.Now()) {
		t.Fatal("expected isLive=true right after poll completion")
	}
}

func TestPollHealth_NotLiveWhenGapExceeded(t *testing.T) {
	maxGap := 3 * time.Minute
	h := NewPollHealth(maxGap)
	h.MarkPollComplete()
	// Simulate asking "now" far in the future (gap + 1 second).
	futureNow := time.Now().Add(maxGap + time.Second)
	if h.isLive(futureNow) {
		t.Fatal("expected isLive=false when last poll is older than maxGap")
	}
}

func TestPollHealth_LiveExactlyAtGapBoundary(t *testing.T) {
	maxGap := 3 * time.Minute
	h := NewPollHealth(maxGap)
	h.MarkPollComplete()
	// Exactly at the boundary should still be live (<=, not <).
	boundaryNow := time.Unix(0, h.lastPollNano.Load()).Add(maxGap)
	if !h.isLive(boundaryNow) {
		t.Fatal("expected isLive=true exactly at the gap boundary")
	}
}

// ── EffectiveMaxPollGap tests ─────────────────────────────────────────────────

func TestEffectiveMaxPollGap_ExplicitOverride(t *testing.T) {
	cfg := Config{
		PollInterval:     60 * time.Second,
		CheckTimeout:     30 * time.Second,
		HealthMaxPollGap: 10 * time.Minute,
	}
	got := EffectiveMaxPollGap(cfg)
	if got != 10*time.Minute {
		t.Fatalf("expected 10m, got %s", got)
	}
}

func TestEffectiveMaxPollGap_DefaultFormula_3xInterval(t *testing.T) {
	// 3 * 60s = 180s; 60s + 30s = 90s → should pick 180s
	cfg := Config{
		PollInterval: 60 * time.Second,
		CheckTimeout: 30 * time.Second,
	}
	got := EffectiveMaxPollGap(cfg)
	if got != 3*time.Minute {
		t.Fatalf("expected 3m, got %s", got)
	}
}

func TestEffectiveMaxPollGap_DefaultFormula_IntervalPlusTimeout(t *testing.T) {
	// 3 * 10s = 30s; 10s + 45s = 55s → should pick 55s
	cfg := Config{
		PollInterval: 10 * time.Second,
		CheckTimeout: 45 * time.Second,
	}
	got := EffectiveMaxPollGap(cfg)
	if got != 55*time.Second {
		t.Fatalf("expected 55s, got %s", got)
	}
}

// ── HTTP handler tests ────────────────────────────────────────────────────────

func TestLivezHandler_BeforeFirstPoll(t *testing.T) {
	h := NewPollHealth(3 * time.Minute)
	rec := httptest.NewRecorder()
	LivezHandler(h)(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 before first poll (startup grace), got %d", rec.Code)
	}
}

func TestLivezHandler_AfterRecentPoll(t *testing.T) {
	h := NewPollHealth(3 * time.Minute)
	h.MarkPollComplete()
	rec := httptest.NewRecorder()
	LivezHandler(h)(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 after recent poll, got %d", rec.Code)
	}
}

func TestLivezHandler_WhenGapExceeded(t *testing.T) {
	maxGap := 3 * time.Minute
	h := NewPollHealth(maxGap)
	h.MarkPollComplete()

	// Temporarily override the "now" by manipulating lastPollNano to a time
	// that puts us beyond the gap.
	staleTime := time.Now().Add(-(maxGap + time.Second))
	h.lastPollNano.Store(staleTime.UnixNano())

	rec := httptest.NewRecorder()
	LivezHandler(h)(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when poll gap exceeded, got %d", rec.Code)
	}
}

func TestReadyzHandler_BeforeFirstPoll(t *testing.T) {
	h := NewPollHealth(3 * time.Minute)
	rec := httptest.NewRecorder()
	ReadyzHandler(h)(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 before first poll, got %d", rec.Code)
	}
}

func TestReadyzHandler_AfterFirstPoll(t *testing.T) {
	h := NewPollHealth(3 * time.Minute)
	h.MarkPollComplete()
	rec := httptest.NewRecorder()
	ReadyzHandler(h)(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 after first poll, got %d", rec.Code)
	}
}

// TestPollHealthRace verifies that concurrent reads and writes to PollHealth
// are data-race-free. Run with: go test -race ./...
func TestPollHealthRace(t *testing.T) {
	h := NewPollHealth(time.Minute)
	done := make(chan struct{})

	// Writer goroutine.
	go func() {
		for i := 0; i < 100; i++ {
			h.MarkPollComplete()
		}
		close(done)
	}()

	// Reader goroutines.
	for i := 0; i < 4; i++ {
		go func() {
			for {
				select {
				case <-done:
					return
				default:
					h.isLive(time.Now())
					h.isReady()
				}
			}
		}()
	}

	<-done
}
