package main

import (
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

// PollHealth tracks liveness state for the poll loop. It is safe for
// concurrent use — the HTTP handlers read from a different goroutine than
// the poll loop that writes.
//
// Fields are int64 UnixNano values stored atomically:
//   - lastPollNano: set to time.Now().UnixNano() at the end of each Poll call.
//     Zero means no poll has completed yet.
//   - firstPollDone: 1 once the first Poll has completed, 0 before.
type PollHealth struct {
	lastPollNano  atomic.Int64
	firstPollDone atomic.Int32

	// maxGap is the maximum allowed duration between completed polls before
	// /healthz returns 503. Computed once at startup from Config.HealthMaxPollGap
	// (or the default formula) and never changed.
	maxGap time.Duration
}

// NewPollHealth creates a PollHealth with the given max gap.
func NewPollHealth(maxGap time.Duration) *PollHealth {
	return &PollHealth{maxGap: maxGap}
}

// EffectiveMaxPollGap returns the max gap to use for /healthz.
// If cfg.HealthMaxPollGap is set it is used verbatim. Otherwise the
// formula max(3 * PollInterval, PollInterval + CheckTimeout) is applied,
// which provides enough slack for one slow poll before alarming.
func EffectiveMaxPollGap(cfg Config) time.Duration {
	if cfg.HealthMaxPollGap > 0 {
		return cfg.HealthMaxPollGap
	}
	a := 3 * cfg.PollInterval
	b := cfg.PollInterval + cfg.CheckTimeout
	if a > b {
		return a
	}
	return b
}

// MarkPollComplete records a successful poll completion. Call this at the
// end of Monitor.Poll, after all checks have run.
func (h *PollHealth) MarkPollComplete() {
	h.lastPollNano.Store(time.Now().UnixNano())
	h.firstPollDone.Store(1)
}

// isReady returns true once the first poll has completed.
func (h *PollHealth) isReady() bool {
	return h.firstPollDone.Load() == 1
}

// isLive returns true when the process is considered alive:
//   - Before the first poll completes, always true (startup grace window).
//   - After the first poll, true if the last poll completed within maxGap.
//
// The now parameter is injectable so tests can control time.
func (h *PollHealth) isLive(now time.Time) bool {
	ns := h.lastPollNano.Load()
	if ns == 0 {
		// No poll has completed yet — still starting up.
		return true
	}
	last := time.Unix(0, ns)
	return now.Sub(last) <= h.maxGap
}

// LivezHandler returns an HTTP handler for /healthz.
// 200 while the process is within the allowed poll gap (or still starting up).
// 503 when the poll loop appears wedged.
func LivezHandler(h *PollHealth) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h.isLive(time.Now()) {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprintln(w, "ok")
			return
		}
		ns := h.lastPollNano.Load()
		last := time.Unix(0, ns)
		age := time.Since(last).Truncate(time.Second)
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprintf(w, "poll loop stalled: last poll completed %s ago (max gap %s)\n", age, h.maxGap)
	}
}

// ReadyzHandler returns an HTTP handler for /readyz.
// 200 once the first poll has completed (state loaded, first scan done).
// 503 before the first poll completes.
func ReadyzHandler(h *PollHealth) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h.isReady() {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprintln(w, "ok")
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprintln(w, "waiting for first poll to complete")
	}
}
