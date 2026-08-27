package ratelimiter

import (
	"testing"
	"time"
)

func TestLimiter_PerClientIsolation(t *testing.T) {
	l := NewLimiter(1, 0) // no refill

	if !l.Allow("a") {
		t.Fatal("expected client a's first request to be allowed")
	}
	if l.Allow("a") {
		t.Fatal("expected client a's second request to be denied")
	}
	if !l.Allow("b") {
		t.Fatal("expected client b to have its own independent bucket")
	}
}

func TestLimiter_CleanupRemovesIdleClients(t *testing.T) {
	l := NewLimiter(1, 0)
	l.Allow("stale")

	l.mu.Lock()
	l.buckets["stale"].lastRefill = time.Now().Add(-time.Hour)
	l.mu.Unlock()

	l.cleanup(time.Minute)

	l.mu.Lock()
	_, exists := l.buckets["stale"]
	l.mu.Unlock()

	if exists {
		t.Fatal("expected idle client bucket to be removed by cleanup")
	}
}

func TestLimiter_CleanupKeepsActiveClients(t *testing.T) {
	l := NewLimiter(1, 0)
	l.Allow("active")

	l.cleanup(time.Minute)

	l.mu.Lock()
	_, exists := l.buckets["active"]
	l.mu.Unlock()

	if !exists {
		t.Fatal("expected recently used client bucket to survive cleanup")
	}
}
