package ratelimiter

import (
	"sync"
	"testing"
	"time"
)

func TestAllow_InitialCapacityIsAvailable(t *testing.T) {
	b := NewTokenBucket(3, 1)

	for i := 0; i < 3; i++ {
		if !b.Allow() {
			t.Fatalf("expected request %d to be allowed, bucket started with capacity 3", i+1)
		}
	}
}

func TestAllow_BlocksOnceExhausted(t *testing.T) {
	b := NewTokenBucket(1, 0) // no refill

	if !b.Allow() {
		t.Fatal("expected first request to be allowed")
	}
	if b.Allow() {
		t.Fatal("expected second request to be denied, bucket should be empty")
	}
}

func TestAllow_RefillsOverTime(t *testing.T) {
	b := NewTokenBucket(1, 10) // 10 tokens/sec

	if !b.Allow() {
		t.Fatal("expected first request to be allowed")
	}
	if b.Allow() {
		t.Fatal("expected immediate second request to be denied")
	}

	time.Sleep(150 * time.Millisecond) // ~1.5 tokens refilled, capped at 1

	if !b.Allow() {
		t.Fatal("expected request to be allowed after refill window")
	}
}

func TestAllow_RefillDoesNotExceedCapacity(t *testing.T) {
	b := NewTokenBucket(2, 1000) // fast refill

	time.Sleep(50 * time.Millisecond) // would add ~50 tokens without capping

	allowed := 0
	for i := 0; i < 5; i++ {
		if b.Allow() {
			allowed++
		}
	}

	if allowed != 2 {
		t.Fatalf("expected exactly 2 allowed requests (capacity cap), got %d", allowed)
	}
}

func TestAllow_ConcurrentAccessRespectsCapacity(t *testing.T) {
	b := NewTokenBucket(50, 0) // no refill, fixed budget

	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed := 0

	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if b.Allow() {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if allowed != 50 {
		t.Fatalf("expected exactly 50 allowed requests under concurrency, got %d", allowed)
	}
}
