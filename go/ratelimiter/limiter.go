package ratelimiter

import (
	"sync"
	"time"
)

type Limiter struct {
	mu         sync.Mutex
	buckets    map[string]*TokenBucket
	capacity   float64
	refillRate float64
}

func NewLimiter(capacity, refillRate float64) *Limiter {
	return &Limiter{
		buckets:    make(map[string]*TokenBucket),
		capacity:   capacity,
		refillRate: refillRate,
	}
}

func (l *Limiter) Allow(clientID string) bool {
	l.mu.Lock()
	bucket, exists := l.buckets[clientID]

	if !exists {
		bucket = NewTokenBucket(l.capacity, l.refillRate)
		l.buckets[clientID] = bucket
	}
	l.mu.Unlock()
	return l.buckets[clientID].Allow()
}

func (l *Limiter) StartCleanup(interval, idleTimeout time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			l.cleanup(idleTimeout)
		}
	}()
}

func (l *Limiter) cleanup(idleTimeout time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	for clientID, bucket := range l.buckets {
		bucket.mu.Lock()
		idle := now.Sub(bucket.lastRefill)
		bucket.mu.Unlock()
		if idle > idleTimeout {
			delete(l.buckets, clientID)
		}
	}
}
