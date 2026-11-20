package delivery

import (
	"sync"
	"time"
)

const (
	breakerThreshold = 3
	breakerCooldown  = 30 * time.Second
)

type circuit struct {
	failures   int
	openUntil  time.Time
	probing    bool
	generation uint64
}

// Breakers are shared by all accounts in one worker process. A generation
// prevents late responses from older calls from closing a newly opened circuit.
type providerBreakers struct {
	mu     sync.Mutex
	states map[string]*circuit
}

func (b *providerBreakers) allow(provider string, now time.Time) (bool, uint64, time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.states == nil {
		b.states = make(map[string]*circuit)
	}
	s := b.states[provider]
	if s == nil {
		s = &circuit{}
		b.states[provider] = s
	}
	if !s.openUntil.IsZero() {
		if now.Before(s.openUntil) {
			return false, 0, s.openUntil.Sub(now)
		}
		if s.probing {
			return false, 0, breakerCooldown
		}
		s.probing = true
	}
	return true, s.generation, 0
}

func (b *providerBreakers) record(provider string, generation uint64, failed bool, now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.states[provider]
	if s.generation != generation {
		return
	}
	if !failed {
		s.failures = 0
		s.openUntil = time.Time{}
		if s.probing {
			s.generation++
		}
		s.probing = false
		return
	}
	s.failures++
	if s.probing || s.failures >= breakerThreshold {
		s.openUntil = now.Add(breakerCooldown)
		s.probing = false
		s.generation++
	}
}
