package fcm

import (
	"time"

	"github.com/jpillora/backoff"
)

const (
	// DefaultMinBackoff is a default min delay for backoff.
	DefaultMinBackoff = 1 * time.Second
	// DefaultMaxBackoff is the default max delay for backoff.
	DefaultMaxBackoff = 10 * time.Second
)

// backoffProvider defines an interface for backoff.
type backoffProvider interface {
	sendAnother() bool
	getMin() time.Duration
	setMin(min time.Duration)
	wait()
}

// Implementation of backoff provider using exponential backoff.
type exponentialBackoff struct {
	b            backoff.Backoff
	currentDelay time.Duration
}

// Factory method for exponential backoff, uses default values for Min and Max and
// adds Jitter.
func newExponentialBackoff() backoffProvider {
	b := &backoff.Backoff{
		Min:    DefaultMinBackoff,
		Max:    DefaultMaxBackoff,
		Jitter: true,
	}
	return &exponentialBackoff{b: *b, currentDelay: b.Duration()}
}

// Returns true if not over the retries limit.
func (eb *exponentialBackoff) sendAnother() bool {
	return eb.currentDelay <= eb.b.Max
}

func (eb *exponentialBackoff) getMin() time.Duration {
	return eb.b.Min
}

// Set the minumim delay for backoff.
func (eb *exponentialBackoff) setMin(min time.Duration) {
	eb.b.Min = min
	if (eb.currentDelay) < min {
		eb.currentDelay = min
	}
}

// Wait for the current value of backoff.
func (eb *exponentialBackoff) wait() {
	time.Sleep(eb.currentDelay)
	eb.currentDelay = eb.b.Duration()
}
