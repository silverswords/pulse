package internal

import (
	"context"
	"errors"
	"math"
	"time"
)

// Potentially retry when:
// - 404 Not Found
// - 413 Payload Too Large with Retry-After (NOT SUPPORTED)
// - 425 Too Early
// - 429 Too Many Requests
// - 503 Service Unavailable (with or without Retry-After) (IGNORE Retry-After)
// - 504 Gateway Timeout

type BackoffStrategy string

const (
	BackoffStrategyNone        = "none"
	BackoffStrategyConstant    = "constant"
	BackoffStrategyLinear      = "linear"
	BackoffStrategyExponential = "exponential"
)

// DefaultRetryParams max wait time is 64 + 32 + 16 + 8 + 4 + 2 + 1 sum 123 seconds with 6 times call.
var DefaultRetryParams = RetryParams{Strategy: BackoffStrategyLinear, MaxTries: 6, Period: 500 * time.Millisecond}

// RetryParams holds parameters applied to retries
type RetryParams struct {
	// Strategy is the backoff strategy to applies between retries
	Strategy BackoffStrategy

	// MaxTries is the maximum number of times to retry request before giving up
	MaxTries int

	// Period is
	// - for none strategy: no delay
	// - for constant strategy: the delay interval between retries
	// - for linear strategy: interval between retries = Period * retries
	// - for exponential strategy: interval between retries = Period * retries^2
	Period time.Duration
}

// BackoffFor tries will return the time duration that should be used for this
// current try count.
// `tries` is assumed to be the number of times the caller has already retried.
func (r *RetryParams) BackoffFor(tries int) time.Duration {
	switch r.Strategy {
	case BackoffStrategyConstant:
		return r.Period
	case BackoffStrategyLinear:
		return r.Period * time.Duration(tries)
	case BackoffStrategyExponential:
		exp := math.Exp2(float64(tries))
		return r.Period * time.Duration(exp)
	case BackoffStrategyNone:
		fallthrough // default
	default:
		return r.Period
	}
}

// Backoff is a blocking call to wait for the correct amount of time for the retry.
// `tries` is assumed to be the number of times the caller has already retried.
func (r *RetryParams) Backoff(ctx context.Context, tries int) error {
	if tries > r.MaxTries {
		return errors.New("too many retries")
	}
	ticker := time.NewTicker(r.BackoffFor(tries))
	select {
	case <-ctx.Done():
		ticker.Stop()
		return errors.New("context has been cancelled")
	case <-ticker.C:
		ticker.Stop()
	}
	return nil
}
