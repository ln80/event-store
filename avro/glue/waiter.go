package glue

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/aws/smithy-go/rand"
)

// wait is actually copy-pasted from smithy package
func wait[T any](ctx context.Context, get func(ctx context.Context) (*T, error), retryable func(*T) bool) (*T, error) {
	minDelay, maxDelay, maxWaitDur := 1*time.Second, 5*time.Second, 25*time.Second

	ctx, cancelFn := context.WithTimeout(ctx, maxWaitDur)
	defer cancelFn()

	remainingTime := maxWaitDur
	var attempt int64
	for {
		attempt++
		start := time.Now()
		out, err := get(ctx)
		if err != nil {
			return nil, err
		}

		if !retryable(out) {
			return out, nil
		}

		remainingTime -= time.Since(start)
		if remainingTime < minDelay || remainingTime <= 0 {
			break
		}

		delay, err := computeDelay(
			attempt, minDelay, maxDelay, remainingTime,
		)

		if err != nil {
			return nil, fmt.Errorf("error computing waiter delay, %w", err)
		}

		remainingTime -= delay

		if err := sleepWithContext(ctx, delay); err != nil {
			return nil, fmt.Errorf("request cancelled while waiting, %w", err)
		}
	}

	return nil, fmt.Errorf("exceeded max wait time for GetSchemaVersion waiter")
}

func sleepWithContext(ctx context.Context, dur time.Duration) error {
	t := time.NewTimer(dur)
	defer t.Stop()

	select {
	case <-t.C:
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

// computeDelay copy-pasted from smithywaiter package
func computeDelay(attempt int64, minDelay, maxDelay, remainingTime time.Duration) (delay time.Duration, err error) {
	if attempt <= 0 {
		return 0, nil
	}

	if remainingTime <= 0 {
		return 0, nil
	}

	if minDelay == 0 {
		return 0, fmt.Errorf("minDelay must be greater than zero when computing Delay")
	}

	if maxDelay == 0 {
		return 0, fmt.Errorf("maxDelay must be greater than zero when computing Delay")
	}

	attemptCeiling := (math.Log(float64(maxDelay/minDelay)) / math.Log(2)) + 1

	if attempt > int64(attemptCeiling) {
		delay = maxDelay
	} else {
		ri := 1 << uint64(attempt-1)
		delay = minDelay * time.Duration(ri)
	}

	if delay != minDelay {
		d, err := rand.CryptoRandInt63n(int64(delay - minDelay))
		if err != nil {
			return 0, fmt.Errorf("error computing retry jitter, %w", err)
		}

		delay = time.Duration(d) + minDelay
	}

	if remainingTime-delay <= minDelay {
		delay = remainingTime - minDelay
	}

	return delay, nil
}
