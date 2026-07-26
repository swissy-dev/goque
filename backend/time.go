package backend

import (
	"fmt"
	"math"
	"time"
)

// MinInstant and MaxInstant bound the instants every goque backend must be
// able to store. Backends order jobs by an int64 count of nanoseconds since
// the Unix epoch, and these are the endpoints that count can represent —
// roughly 1677-09-21 to 2262-04-11. An instant outside them is rejected with
// ErrTimeOutOfRange rather than silently wrapping.
var (
	MinInstant = time.Unix(0, math.MinInt64).UTC()
	MaxInstant = time.Unix(0, math.MaxInt64).UTC()
)

// ValidateInstant returns nil when t is inside the range every backend can
// store, and an error wrapping ErrTimeOutOfRange when it is not. Both
// endpoints are valid. The zero time.Time is not.
func ValidateInstant(t time.Time) error {
	if t.Before(MinInstant) || t.After(MaxInstant) {
		return fmt.Errorf("%w: %s", ErrTimeOutOfRange, t.Format(time.RFC3339Nano))
	}
	return nil
}

// SubDuration returns t minus d, reporting an error wrapping ErrTimeOutOfRange
// if t is unstorable or if the subtraction leaves the storable range. Backends
// use it to derive a job's effective priority instant from a ScheduledAt and a
// PriorityBoost the caller supplied, so that an impossible request fails the
// call instead of wrapping around into the distant future.
func SubDuration(t time.Time, d time.Duration) (time.Time, error) {
	if err := ValidateInstant(t); err != nil {
		return time.Time{}, err
	}
	ns := t.UnixNano()
	res := ns - int64(d)
	if (d > 0 && res > ns) || (d < 0 && res < ns) {
		return time.Time{}, fmt.Errorf("%w: %s minus %s", ErrTimeOutOfRange, t.Format(time.RFC3339Nano), d)
	}
	return time.Unix(0, res).UTC(), nil
}

// SubDurationClamped returns t minus d, pinned to MinInstant or MaxInstant
// instead of failing when the result leaves the storable range. An input t
// that is itself outside the range is first clamped to the nearer bound, so
// the subtraction always starts from a storable instant. Backends use it
// where the boost comes from a stored row rather than from the caller — a
// rescue or a retry re-applying the boost of a job enqueued long ago — because
// refusing to finalize such a job would strand it.
func SubDurationClamped(t time.Time, d time.Duration) time.Time {
	if t.Before(MinInstant) {
		t = MinInstant
	}
	if t.After(MaxInstant) {
		t = MaxInstant
	}
	ns := t.UnixNano()
	res := ns - int64(d)
	if d > 0 && res > ns {
		return MinInstant
	}
	if d < 0 && res < ns {
		return MaxInstant
	}
	return time.Unix(0, res).UTC()
}
