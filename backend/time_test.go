package backend

import (
	"errors"
	"math"
	"testing"
	"time"
)

func TestValidateInstantBoundaries(t *testing.T) {
	cases := []struct {
		name string
		in   time.Time
		ok   bool
	}{
		{"min", MinInstant, true},
		{"max", MaxInstant, true},
		{"inside", time.Unix(1_700_000_000, 0).UTC(), true},
		{"one nanosecond below min", MinInstant.Add(-time.Nanosecond), false},
		{"one nanosecond above max", MaxInstant.Add(time.Nanosecond), false},
		{"zero value", time.Time{}, false},
		{"far future", time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateInstant(tc.in)
			if tc.ok && err != nil {
				t.Fatalf("%s must be storable, got %v", tc.in, err)
			}
			if !tc.ok {
				if !errors.Is(err, ErrTimeOutOfRange) {
					t.Fatalf("%s must be rejected with ErrTimeOutOfRange, got %v", tc.in, err)
				}
			}
		})
	}
}

func TestMinMaxInstantAreExactNanosecondBounds(t *testing.T) {
	if got := MinInstant.UnixNano(); got != math.MinInt64 {
		t.Fatalf("MinInstant.UnixNano() = %d, want math.MinInt64", got)
	}
	if got := MaxInstant.UnixNano(); got != math.MaxInt64 {
		t.Fatalf("MaxInstant.UnixNano() = %d, want math.MaxInt64", got)
	}
}

func TestSubDuration(t *testing.T) {
	base := time.Unix(1_700_000_000, 0).UTC()
	got, err := SubDuration(base, 90*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if want := base.Add(-90 * time.Minute); !got.Equal(want) {
		t.Fatalf("SubDuration = %s, want %s", got, want)
	}
	if got, err := SubDuration(base, -time.Hour); err != nil || !got.Equal(base.Add(time.Hour)) {
		t.Fatalf("a negative duration must add: got %s, %v", got, err)
	}
	if _, err := SubDuration(MinInstant, time.Nanosecond); !errors.Is(err, ErrTimeOutOfRange) {
		t.Fatalf("subtracting past MinInstant must be ErrTimeOutOfRange, got %v", err)
	}
	if _, err := SubDuration(MaxInstant, -time.Nanosecond); !errors.Is(err, ErrTimeOutOfRange) {
		t.Fatalf("adding past MaxInstant must be ErrTimeOutOfRange, got %v", err)
	}
	if _, err := SubDuration(time.Time{}, 0); !errors.Is(err, ErrTimeOutOfRange) {
		t.Fatalf("an unstorable input must be rejected before the arithmetic, got %v", err)
	}
}

func TestSubDurationClampedSaturates(t *testing.T) {
	if got := SubDurationClamped(MinInstant, time.Hour); !got.Equal(MinInstant) {
		t.Fatalf("underflow must pin to MinInstant, got %s", got)
	}
	if got := SubDurationClamped(MaxInstant, -time.Hour); !got.Equal(MaxInstant) {
		t.Fatalf("overflow must pin to MaxInstant, got %s", got)
	}
	if got := SubDurationClamped(time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC), 0); !got.Equal(MaxInstant) {
		t.Fatalf("an unstorable input must be pinned, got %s", got)
	}
	base := time.Unix(1_700_000_000, 0).UTC()
	if got := SubDurationClamped(base, time.Hour); !got.Equal(base.Add(-time.Hour)) {
		t.Fatalf("an in-range subtraction must be exact, got %s", got)
	}
}
