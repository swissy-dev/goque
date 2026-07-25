package goque

import (
	"reflect"
	"testing"
	"time"
)

func TestExponentialDelay(t *testing.T) {
	p := Exponential{Base: 2 * time.Second, Max: time.Minute}
	cases := []struct {
		attempt int
		want    time.Duration
	}{{1, 2 * time.Second}, {2, 4 * time.Second}, {3, 8 * time.Second}, {10, time.Minute}}
	for _, c := range cases {
		if got := p.Delay(c.attempt); got != c.want {
			t.Fatalf("attempt %d: got %v want %v", c.attempt, got, c.want)
		}
	}
}

func TestExponentialJitterBounds(t *testing.T) {
	p := Exponential{Base: 10 * time.Second, Max: time.Hour, Jitter: 0.2}
	for i := 0; i < 100; i++ {
		d := p.Delay(1)
		if d < 8*time.Second || d > 12*time.Second {
			t.Fatalf("jittered delay %v outside [8s,12s]", d)
		}
	}
}

func TestLinearFixedIntervals(t *testing.T) {
	if got := (Linear{Step: 5 * time.Second, Max: 12 * time.Second}).Delay(3); got != 12*time.Second {
		t.Fatalf("linear capped: got %v", got)
	}
	if got := (Fixed{Interval: 7 * time.Second}).Delay(9); got != 7*time.Second {
		t.Fatalf("fixed: got %v", got)
	}
	iv := Intervals{time.Second, time.Minute}
	if got := iv.Delay(1); got != time.Second {
		t.Fatalf("intervals[0]: got %v", got)
	}
	if got := iv.Delay(5); got != time.Minute {
		t.Fatalf("intervals clamp: got %v", got)
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	for _, p := range []RetryPolicy{
		Exponential{Base: time.Second, Max: time.Hour, Jitter: 0.1},
		Linear{Step: time.Second, Max: time.Minute},
		Fixed{Interval: 3 * time.Second},
		Intervals{time.Second, 2 * time.Second},
	} {
		b, err := EncodeRetryPolicy(p)
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeRetryPolicy(b)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, p) {
			t.Fatalf("round-trip mismatch: got %#v want %#v", got, p)
		}
	}
	if p, err := DecodeRetryPolicy(nil); p != nil || err != nil {
		t.Fatal("nil data must decode to nil policy")
	}
}

func TestNamedPolicy(t *testing.T) {
	RegisterRetryPolicy("cliff", func(attempt int) time.Duration { return time.Duration(attempt) * time.Hour })
	b, err := EncodeRetryPolicy(Named{Name: "cliff"})
	if err != nil {
		t.Fatal(err)
	}
	p, err := DecodeRetryPolicy(b)
	if err != nil {
		t.Fatal(err)
	}
	if p.Delay(2) != 2*time.Hour {
		t.Fatalf("named delay: got %v", p.Delay(2))
	}
	if _, err := DecodeRetryPolicy([]byte(`{"type":"named","name":"ghost"}`)); err == nil {
		t.Fatal("unregistered name must error")
	}
}
