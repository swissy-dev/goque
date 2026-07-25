package goque

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

// RetryPolicy computes how long to wait before a failed job's next attempt.
// The built-in policies are [Exponential], [Linear], [Fixed], [Intervals], and
// [Named]; only those can be stored on a job row, so a custom curve is
// registered with [RegisterRetryPolicy] and referenced by [Named].
type RetryPolicy interface {
	// Delay returns the wait before the next attempt. attempt is the number of
	// the attempt that just failed, starting at 1.
	Delay(attempt int) time.Duration
}

// Exponential waits Base after the first failure and doubles the wait after
// each subsequent one. A non-zero Max caps the result, and a non-zero Jitter
// spreads it to avoid retry storms.
type Exponential struct {
	// Base is the wait after the first failed attempt.
	Base time.Duration `json:"base"`
	// Max caps the doubling. Zero means uncapped.
	Max time.Duration `json:"max"`
	// Jitter randomizes the wait by up to this fraction either way, so 0.1
	// spreads it uniformly across 90% to 110% of the computed delay. It is
	// applied after Max, so the result can exceed Max by that fraction.
	Jitter float64 `json:"jitter,omitempty"`
}

// Delay returns Base doubled once per attempt beyond the first, capped at Max
// and then jittered.
func (p Exponential) Delay(attempt int) time.Duration {
	d := p.Base
	for i := 1; i < attempt; i++ {
		d *= 2
		if p.Max > 0 && d >= p.Max {
			d = p.Max
			break
		}
	}
	if p.Max > 0 && d > p.Max {
		d = p.Max
	}
	if p.Jitter > 0 {
		f := 1 + p.Jitter*(2*rand.Float64()-1)
		d = time.Duration(float64(d) * f)
	}
	return d
}

// Linear grows the wait by a fixed increment per attempt: Step after the first
// failure, twice Step after the second, and so on.
type Linear struct {
	// Step is the increment added per attempt, and the wait after the first
	// failed attempt.
	Step time.Duration `json:"step"`
	// Max caps the wait. Zero means uncapped.
	Max time.Duration `json:"max"`
}

// Delay returns attempt times Step, capped at Max.
func (p Linear) Delay(attempt int) time.Duration {
	d := time.Duration(attempt) * p.Step
	if p.Max > 0 && d > p.Max {
		d = p.Max
	}
	return d
}

// Fixed waits the same amount of time before every attempt.
type Fixed struct {
	// Interval is the wait before each retry.
	Interval time.Duration `json:"interval"`
}

// Delay returns Interval regardless of the attempt number.
func (p Fixed) Delay(int) time.Duration {
	return p.Interval
}

// Intervals is an explicit backoff schedule: the first entry is the wait after
// the first failed attempt, the second entry the wait after the second, and so
// on. Attempts past the end of the list repeat the last entry.
type Intervals []time.Duration

// Delay returns the entry for attempt, clamped to the first and last entries.
// An empty Intervals retries immediately.
func (p Intervals) Delay(attempt int) time.Duration {
	if len(p) == 0 {
		return 0
	}
	if attempt < 1 {
		attempt = 1
	}
	if attempt > len(p) {
		attempt = len(p)
	}
	return p[attempt-1]
}

// Named refers to a custom backoff curve registered with
// [RegisterRetryPolicy]. Only the name is stored on the job row, which keeps an
// arbitrary Go function serializable — at the cost that every process which may
// retry the job has to register the same name.
type Named struct {
	// Name is the key the curve was registered under.
	Name string `json:"name"`
}

// Delay calls the registered function. It returns 0 if Name is not registered
// in this process.
func (p Named) Delay(attempt int) time.Duration {
	namedMu.RLock()
	fn := namedPolicies[p.Name]
	namedMu.RUnlock()
	if fn == nil {
		return 0
	}
	return fn(attempt)
}

var (
	namedMu       sync.RWMutex
	namedPolicies = map[string]func(int) time.Duration{}
)

// RegisterRetryPolicy registers fn as the backoff curve called name, for use
// through [Named]. Registering the same name again replaces the function.
// Registration is process-wide and safe for concurrent use, and is expected to
// happen during start-up, before any job referring to the name is decoded.
func RegisterRetryPolicy(name string, fn func(attempt int) time.Duration) {
	namedMu.Lock()
	defer namedMu.Unlock()
	namedPolicies[name] = fn
}

type policyEnvelope struct {
	Type      string          `json:"type"`
	Base      time.Duration   `json:"base,omitempty"`
	Max       time.Duration   `json:"max,omitempty"`
	Jitter    float64         `json:"jitter,omitempty"`
	Step      time.Duration   `json:"step,omitempty"`
	Interval  time.Duration   `json:"interval,omitempty"`
	Intervals []time.Duration `json:"intervals,omitempty"`
	Name      string          `json:"name,omitempty"`
}

// EncodeRetryPolicy serializes p into the form stored on a job row. A nil
// policy encodes to nil bytes, which means the job falls back to its client's
// default policy. Only the built-in policies can be encoded; any other
// implementation of [RetryPolicy] returns an error, so reach for [Named]
// instead.
func EncodeRetryPolicy(p RetryPolicy) ([]byte, error) {
	var env policyEnvelope
	switch v := p.(type) {
	case nil:
		return nil, nil
	case Exponential:
		env = policyEnvelope{Type: "exponential", Base: v.Base, Max: v.Max, Jitter: v.Jitter}
	case Linear:
		env = policyEnvelope{Type: "linear", Step: v.Step, Max: v.Max}
	case Fixed:
		env = policyEnvelope{Type: "fixed", Interval: v.Interval}
	case Intervals:
		env = policyEnvelope{Type: "intervals", Intervals: v}
	case Named:
		env = policyEnvelope{Type: "named", Name: v.Name}
	default:
		return nil, fmt.Errorf("goque: unencodable retry policy %T", p)
	}
	return json.Marshal(env)
}

// DecodeRetryPolicy reconstructs a policy encoded by [EncodeRetryPolicy].
// Empty data yields a nil policy and a nil error. It reports an error for an
// unknown policy type and for a [Named] policy whose name is not registered in
// this process; in both cases the executor falls back to the client's default
// policy.
func DecodeRetryPolicy(data []byte) (RetryPolicy, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var env policyEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, err
	}
	switch env.Type {
	case "exponential":
		return Exponential{Base: env.Base, Max: env.Max, Jitter: env.Jitter}, nil
	case "linear":
		return Linear{Step: env.Step, Max: env.Max}, nil
	case "fixed":
		return Fixed{Interval: env.Interval}, nil
	case "intervals":
		return Intervals(env.Intervals), nil
	case "named":
		namedMu.RLock()
		_, ok := namedPolicies[env.Name]
		namedMu.RUnlock()
		if !ok {
			return nil, fmt.Errorf("goque: unregistered retry policy %q", env.Name)
		}
		return Named{Name: env.Name}, nil
	default:
		return nil, fmt.Errorf("goque: unknown retry policy type %q", env.Type)
	}
}
