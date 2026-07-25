package backend

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestAttemptErrorJSONRoundTrip(t *testing.T) {
	in := AttemptError{At: time.Unix(100, 0).UTC(), Attempt: 3, Err: "boom", Stack: "s"}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out AttemptError
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out != in {
		t.Fatalf("got %+v want %+v", out, in)
	}
}

func TestStaleErrorUnwraps(t *testing.T) {
	err := error(&StaleError{IDs: []int64{7, 9}})
	if !errors.Is(err, ErrStaleAttempt) {
		t.Fatal("StaleError must unwrap to ErrStaleAttempt")
	}
	if err.Error() != "stale attempt for job ids [7 9]" {
		t.Fatalf("unexpected message %q", err.Error())
	}
}

func TestTerminalStates(t *testing.T) {
	terminal := map[State]bool{StateCompleted: true, StateCancelled: true, StateDead: true}
	for _, s := range []State{StateScheduled, StateAvailable, StateRunning, StateRetryable, StateCompleted, StateCancelled, StateDead} {
		if s.Terminal() != terminal[s] {
			t.Fatalf("state %s Terminal()=%v", s, s.Terminal())
		}
	}
}
