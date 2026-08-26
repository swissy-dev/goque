package postgres

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/swissy-dev/goque/backend"
)

type enqueueRow struct {
	Kind             string          `json:"kind"`
	Queue            string          `json:"queue"`
	Payload          json.RawMessage `json:"payload"`
	State            string          `json:"state"`
	PriorityBoostNS  int64           `json:"priority_boost_ns"`
	ScheduledAtNS    int64           `json:"scheduled_at_ns"`
	PriorityAtNS     int64           `json:"priority_at_ns"`
	MaxAttempts      int             `json:"max_attempts"`
	CreatedAt        string          `json:"created_at"`
	ConcurrencyKey   string          `json:"concurrency_key"`
	ThrottleKey      string          `json:"throttle_key"`
	DebounceKey      string          `json:"debounce_key"`
	DebounceDeadline *string         `json:"debounce_deadline"`
	RetryPolicy      json.RawMessage `json:"retry_policy"`
	Metadata         json.RawMessage `json:"metadata"`
	Output           json.RawMessage `json:"output"`
	Version          int             `json:"version"`
}

type finalizeRow struct {
	ID         int64           `json:"id"`
	Generation int             `json:"generation"`
	Metadata   json.RawMessage `json:"metadata"`
	AtNS       *int64          `json:"at_ns"`
	Err        json.RawMessage `json:"err"`
}

type heartbeatRow struct {
	ID         int64 `json:"id"`
	Generation int   `json:"generation"`
}

func nanos(t time.Time) (int64, error) {
	if err := backend.ValidateInstant(t); err != nil {
		return 0, err
	}
	return t.UnixNano(), nil
}

func fromNanos(ns int64) time.Time {
	return time.Unix(0, ns).UTC()
}

func nonNullJSON(b []byte) json.RawMessage {
	trimmed := bytes.TrimSpace(b)
	if len(trimmed) == 0 || string(trimmed) == "null" {
		return json.RawMessage(`{}`)
	}
	return json.RawMessage(b)
}

func rawOrNull(b []byte) json.RawMessage {
	if len(b) == 0 {
		return nil
	}
	return json.RawMessage(b)
}

func newEnqueueRows(jobs []*backend.JobRow, now time.Time) ([]enqueueRow, error) {
	out := make([]enqueueRow, len(jobs))
	for i, j := range jobs {
		at := j.ScheduledAt
		if at.IsZero() {
			at = now
		}
		priority, err := backend.SubDuration(at, j.PriorityBoost)
		if err != nil {
			return nil, err
		}
		atNS, err := nanos(at)
		if err != nil {
			return nil, err
		}
		priorityNS, err := nanos(priority)
		if err != nil {
			return nil, err
		}
		state := backend.StateAvailable
		if at.After(now) {
			state = backend.StateScheduled
		}
		version := j.Version
		if version == 0 {
			version = 1
		}
		var deadline *string
		if !j.DebounceDeadline.IsZero() {
			if err := backend.ValidateInstant(j.DebounceDeadline); err != nil {
				return nil, err
			}
			s := j.DebounceDeadline.UTC().Format(time.RFC3339Nano)
			deadline = &s
		}
		out[i] = enqueueRow{
			Kind:             j.Kind,
			Queue:            j.Queue,
			Payload:          nonNullJSON(j.Payload),
			State:            string(state),
			PriorityBoostNS:  int64(j.PriorityBoost),
			ScheduledAtNS:    atNS,
			PriorityAtNS:     priorityNS,
			MaxAttempts:      j.MaxAttempts,
			CreatedAt:        now.UTC().Format(time.RFC3339Nano),
			ConcurrencyKey:   j.ConcurrencyKey,
			ThrottleKey:      j.ThrottleKey,
			DebounceKey:      j.DebounceKey,
			DebounceDeadline: deadline,
			RetryPolicy:      rawOrNull(j.RetryPolicy),
			Metadata:         nonNullJSON(j.Metadata),
			Output:           rawOrNull(j.Output),
			Version:          version,
		}
	}
	return out, nil
}

func encodeBatch(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
