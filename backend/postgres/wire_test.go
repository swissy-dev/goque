package postgres

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/swissy-dev/goque/backend"
)

var wireNow = time.Unix(1_700_000_000, 0).UTC()

func TestEncodeBatchDoesNotBase64EncodeJSON(t *testing.T) {
	rows, err := newEnqueueRows([]*backend.JobRow{{
		Kind:        "k",
		Queue:       "q",
		Payload:     []byte(`{"user":42}`),
		MaxAttempts: 3,
		ScheduledAt: wireNow,
		Metadata:    []byte(`{"trace":"abc"}`),
		RetryPolicy: []byte(`{"kind":"exponential"}`),
	}}, wireNow)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := encodeBatch(rows)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"payload":{"user":42}`, `"metadata":{"trace":"abc"}`, `"retry_policy":{"kind":"exponential"}`} {
		if !strings.Contains(doc, want) {
			t.Fatalf("encoded batch is missing %s; a []byte field was probably base64-encoded:\n%s", want, doc)
		}
	}
}

func TestNilMetadataBecomesAnEmptyObject(t *testing.T) {
	rows, err := newEnqueueRows([]*backend.JobRow{{
		Kind: "k", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: wireNow,
	}}, wireNow)
	if err != nil {
		t.Fatal(err)
	}
	if string(rows[0].Metadata) != "{}" {
		t.Fatalf("nil Metadata encoded as %q, want {} — the column is NOT NULL and the default does not apply when a value is supplied", rows[0].Metadata)
	}
	doc, err := encodeBatch(rows)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(doc, `"metadata":null`) {
		t.Fatalf("encoded batch emits a JSON null for metadata:\n%s", doc)
	}
}

func TestMetadataJSONNullBecomesAnEmptyObject(t *testing.T) {
	for _, tc := range []struct {
		name string
		meta []byte
	}{
		{"bare null", []byte("null")},
		{"null with surrounding whitespace", []byte(" null ")},
		{"empty slice", []byte{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := newEnqueueRows([]*backend.JobRow{{
				Kind: "k", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: wireNow, Metadata: tc.meta,
			}}, wireNow)
			if err != nil {
				t.Fatal(err)
			}
			doc, err := encodeBatch(rows)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(doc, `"metadata":{}`) {
				t.Fatalf("Metadata %q must encode as {} in the emitted document; the column is NOT NULL:\n%s", tc.meta, doc)
			}
		})
	}
}

func TestMetadataNullValuedKeySurvivesUnchanged(t *testing.T) {
	rows, err := newEnqueueRows([]*backend.JobRow{{
		Kind: "k", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: wireNow, Metadata: []byte(`{"a":null}`),
	}}, wireNow)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := encodeBatch(rows)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(doc, `"metadata":{"a":null}`) {
		t.Fatalf("a Metadata object with a null-valued key must survive unchanged, not be collapsed to {}:\n%s", doc)
	}
}

func TestNilPayloadBecomesAnEmptyObject(t *testing.T) {
	rows, err := newEnqueueRows([]*backend.JobRow{{
		Kind: "k", Queue: "q", MaxAttempts: 3, ScheduledAt: wireNow,
	}}, wireNow)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := encodeBatch(rows)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(doc, `"payload":{}`) {
		t.Fatalf("a nil Payload must encode as {} in the emitted document; the column is NOT NULL:\n%s", doc)
	}
}

func TestNilRetryPolicyStaysNull(t *testing.T) {
	rows, err := newEnqueueRows([]*backend.JobRow{{
		Kind: "k", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: wireNow,
	}}, wireNow)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := encodeBatch(rows)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(doc, `"retry_policy":null`) {
		t.Fatalf("retry_policy is a nullable column and an unset policy must encode as null:\n%s", doc)
	}
}

func TestEnqueueRowDerivesStateAndInstants(t *testing.T) {
	future := wireNow.Add(time.Hour)
	rows, err := newEnqueueRows([]*backend.JobRow{
		{Kind: "due", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: wireNow},
		{Kind: "later", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: future},
		{Kind: "boosted", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: wireNow, PriorityBoost: time.Minute},
		{Kind: "unset", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3},
	}, wireNow)
	if err != nil {
		t.Fatal(err)
	}
	if rows[0].State != string(backend.StateAvailable) {
		t.Fatalf("a job scheduled at now is %q, want available", rows[0].State)
	}
	if rows[1].State != string(backend.StateScheduled) {
		t.Fatalf("a job scheduled in the future is %q, want scheduled", rows[1].State)
	}
	if rows[2].PriorityAtNS != rows[2].ScheduledAtNS-int64(time.Minute) {
		t.Fatalf("PriorityAt is not ScheduledAt minus the boost: %d vs %d", rows[2].PriorityAtNS, rows[2].ScheduledAtNS-int64(time.Minute))
	}
	if rows[3].ScheduledAtNS != wireNow.UnixNano() {
		t.Fatalf("an unset ScheduledAt must default to Now, got %d", rows[3].ScheduledAtNS)
	}
	if rows[3].State != string(backend.StateAvailable) {
		t.Fatalf("a job defaulted to Now is %q, want available", rows[3].State)
	}
}

func TestEnqueueRowsRejectUnstorableInstants(t *testing.T) {
	beyond := backend.MaxInstant.Add(time.Hour)

	rows, err := newEnqueueRows([]*backend.JobRow{
		{Kind: "k", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: beyond},
	}, wireNow)
	if !errors.Is(err, backend.ErrTimeOutOfRange) {
		t.Fatalf("an instant beyond MaxInstant must be rejected, got %v", err)
	}
	if rows != nil {
		t.Fatalf("a rejected batch must return nil rows, not a partial write, got %v", rows)
	}

	rows, err = newEnqueueRows([]*backend.JobRow{
		{Kind: "k", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: backend.MinInstant, PriorityBoost: time.Hour},
	}, wireNow)
	if !errors.Is(err, backend.ErrTimeOutOfRange) {
		t.Fatalf("a boost underflowing MinInstant must be rejected, got %v", err)
	}
	if rows != nil {
		t.Fatalf("a rejected batch must return nil rows, not a partial write, got %v", rows)
	}

	rows, err = newEnqueueRows([]*backend.JobRow{
		{Kind: "k", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: wireNow, DebounceDeadline: beyond},
	}, wireNow)
	if !errors.Is(err, backend.ErrTimeOutOfRange) {
		t.Fatalf("a DebounceDeadline beyond MaxInstant must be rejected, got %v", err)
	}
	if rows != nil {
		t.Fatalf("a rejected batch must return nil rows, not a partial write, got %v", rows)
	}
}

func TestNanosRoundTrip(t *testing.T) {
	for _, in := range []time.Time{backend.MinInstant, backend.MaxInstant, wireNow, wireNow.Add(3 * time.Nanosecond)} {
		ns, err := nanos(in)
		if err != nil {
			t.Fatalf("%s must be storable: %v", in, err)
		}
		if got := fromNanos(ns); !got.Equal(in) {
			t.Fatalf("round trip of %s gave %s", in, got)
		}
	}
	if _, err := nanos(backend.MaxInstant.Add(time.Nanosecond)); !errors.Is(err, backend.ErrTimeOutOfRange) {
		t.Fatalf("one nanosecond past MaxInstant must be rejected, got %v", err)
	}
}

func TestFinalizeRowEncodesPerRowValues(t *testing.T) {
	at := wireNow.Add(time.Minute).UnixNano()
	doc, err := encodeBatch([]finalizeRow{
		{ID: 1, Generation: 2, Metadata: json.RawMessage(`{"a":1}`), AtNS: &at, Err: json.RawMessage(`{"err":"boom"}`)},
		{ID: 3, Generation: 4},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(doc, `"metadata":{"a":1}`) || !strings.Contains(doc, `"err":{"err":"boom"}`) {
		t.Fatalf("per-row values were not encoded literally:\n%s", doc)
	}
	if !strings.Contains(doc, `"metadata":null`) {
		t.Fatalf("an unset Metadata must encode as null so COALESCE leaves the stored value alone:\n%s", doc)
	}
	if !strings.Contains(doc, `"at_ns":null`) {
		t.Fatalf("an unset AtNS must encode as null:\n%s", doc)
	}
}
