package memory

import (
	"context"
	"testing"
	"time"

	"github.com/swissy-dev/goque/backend"
	"github.com/swissy-dev/goque/backendtest"
)

func TestConformance(t *testing.T) {
	backendtest.Run(t, func(t *testing.T) backend.Backend { return New() })
}

func TestSnapshotAllAndReset(t *testing.T) {
	b := New()
	now := time.Unix(1_700_000_000, 0).UTC()
	rows := []*backend.JobRow{
		{Kind: "a", Queue: "q1", Payload: []byte(`{}`), MaxAttempts: 1, ScheduledAt: now},
		{Kind: "b", Queue: "q2", Payload: []byte(`{}`), MaxAttempts: 1, ScheduledAt: now},
	}
	if err := b.Enqueue(context.Background(), backend.EnqueueParams{Jobs: rows, Now: now}); err != nil {
		t.Fatal(err)
	}
	all := b.SnapshotAll()
	if len(all) != 2 || all[0].ID >= all[1].ID {
		t.Fatalf("SnapshotAll order/len: %+v", all)
	}
	all[0].Queue = "mutated"
	if b.Snapshot(all[0].ID).Queue == "mutated" {
		t.Fatal("SnapshotAll must return deep copies")
	}
	firstID := all[0].ID
	b.Reset()
	if got := b.SnapshotAll(); len(got) != 0 {
		t.Fatalf("Reset must wipe jobs, got %d", len(got))
	}
	fresh := []*backend.JobRow{{Kind: "c", Queue: "q1", Payload: []byte(`{}`), MaxAttempts: 1, ScheduledAt: now}}
	if err := b.Enqueue(context.Background(), backend.EnqueueParams{Jobs: fresh, Now: now}); err != nil {
		t.Fatal(err)
	}
	if fresh[0].ID <= firstID {
		t.Fatalf("nextID must stay monotonic across Reset: got %d after %d", fresh[0].ID, firstID)
	}
}
