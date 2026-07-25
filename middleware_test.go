package goque

import (
	"context"
	"testing"
)

func TestChainWorkOrder(t *testing.T) {
	var trace []string
	mk := func(name string) Middleware {
		return func(next WorkFunc) WorkFunc {
			return func(ctx context.Context, job *JobRow) error {
				trace = append(trace, name+"-in")
				err := next(ctx, job)
				trace = append(trace, name+"-out")
				return err
			}
		}
	}
	final := func(ctx context.Context, job *JobRow) error {
		trace = append(trace, "work")
		return nil
	}
	if err := chainWork([]Middleware{mk("a"), mk("b")}, final)(context.Background(), &JobRow{}); err != nil {
		t.Fatal(err)
	}
	want := []string{"a-in", "b-in", "work", "b-out", "a-out"}
	for i := range want {
		if trace[i] != want[i] {
			t.Fatalf("trace=%v", trace)
		}
	}
}

func TestChainEnqueueMutatesMetadata(t *testing.T) {
	tag := func(next EnqueueFunc) EnqueueFunc {
		return func(ctx context.Context, jobs []*JobRow) error {
			for _, j := range jobs {
				j.Metadata = []byte(`{"traced":true}`)
			}
			return next(ctx, jobs)
		}
	}
	var got []*JobRow
	sink := func(ctx context.Context, jobs []*JobRow) error {
		got = jobs
		return nil
	}
	rows := []*JobRow{{}, {}}
	if err := chainEnqueue([]EnqueueMiddleware{tag}, sink)(context.Background(), rows); err != nil {
		t.Fatal(err)
	}
	if string(got[1].Metadata) != `{"traced":true}` {
		t.Fatalf("metadata=%s", got[1].Metadata)
	}
}
