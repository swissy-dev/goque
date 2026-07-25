package goque

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/swissy-dev/goque/backend"
)

var T1 = time.Unix(1_700_000_100, 0).UTC()

type addArgs struct {
	A int `json:"a"`
	B int `json:"b"`
}

func (addArgs) Kind() string { return "test.add" }

type addWorker struct {
	WorkerDefaults[addArgs]
	got chan int
}

func (w *addWorker) Work(_ context.Context, job *Job[addArgs]) error {
	w.got <- job.Args.A + job.Args.B
	return nil
}

func TestRegisterAndDispatch(t *testing.T) {
	w := NewWorkers()
	aw := &addWorker{got: make(chan int, 1)}
	if err := Register(w, aw); err != nil {
		t.Fatal(err)
	}
	fn, ok := w.dispatch("test.add")
	if !ok {
		t.Fatal("dispatch not found")
	}
	row := &JobRow{ID: 1, Kind: "test.add", Payload: []byte(`{"a":2,"b":3}`)}
	if err := fn(context.Background(), row); err != nil {
		t.Fatal(err)
	}
	if got := <-aw.got; got != 5 {
		t.Fatalf("worker computed %d", got)
	}
}

func TestRegisterFuncAndDuplicateKind(t *testing.T) {
	w := NewWorkers()
	err := RegisterFunc(w, func(_ context.Context, _ *Job[addArgs]) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := RegisterFunc(w, func(_ context.Context, _ *Job[addArgs]) error { return nil }); !errors.Is(err, backend.ErrDuplicateKind) {
		t.Fatalf("want ErrDuplicateKind, got %v", err)
	}
}

func TestDispatchBadPayload(t *testing.T) {
	w := NewWorkers()
	if err := RegisterFunc(w, func(_ context.Context, _ *Job[addArgs]) error { return nil }); err != nil {
		t.Fatal(err)
	}
	fn, _ := w.dispatch("test.add")
	if err := fn(context.Background(), &JobRow{Kind: "test.add", Payload: []byte(`{`)}); err == nil {
		t.Fatal("malformed payload must error")
	}
}

func TestControlErrors(t *testing.T) {
	base := errors.New("why")
	if c, ok := asCancel(Cancel(base)); !ok || !errors.Is(c.err, base) {
		t.Fatal("Cancel wrap/unwrap failed")
	}
	if s, ok := asSnooze(SnoozeFor(5)); !ok || s.d != 5 {
		t.Fatal("SnoozeFor wrap failed")
	}
	if r, ok := asRetryAt(RetryAt(T1, base)); !ok || !r.at.Equal(T1) {
		t.Fatal("RetryAt wrap failed")
	}
	if _, ok := asCancel(base); ok {
		t.Fatal("plain error must not match Cancel")
	}
}
