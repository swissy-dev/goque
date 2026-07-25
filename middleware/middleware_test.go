package middleware

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/swissy-dev/goque"
)

func TestRecoveryConvertsPanic(t *testing.T) {
	wf := Recovery()(func(ctx context.Context, job *goque.JobRow) error { panic("kaboom") })
	err := wf(context.Background(), &goque.JobRow{})
	if err == nil || !strings.Contains(err.Error(), "kaboom") {
		t.Fatalf("err=%v", err)
	}
	var pe *goque.PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("err=%v, want it to be a *goque.PanicError", err)
	}
	if len(pe.Stack) == 0 {
		t.Fatal("PanicError.Stack is empty")
	}
	if pe.Recovered != "kaboom" {
		t.Fatalf("PanicError.Recovered=%v want %q", pe.Recovered, "kaboom")
	}
}

func TestTimeoutAppliesDeadline(t *testing.T) {
	row := &goque.JobRow{}
	row.Metadata = []byte(`{"goque":{"timeout_ms":10}}`)
	wf := Timeout()(func(ctx context.Context, job *goque.JobRow) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
			return nil
		}
	})
	start := time.Now()
	err := wf(context.Background(), row)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("deadline not applied")
	}
}

func TestHooksAndLogger(t *testing.T) {
	var before, after int
	h := Hooks(HookFuncs{
		Before: func(ctx context.Context, row *goque.JobRow) { before++ },
		After:  func(ctx context.Context, row *goque.JobRow, d time.Duration, err error) { after++ },
	})
	var sb strings.Builder
	lg := Logger(slog.New(slog.NewTextHandler(&sb, nil)))
	wf := h(lg(func(ctx context.Context, job *goque.JobRow) error { return errors.New("x") }))
	_ = wf(context.Background(), &goque.JobRow{Kind: "k", Queue: "q"})
	if before != 1 || after != 1 {
		t.Fatalf("hooks before=%d after=%d", before, after)
	}
	if !strings.Contains(sb.String(), "kind=k") {
		t.Fatalf("log output: %s", sb.String())
	}
}
