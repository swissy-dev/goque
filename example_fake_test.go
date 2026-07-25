package goque_test

import (
	"context"
	"fmt"
	"time"

	"github.com/swissy-dev/goque"
	"github.com/swissy-dev/goque/backend/memory"
)

type ReminderArgs struct {
	To string `json:"to"`
}

func (ReminderArgs) Kind() string { return "email.reminder" }

type exampleT struct{}

func (exampleT) Helper() {}

func (exampleT) Fatalf(format string, args ...any) { panic(fmt.Sprintf(format, args...)) }

func ExampleClient_Fake() {
	workers := goque.NewWorkers()
	goque.RegisterFunc(workers, func(ctx context.Context, job *goque.Job[ReminderArgs]) error {
		fmt.Println("reminder sent to", job.Args.To)
		return nil
	})
	client, err := goque.NewClient(memory.New(), &goque.Config{Workers: workers})
	if err != nil {
		panic(err)
	}
	f := client.Fake(exampleT{})
	if _, err := client.Enqueue(context.Background(), ReminderArgs{To: "a@b.c"}, goque.WithDelay(24*time.Hour)); err != nil {
		panic(err)
	}
	f.RunReady(context.Background()).AssertNoneRan()
	f.Advance(24 * time.Hour)
	f.RunReady(context.Background()).AssertRan("email.reminder")
	// Output: reminder sent to a@b.c
}
