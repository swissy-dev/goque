package goque_test

import (
	"context"
	"fmt"
	"time"

	"github.com/swissy-dev/goque"
	"github.com/swissy-dev/goque/backend/memory"
)

type EmailArgs struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

func (EmailArgs) Kind() string { return "email.send" }

func Example() {
	workers := goque.NewWorkers()
	sent := make(chan string, 1)
	goque.RegisterFunc(workers, func(ctx context.Context, job *goque.Job[EmailArgs]) error {
		sent <- fmt.Sprintf("%s: %s", job.Args.To, job.Args.Subject)
		return nil
	})
	client, err := goque.NewClient(memory.New(), &goque.Config{
		Workers: workers,
		Queues:  map[string]goque.QueueConfig{"default": {Concurrency: 2, PollInterval: 10 * time.Millisecond}},
	})
	if err != nil {
		panic(err)
	}
	if _, err := client.Enqueue(context.Background(), EmailArgs{To: "a@b.c", Subject: "hi"}); err != nil {
		panic(err)
	}
	if err := client.Start(context.Background()); err != nil {
		panic(err)
	}
	fmt.Println(<-sent)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client.Stop(ctx)
	// Output: a@b.c: hi
}
