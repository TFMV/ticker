package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/TFMV/ticker/queue"
	"github.com/TFMV/ticker/spanner"
)

const (
	queueName = "messages"

	consumerPriority     = "priority-consumer"
	consumerRoutingPay   = "payment-worker"
	consumerRoutingNotif = "notification-worker"
	consumerBatch        = "batch-consumer"
	consumerDelayed      = "delayed-consumer"
	consumerDedup        = "dedup-consumer"
)

func main() {
	ctx := context.Background()

	client := mustInitSpanner(ctx)
	defer client.Close()

	q := queue.NewSpannerQueue(client.Client, queueName)

	done := q.StartRequeueWorker(ctx, 1*time.Minute)
	defer close(done)

	demoMessagePriorities(ctx, q)
	demoRoutingAndConsumerGroups(ctx, q)
	demoBatchProcessing(ctx, q)
	demoDelayedDelivery(ctx, q)
	demoMessageDeduplication(ctx, q)
}

// ---------- init ----------

func mustInitSpanner(ctx context.Context) *spanner.Client {
	client, err := spanner.NewClient(ctx, spanner.Config{
		ProjectID:  "your-project",
		InstanceID: "your-instance",
		DatabaseID: "your-database",
	})
	if err != nil {
		log.Fatalf("spanner init failed: %v", err)
	}

	if err := client.EnsureSchema(ctx); err != nil {
		log.Fatalf("schema init failed: %v", err)
	}

	return client
}

// ---------- helpers ----------

func enqueue(ctx context.Context, q *queue.SpannerQueue, p queue.EnqueueParams, label string) string {
	id, err := q.Enqueue(ctx, p)
	if err != nil {
		log.Fatalf("enqueue failed (%s): %v", label, err)
	}
	return id
}

func dequeueOne(ctx context.Context, q *queue.SpannerQueue, p queue.DequeueParams) *queue.Message {
	msgs, err := q.Dequeue(ctx, p)
	if err != nil {
		log.Fatalf("dequeue failed: %v", err)
	}
	if len(msgs) == 0 {
		return nil
	}
	return msgs[0]
}

func ack(ctx context.Context, q *queue.SpannerQueue, msg *queue.Message, consumerID string) {
	if msg == nil {
		return
	}

	if err := q.Acknowledge(ctx, queue.UpdateParams{
		MessageID:  msg.ID,
		ConsumerID: consumerID,
	}); err != nil {
		log.Fatalf("ack failed: %v", err)
	}
}

// ---------- demos ----------

func demoMessagePriorities(ctx context.Context, q *queue.SpannerQueue) {
	fmt.Println("\n=== Message Priorities ===")

	priorities := []struct {
		p    queue.Priority
		name string
	}{
		{queue.PriorityLow, "Low"},
		{queue.PriorityNormal, "Normal"},
		{queue.PriorityHigh, "High"},
		{queue.PriorityCritical, "Critical"},
	}

	for _, pr := range priorities {
		id := enqueue(ctx, q, queue.EnqueueParams{
			Payload:  fmt.Sprintf("%s priority message", pr.name),
			Priority: pr.p,
		}, pr.name)

		fmt.Printf("enqueued %s: %s\n", pr.name, id)
	}

	fmt.Println("\nprocessing by priority:")

	for range priorities {
		msg := dequeueOne(ctx, q, queue.DequeueParams{
			ConsumerID: consumerPriority,
			BatchSize:  1,
		})
		if msg == nil {
			break
		}

		fmt.Printf("got: %s (p=%d)\n", msg.Payload, msg.Priority)
		ack(ctx, q, msg, consumerPriority)
	}
}

func demoRoutingAndConsumerGroups(ctx context.Context, q *queue.SpannerQueue) {
	fmt.Println("\n=== Routing + Consumer Groups ===")

	routes := []struct {
		route string
		group string
	}{
		{"payments", "payment-processors"},
		{"notifications", "notification-handlers"},
		{"analytics", "data-analysts"},
	}

	for _, r := range routes {
		enqueue(ctx, q, queue.EnqueueParams{
			Payload:       fmt.Sprintf("event for %s", r.route),
			RouteKey:      r.route,
			ConsumerGroup: r.group,
		}, r.route)

		fmt.Printf("queued route=%s group=%s\n", r.route, r.group)
	}

	fmt.Println("\npayments only:")

	msgs, err := q.Dequeue(ctx, queue.DequeueParams{
		ConsumerID:    consumerRoutingPay,
		RouteKeys:     []string{"payments"},
		ConsumerGroup: "payment-processors",
		BatchSize:     10,
	})
	if err != nil {
		log.Fatalf("route dequeue failed: %v", err)
	}

	for _, m := range msgs {
		fmt.Println("payment:", m.Payload)
		_ = q.Acknowledge(ctx, queue.UpdateParams{
			MessageID:  m.ID,
			ConsumerID: consumerRoutingPay,
		})
	}

	fmt.Println("\nnotifications only:")

	msgs, err = q.Dequeue(ctx, queue.DequeueParams{
		ConsumerID:    consumerRoutingNotif,
		RouteKeys:     []string{"notifications"},
		ConsumerGroup: "notification-handlers",
		BatchSize:     10,
	})
	if err != nil {
		log.Fatalf("route dequeue failed: %v", err)
	}

	for _, m := range msgs {
		fmt.Println("notification:", m.Payload)
		_ = q.Acknowledge(ctx, queue.UpdateParams{
			MessageID:  m.ID,
			ConsumerID: consumerRoutingNotif,
		})
	}
}

func demoBatchProcessing(ctx context.Context, q *queue.SpannerQueue) {
	fmt.Println("\n=== Batch Processing ===")

	for i := 1; i <= 5; i++ {
		enqueue(ctx, q, queue.EnqueueParams{
			Payload:  fmt.Sprintf("batch-%d", i),
			Priority: queue.PriorityNormal,
		}, "batch")
	}

	msgs, err := q.Dequeue(ctx, queue.DequeueParams{
		ConsumerID: consumerBatch,
		BatchSize:  5,
	})
	if err != nil {
		log.Fatalf("batch dequeue failed: %v", err)
	}

	fmt.Printf("got batch size=%d\n", len(msgs))

	for _, m := range msgs {
		fmt.Println("processing:", m.Payload)
		_ = q.Acknowledge(ctx, queue.UpdateParams{
			MessageID:  m.ID,
			ConsumerID: consumerBatch,
		})
	}
}

func demoDelayedDelivery(ctx context.Context, q *queue.SpannerQueue) {
	fmt.Println("\n=== Delayed Delivery ===")

	delay := 3 * time.Second
	visibleAt := time.Now().Add(delay)

	enqueue(ctx, q, queue.EnqueueParams{
		Payload:      "delayed message",
		VisibleAfter: visibleAt,
	}, "delayed")

	fmt.Println("trying early dequeue...")

	msg := dequeueOne(ctx, q, queue.DequeueParams{
		ConsumerID: consumerDelayed,
		BatchSize:  1,
	})

	if msg != nil {
		fmt.Println("unexpected early message:", msg.Payload)
	} else {
		fmt.Println("no message yet (correct)")
	}

	time.Sleep(delay + 300*time.Millisecond)

	fmt.Println("retry after delay:")

	msg = dequeueOne(ctx, q, queue.DequeueParams{
		ConsumerID: consumerDelayed,
		BatchSize:  1,
	})

	if msg == nil {
		fmt.Println("still nothing (bad)")
		return
	}

	fmt.Println("got:", msg.Payload)
	ack(ctx, q, msg, consumerDelayed)
}

func demoMessageDeduplication(ctx context.Context, q *queue.SpannerQueue) {
	fmt.Println("\n=== Deduplication ===")

	key := "payment-12345"

	id1 := enqueue(ctx, q, queue.EnqueueParams{
		Payload:          "payment 12345",
		DeduplicationKey: key,
	}, "dedup-1")

	id2 := enqueue(ctx, q, queue.EnqueueParams{
		Payload:          "payment 12345 duplicate",
		DeduplicationKey: key,
	}, "dedup-2")

	if id1 == id2 {
		fmt.Println("dedup OK:", id1)
	} else {
		fmt.Println("dedup mismatch:", id1, id2)
	}

	msg := dequeueOne(ctx, q, queue.DequeueParams{
		ConsumerID: consumerDedup,
		BatchSize:  1,
	})

	if msg != nil {
		fmt.Println("processing dedup:", msg.Payload)
		ack(ctx, q, msg, consumerDedup)
	}
}
