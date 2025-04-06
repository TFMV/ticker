package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/TFMV/ticker/queue"
	"github.com/TFMV/ticker/spanner"
)

// This example demonstrates Ticker's advanced features:
// - Message priorities
// - Message routing
// - Consumer groups
// - Batch processing
// - Delayed delivery
// - Message metadata
func main() {
	ctx := context.Background()

	// Initialize Spanner client (replace these with your own project details)
	client, err := spanner.NewClient(ctx, spanner.Config{
		ProjectID:  "your-project",
		InstanceID: "your-instance",
		DatabaseID: "your-database",
	})
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Ensure schema exists
	if err := client.EnsureSchema(ctx); err != nil {
		log.Fatalf("Failed to ensure schema: %v", err)
	}

	// Create a queue
	q := queue.NewSpannerQueue(client.Client, "messages")

	// Start background requeue worker
	done := q.StartRequeueWorker(ctx, 1*time.Minute)
	defer close(done)

	// Demonstrate various advanced features
	demoMessagePriorities(ctx, q)
	demoRoutingAndConsumerGroups(ctx, q)
	demoBatchProcessing(ctx, q)
	demoDelayedDelivery(ctx, q)
	demoMessageDeduplication(ctx, q)
}

func demoMessagePriorities(ctx context.Context, q *queue.SpannerQueue) {
	fmt.Println("\n=== Message Priorities ===")

	// Enqueue messages with different priorities
	priorities := []struct {
		priority queue.Priority
		name     string
	}{
		{queue.PriorityLow, "Low"},
		{queue.PriorityNormal, "Normal"},
		{queue.PriorityHigh, "High"},
		{queue.PriorityCritical, "Critical"},
	}

	// Add messages in reverse order (low to critical)
	for _, p := range priorities {
		msgID, err := q.Enqueue(ctx, queue.EnqueueParams{
			Payload:  fmt.Sprintf("Priority %s message", p.name),
			Priority: p.priority,
		})
		if err != nil {
			log.Fatalf("Failed to enqueue %s priority message: %v", p.name, err)
		}
		fmt.Printf("Enqueued %s priority message: %s\n", p.name, msgID)
	}

	// Dequeue messages - should come back in priority order (critical to low)
	fmt.Println("\nDequeuing messages (should be in priority order):")
	for i := 0; i < len(priorities); i++ {
		msgs, err := q.Dequeue(ctx, queue.DequeueParams{
			ConsumerID: "priority-consumer",
			BatchSize:  1,
		})
		if err != nil {
			log.Fatalf("Failed to dequeue message: %v", err)
		}

		if len(msgs) == 0 {
			fmt.Println("No more messages available")
			break
		}

		msg := msgs[0]
		fmt.Printf("Received: %s (Priority: %d)\n", msg.Payload, msg.Priority)

		// Acknowledge the message
		err = q.Acknowledge(ctx, queue.UpdateParams{
			MessageID:  msg.ID,
			ConsumerID: "priority-consumer",
		})
		if err != nil {
			log.Fatalf("Failed to acknowledge message: %v", err)
		}
	}
}

func demoRoutingAndConsumerGroups(ctx context.Context, q *queue.SpannerQueue) {
	fmt.Println("\n=== Message Routing & Consumer Groups ===")

	// Define routes and consumer groups
	routes := []string{"payments", "notifications", "analytics"}
	groups := []string{"payment-processors", "notification-handlers", "data-analysts"}

	// Enqueue messages for different routes and consumer groups
	for i, route := range routes {
		group := groups[i]

		msgID, err := q.Enqueue(ctx, queue.EnqueueParams{
			Payload:       fmt.Sprintf("Message for %s route", route),
			RouteKey:      route,
			ConsumerGroup: group,
		})
		if err != nil {
			log.Fatalf("Failed to enqueue message for %s route: %v", route, err)
		}
		fmt.Printf("Enqueued message for %s route (group: %s): %s\n", route, group, msgID)
	}

	// Demonstrate route-specific dequeuing
	fmt.Println("\nDequeuing only payment messages:")
	msgs, err := q.Dequeue(ctx, queue.DequeueParams{
		ConsumerID:    "payment-worker",
		RouteKeys:     []string{"payments"},
		ConsumerGroup: "payment-processors",
		BatchSize:     5,
	})
	if err != nil {
		log.Fatalf("Failed to dequeue payment messages: %v", err)
	}

	for _, msg := range msgs {
		fmt.Printf("Received payment message: %s\n", msg.Payload)
		// Acknowledge the message
		q.Acknowledge(ctx, queue.UpdateParams{
			MessageID:  msg.ID,
			ConsumerID: "payment-worker",
		})
	}

	// Try to dequeue messages for a different route
	fmt.Println("\nDequeuing only notification messages:")
	msgs, err = q.Dequeue(ctx, queue.DequeueParams{
		ConsumerID:    "notification-worker",
		RouteKeys:     []string{"notifications"},
		ConsumerGroup: "notification-handlers",
		BatchSize:     5,
	})
	if err != nil {
		log.Fatalf("Failed to dequeue notification messages: %v", err)
	}

	for _, msg := range msgs {
		fmt.Printf("Received notification message: %s\n", msg.Payload)
		// Acknowledge the message
		q.Acknowledge(ctx, queue.UpdateParams{
			MessageID:  msg.ID,
			ConsumerID: "notification-worker",
		})
	}
}

func demoBatchProcessing(ctx context.Context, q *queue.SpannerQueue) {
	fmt.Println("\n=== Batch Processing ===")

	// Enqueue multiple messages
	batchSize := 5
	for i := 1; i <= batchSize; i++ {
		msgID, err := q.Enqueue(ctx, queue.EnqueueParams{
			Payload:  fmt.Sprintf("Batch message %d", i),
			Priority: queue.PriorityNormal,
		})
		if err != nil {
			log.Fatalf("Failed to enqueue batch message %d: %v", i, err)
		}
		fmt.Printf("Enqueued batch message %d: %s\n", i, msgID)
	}

	// Dequeue messages in a batch
	fmt.Printf("\nDequeuing messages in batch (size=%d):\n", batchSize)
	msgs, err := q.Dequeue(ctx, queue.DequeueParams{
		ConsumerID: "batch-consumer",
		BatchSize:  batchSize,
	})
	if err != nil {
		log.Fatalf("Failed to dequeue batch: %v", err)
	}

	fmt.Printf("Retrieved %d messages in batch\n", len(msgs))
	for i, msg := range msgs {
		fmt.Printf("Batch message %d: %s\n", i+1, msg.Payload)

		// Acknowledge each message
		err = q.Acknowledge(ctx, queue.UpdateParams{
			MessageID:  msg.ID,
			ConsumerID: "batch-consumer",
		})
		if err != nil {
			log.Fatalf("Failed to acknowledge message: %v", err)
		}
	}
}

func demoDelayedDelivery(ctx context.Context, q *queue.SpannerQueue) {
	fmt.Println("\n=== Delayed Delivery ===")

	// Enqueue a message with delayed visibility
	delayTime := 3 * time.Second
	visibleAfter := time.Now().Add(delayTime)

	msgID, err := q.Enqueue(ctx, queue.EnqueueParams{
		Payload:      "This is a delayed message",
		VisibleAfter: visibleAfter,
	})
	if err != nil {
		log.Fatalf("Failed to enqueue delayed message: %v", err)
	}
	fmt.Printf("Enqueued delayed message: %s (visible after: %s)\n",
		msgID, visibleAfter.Format(time.RFC3339))

	// Try to dequeue immediately (should not be visible yet)
	fmt.Println("\nAttempting to dequeue before delay expires (should be empty):")
	msgs, err := q.Dequeue(ctx, queue.DequeueParams{
		ConsumerID: "delayed-consumer",
		BatchSize:  1,
	})
	if err != nil {
		log.Fatalf("Failed to dequeue: %v", err)
	}

	if len(msgs) == 0 {
		fmt.Println("No messages available yet (as expected)")
	} else {
		fmt.Printf("Unexpected: found message before delay expired: %s\n", msgs[0].Payload)
	}

	// Wait for the delay to expire
	fmt.Printf("Waiting for %s delay to expire...\n", delayTime)
	time.Sleep(delayTime + 500*time.Millisecond)

	// Try again
	fmt.Println("Attempting to dequeue after delay expired:")
	msgs, err = q.Dequeue(ctx, queue.DequeueParams{
		ConsumerID: "delayed-consumer",
		BatchSize:  1,
	})
	if err != nil {
		log.Fatalf("Failed to dequeue: %v", err)
	}

	if len(msgs) > 0 {
		fmt.Printf("Retrieved delayed message: %s\n", msgs[0].Payload)

		// Acknowledge the message
		err = q.Acknowledge(ctx, queue.UpdateParams{
			MessageID:  msgs[0].ID,
			ConsumerID: "delayed-consumer",
		})
		if err != nil {
			log.Fatalf("Failed to acknowledge delayed message: %v", err)
		}
	} else {
		fmt.Println("Unexpected: no delayed message found after delay expired")
	}
}

func demoMessageDeduplication(ctx context.Context, q *queue.SpannerQueue) {
	fmt.Println("\n=== Message Deduplication ===")

	dedupKey := "payment-12345"

	// Enqueue first message with deduplication key
	msgID1, err := q.Enqueue(ctx, queue.EnqueueParams{
		Payload:          "Process payment #12345",
		DeduplicationKey: dedupKey,
	})
	if err != nil {
		log.Fatalf("Failed to enqueue first message: %v", err)
	}
	fmt.Printf("Enqueued first message with dedup key '%s': %s\n", dedupKey, msgID1)

	// Try to enqueue a second message with the same deduplication key
	msgID2, err := q.Enqueue(ctx, queue.EnqueueParams{
		Payload:          "Process payment #12345 (duplicate)",
		DeduplicationKey: dedupKey,
	})
	if err != nil {
		log.Fatalf("Failed to enqueue duplicate message: %v", err)
	}

	// If deduplication is working, the second message ID should be the same as the first
	if msgID2 == msgID1 {
		fmt.Printf("Deduplication successful: second message returned same ID: %s\n", msgID2)
	} else {
		fmt.Printf("Unexpected: received different ID for duplicate message: %s\n", msgID2)
	}

	// Dequeue and process the message
	msgs, err := q.Dequeue(ctx, queue.DequeueParams{
		ConsumerID: "dedup-consumer",
		BatchSize:  1,
	})
	if err != nil {
		log.Fatalf("Failed to dequeue: %v", err)
	}

	if len(msgs) > 0 {
		fmt.Printf("Retrieved message with dedup key: %s\n", msgs[0].Payload)

		// Acknowledge the message
		err = q.Acknowledge(ctx, queue.UpdateParams{
			MessageID:  msgs[0].ID,
			ConsumerID: "dedup-consumer",
		})
		if err != nil {
			log.Fatalf("Failed to acknowledge message: %v", err)
		}
	}
}
