package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/TFMV/ticker/queue"
	"github.com/TFMV/ticker/spanner"
)

// This example demonstrates the basic usage of Ticker's queue system.
// It shows how to enqueue, dequeue, and acknowledge messages.
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

	// Start background requeue worker to handle expired locks
	done := q.StartRequeueWorker(ctx, 1*time.Minute)
	defer close(done)

	// Demonstrate basic operations
	basicOperations(ctx, q)
}

func basicOperations(ctx context.Context, q *queue.SpannerQueue) {
	fmt.Println("=== Basic Queue Operations ===")

	// Enqueue a message
	msgID, err := q.Enqueue(ctx, queue.EnqueueParams{
		Payload:  "Hello, Ticker!",
		Priority: queue.PriorityNormal,
	})
	if err != nil {
		log.Fatalf("Failed to enqueue message: %v", err)
	}
	fmt.Printf("Enqueued message: %s\n", msgID)

	// Dequeue a message
	msgs, err := q.Dequeue(ctx, queue.DequeueParams{
		ConsumerID: "basic-consumer",
		BatchSize:  1,
	})
	if err != nil {
		log.Fatalf("Failed to dequeue message: %v", err)
	}

	if len(msgs) == 0 {
		fmt.Println("No messages available")
		return
	}

	msg := msgs[0]
	fmt.Printf("Received message: %s\n", msg.Payload)
	fmt.Printf("  ID: %s\n", msg.ID)
	fmt.Printf("  Priority: %d\n", msg.Priority)
	fmt.Printf("  Enqueued at: %s\n", msg.EnqueueTime.Format(time.RFC3339))
	fmt.Printf("  Delivery attempts: %d\n", msg.DeliveryAttempts)

	// Process the message
	fmt.Println("Processing message...")
	time.Sleep(500 * time.Millisecond) // Simulate processing

	// Acknowledge the message
	err = q.Acknowledge(ctx, queue.UpdateParams{
		MessageID:      msg.ID,
		ConsumerID:     "basic-consumer",
		ProcessingTime: 500 * time.Millisecond,
	})
	if err != nil {
		log.Fatalf("Failed to acknowledge message: %v", err)
	}
	fmt.Println("Message acknowledged successfully")

	// Try to dequeue again (should be empty now)
	msgs, err = q.Dequeue(ctx, queue.DequeueParams{
		ConsumerID: "basic-consumer",
		BatchSize:  1,
	})
	if err != nil {
		log.Fatalf("Failed to dequeue message: %v", err)
	}

	if len(msgs) == 0 {
		fmt.Println("Queue is now empty, as expected")
	} else {
		fmt.Printf("Unexpected: found %d messages still in queue\n", len(msgs))
	}
}
