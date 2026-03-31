package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/TFMV/ticker/queue"
	"github.com/TFMV/ticker/spanner"
)

// This example demonstrates the basic usage of Ticker's queue system.
// It shows how to enqueue, dequeue, and acknowledge messages.
func main() {
	ctx := context.Background()

	// -----------------------------
	// Emulator setup (optional)
	// -----------------------------
	// If running locally:
	// export SPANNER_EMULATOR_HOST=localhost:9010
	//
	// Or uncomment for hard override:
	//
	// os.Setenv("SPANNER_EMULATOR_HOST", "localhost:9010")

	emulatorHost := os.Getenv("SPANNER_EMULATOR_HOST")
	if emulatorHost != "" {
		fmt.Printf("Using Spanner emulator at %s\n", emulatorHost)
	} else {
		fmt.Println("Using production Spanner")
	}

	// -----------------------------
	// Initialize Spanner client
	// -----------------------------
	client, err := spanner.NewClient(ctx, spanner.Config{
		ProjectID:    "your-project",
		InstanceID:   "your-instance",
		DatabaseID:   "your-database",
		EmulatorHost: emulatorHost, // 👈 critical for emulator support
	})
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// -----------------------------
	// Ensure schema exists
	// -----------------------------
	if err := client.EnsureSchema(ctx); err != nil {
		log.Fatalf("Failed to ensure schema: %v", err)
	}

	// -----------------------------
	// Create queue
	// -----------------------------
	q := queue.NewSpannerQueue(client.Client, "messages")

	// -----------------------------
	// Background requeue worker
	// -----------------------------
	done := q.StartRequeueWorker(ctx, 1*time.Minute)
	defer close(done)

	// -----------------------------
	// Run demo
	// -----------------------------
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

	// Simulate processing
	fmt.Println("Processing message...")
	time.Sleep(500 * time.Millisecond)

	// Acknowledge
	err = q.Acknowledge(ctx, queue.UpdateParams{
		MessageID:      msg.ID,
		ConsumerID:     "basic-consumer",
		ProcessingTime: 500 * time.Millisecond,
	})
	if err != nil {
		log.Fatalf("Failed to acknowledge message: %v", err)
	}

	fmt.Println("Message acknowledged successfully")

	// Verify queue is empty
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
