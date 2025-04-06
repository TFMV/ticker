// Package main provides the main entry point for the Ticker server.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/TFMV/ticker/queue"
	"github.com/TFMV/ticker/spanner"
)

var (
	projectID  = flag.String("project", "", "Google Cloud Project ID")
	instanceID = flag.String("instance", "", "Spanner Instance ID")
	databaseID = flag.String("database", "", "Spanner Database ID")
	mode       = flag.String("mode", "demo", "Execution mode: 'demo' or 'server'")
)

func main() {
	flag.Parse()

	// Validate required flags
	if *projectID == "" || *instanceID == "" || *databaseID == "" {
		log.Fatalf("All of -project, -instance, and -database are required")
	}

	// Create a context that we can cancel on SIGTERM
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle termination signals
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-signalChan
		log.Printf("Received signal %v, shutting down...", sig)
		cancel()
	}()

	// Initialize Spanner client
	spannerClient, err := spanner.NewClient(ctx, spanner.Config{
		ProjectID:  *projectID,
		InstanceID: *instanceID,
		DatabaseID: *databaseID,
	})
	if err != nil {
		log.Fatalf("Failed to create Spanner client: %v", err)
	}
	defer spannerClient.Close()

	// Ensure the schema exists
	log.Println("Ensuring schema exists...")
	if err := spannerClient.EnsureSchema(ctx); err != nil {
		log.Fatalf("Failed to ensure schema: %v", err)
	}

	// Initialize the queue
	q := queue.NewSpannerQueue(spannerClient.Client, "messages")

	// Configure the circuit breaker
	if err := q.ConfigureCircuitBreaker(ctx, queue.CircuitBreakerConfig{
		Enabled:                      true,
		ErrorThreshold:               0.5,
		ConsecutiveFailuresThreshold: 5,
		ResetTimeout:                 30 * time.Second,
	}); err != nil {
		log.Printf("Warning: Failed to configure circuit breaker: %v", err)
	}

	// Configure throttling
	if err := q.SetThrottling(ctx, queue.ThrottleParams{
		Enabled:              true,
		MaxMessagesPerSecond: 100,
		MaxBurstSize:         200,
	}); err != nil {
		log.Printf("Warning: Failed to configure throttling: %v", err)
	}

	// Start the requeue worker to automatically unlock expired messages
	log.Println("Starting requeue worker...")
	done := q.StartRequeueWorker(ctx, 1*time.Minute)
	defer close(done)

	if *mode == "demo" {
		runDemo(ctx, q)
	} else {
		// In server mode, just keep running until terminated
		log.Println("Server initialized and ready. Press Ctrl+C to exit.")
		<-ctx.Done()
	}

	log.Println("Shutdown complete")
}

func runDemo(ctx context.Context, q *queue.SpannerQueue) {
	log.Println("Running demo mode...")

	// Demonstration of prioritized messages
	log.Println("Enqueuing messages with different priorities...")

	// Enqueue a low priority message
	lowPriorityID, err := q.Enqueue(ctx, queue.EnqueueParams{
		Payload:  "Low priority message",
		Priority: queue.PriorityLow,
	})
	if err != nil {
		log.Fatalf("Failed to enqueue low priority message: %v", err)
	}
	log.Printf("Enqueued low priority message with ID: %s", lowPriorityID)

	// Enqueue a high priority message
	highPriorityID, err := q.Enqueue(ctx, queue.EnqueueParams{
		Payload:  "High priority message",
		Priority: queue.PriorityHigh,
	})
	if err != nil {
		log.Fatalf("Failed to enqueue high priority message: %v", err)
	}
	log.Printf("Enqueued high priority message with ID: %s", highPriorityID)

	// Enqueue a message with routing
	routedMsgID, err := q.Enqueue(ctx, queue.EnqueueParams{
		Payload:  "Routed message",
		Priority: queue.PriorityNormal,
		RouteKey: "orders",
	})
	if err != nil {
		log.Fatalf("Failed to enqueue routed message: %v", err)
	}
	log.Printf("Enqueued routed message with ID: %s", routedMsgID)

	// Demonstrate consumer group restriction
	groupMsgID, err := q.Enqueue(ctx, queue.EnqueueParams{
		Payload:       "Group-specific message",
		Priority:      queue.PriorityNormal,
		ConsumerGroup: "payment-processors",
	})
	if err != nil {
		log.Fatalf("Failed to enqueue group message: %v", err)
	}
	log.Printf("Enqueued group-specific message with ID: %s", groupMsgID)

	// Demonstrate dequeue with priority - should get high priority message first
	log.Println("Dequeuing messages (should get high priority first)...")
	messages, err := q.Dequeue(ctx, queue.DequeueParams{
		ConsumerID:   "demo-consumer",
		LockDuration: 5 * time.Minute,
		BatchSize:    3, // Get multiple messages to demonstrate ordering
	})
	if err != nil {
		log.Fatalf("Failed to dequeue messages: %v", err)
	}

	for i, msg := range messages {
		log.Printf("Dequeued message %d: ID=%s, Priority=%d, Payload=%s",
			i+1, msg.ID, msg.Priority, msg.Payload)

		// Acknowledge the message
		if err := q.Acknowledge(ctx, queue.UpdateParams{
			MessageID:      msg.ID,
			ConsumerID:     "demo-consumer",
			ProcessingTime: 100 * time.Millisecond,
		}); err != nil {
			log.Fatalf("Failed to acknowledge message: %v", err)
		}
		log.Printf("Acknowledged message: %s", msg.ID)
	}

	// Demonstrate routing-specific dequeue
	log.Println("Dequeuing messages with specific routing key...")
	routedMessages, err := q.Dequeue(ctx, queue.DequeueParams{
		ConsumerID:   "order-consumer",
		LockDuration: 5 * time.Minute,
		BatchSize:    1,
		RouteKeys:    []string{"orders"},
	})
	if err != nil {
		log.Fatalf("Failed to dequeue routed messages: %v", err)
	}

	for _, msg := range routedMessages {
		log.Printf("Dequeued routed message: ID=%s, RouteKey=%s, Payload=%s",
			msg.ID, msg.RouteKey.StringVal, msg.Payload)

		// Requeue the message to simulate an error
		if err := q.Requeue(ctx, queue.UpdateParams{
			MessageID:  msg.ID,
			ConsumerID: "order-consumer",
			Error:      "Simulated processing error",
		}); err != nil {
			log.Fatalf("Failed to requeue message: %v", err)
		}
		log.Printf("Requeued message with error: %s", msg.ID)
	}

	// Demonstrate dead letter queue
	log.Println("Dequeuing and moving to dead letter queue...")

	// Dequeue it again (it should be available since we requeued it)
	dlqMessages, err := q.Dequeue(ctx, queue.DequeueParams{
		ConsumerID:   "order-consumer",
		LockDuration: 5 * time.Minute,
		BatchSize:    1,
		RouteKeys:    []string{"orders"},
	})
	if err != nil {
		log.Fatalf("Failed to dequeue DLQ candidate: %v", err)
	}

	for _, msg := range dlqMessages {
		// Move to dead letter queue
		if err := q.MoveToDeadLetter(ctx, queue.UpdateParams{
			MessageID:  msg.ID,
			ConsumerID: "order-consumer",
			Error:      "Maximum retries exceeded",
		}); err != nil {
			log.Fatalf("Failed to move to DLQ: %v", err)
		}
		log.Printf("Moved message to DLQ: %s", msg.ID)
	}

	// Demonstrate metrics retrieval
	log.Println("Retrieving queue metrics...")
	metrics, err := q.GetMetrics(ctx, queue.MetricEnqueued, time.Now().Add(-1*time.Hour), time.Now())
	if err != nil {
		log.Printf("Failed to retrieve metrics: %v", err)
	} else {
		log.Printf("Retrieved %d enqueue metrics", len(metrics))
		for i, metric := range metrics {
			if i < 3 { // Just show a few to avoid cluttering the output
				log.Printf("  Metric: %+v", metric)
			}
		}
	}

	// Demonstrate consumer health check
	log.Println("Checking consumer health...")
	health, err := q.GetConsumerHealth(ctx, "demo-consumer")
	if err != nil {
		log.Printf("Failed to get consumer health: %v", err)
	} else if health != nil {
		log.Printf("Consumer health: Status=%s, SuccessCount=%d, ErrorCount=%d",
			health.Status, health.SuccessCount, health.ErrorCount)
	} else {
		log.Println("Consumer health not found")
	}

	log.Println("Demo completed")
}
