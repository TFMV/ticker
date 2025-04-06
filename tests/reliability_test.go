package tests

import (
	"context"
	"testing"
	"time"

	"github.com/TFMV/ticker/queue"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCircuitBreaker tests the circuit breaker functionality
func TestCircuitBreaker(t *testing.T) {
	// Set up test environment with emulator
	env, err := SetupEmulatorEnv()
	require.NoError(t, err, "Failed to set up emulator environment")
	defer env.Cleanup()

	// Configure the circuit breaker
	err = env.Queue.ConfigureCircuitBreaker(env.Ctx, queue.CircuitBreakerConfig{
		Enabled:                      true,
		ErrorThreshold:               0.5, // Open circuit after 50% errors
		ConsecutiveFailuresThreshold: 3,   // Or after 3 consecutive failures
		ResetTimeout:                 5 * time.Second,
	})
	require.NoError(t, err, "Failed to configure circuit breaker")

	// Generate a unique consumer ID for this test
	consumerID := "circuit-consumer-" + uuid.New().String()[:8]

	// Enqueue a few test messages
	msgCount := 5
	var messageIDs []string
	for i := 0; i < msgCount; i++ {
		msgID, err := env.Queue.Enqueue(env.Ctx, queue.EnqueueParams{
			Payload: "Circuit breaker test message " + uuid.New().String()[:8],
		})
		require.NoError(t, err, "Failed to enqueue message")
		messageIDs = append(messageIDs, msgID)
	}

	// Process first message successfully
	msgs, err := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
		ConsumerID: consumerID,
		BatchSize:  1,
	})
	require.NoError(t, err, "Failed to dequeue message")
	require.Len(t, msgs, 1, "Should have dequeued 1 message")

	// Acknowledge the message (success case)
	err = env.Queue.Acknowledge(env.Ctx, queue.UpdateParams{
		MessageID:  msgs[0].ID,
		ConsumerID: consumerID,
	})
	require.NoError(t, err, "Failed to acknowledge message")

	// Now simulate failures for the next 3 messages to trigger circuit breaker
	for i := 0; i < 3; i++ {
		msgs, err := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
			ConsumerID: consumerID,
			BatchSize:  1,
		})
		require.NoError(t, err, "Failed to dequeue message")
		require.Len(t, msgs, 1, "Should have dequeued 1 message")

		// Requeue with error to simulate failure
		err = env.Queue.Requeue(env.Ctx, queue.UpdateParams{
			MessageID:  msgs[0].ID,
			ConsumerID: consumerID,
			Error:      "Simulated processing error",
		})
		require.NoError(t, err, "Failed to requeue message with error")
	}

	// Get consumer health - circuit should be open now
	health, err := env.Queue.GetConsumerHealth(env.Ctx, consumerID)
	require.NoError(t, err, "Failed to get consumer health")
	assert.Equal(t, string(queue.ConsumerStatusCircuitOpen), health.Status,
		"Circuit breaker should be open after consecutive failures")
	assert.Equal(t, int64(3), health.ConsecutiveErrors, "Should have 3 consecutive errors")

	// Try to dequeue another message - should be rejected due to open circuit
	_, err = env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
		ConsumerID: consumerID,
		BatchSize:  1,
	})
	assert.Error(t, err, "Dequeue should be rejected when circuit is open")
	assert.Contains(t, err.Error(), "circuit breaker is open", "Error should mention circuit breaker")

	// Wait for reset timeout to expire
	time.Sleep(6 * time.Second)

	// Now we should be able to dequeue again
	msgs, err = env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
		ConsumerID: consumerID,
		BatchSize:  1,
	})
	assert.NoError(t, err, "Should be able to dequeue after circuit reset")

	if len(msgs) > 0 {
		// Acknowledge this message to show we can process again
		err = env.Queue.Acknowledge(env.Ctx, queue.UpdateParams{
			MessageID:  msgs[0].ID,
			ConsumerID: consumerID,
		})
		require.NoError(t, err, "Failed to acknowledge message after circuit reset")
	}

	// Check that health status is reset
	health, err = env.Queue.GetConsumerHealth(env.Ctx, consumerID)
	require.NoError(t, err, "Failed to get consumer health after reset")
	assert.Equal(t, string(queue.ConsumerStatusHealthy), health.Status, "Circuit breaker should be reset to healthy")
	assert.Equal(t, int64(0), health.ConsecutiveErrors, "Consecutive errors should be reset")
}

// TestThrottling tests the rate limiting functionality
func TestThrottling(t *testing.T) {
	// Set up test environment with emulator
	env, err := SetupEmulatorEnv()
	require.NoError(t, err, "Failed to set up emulator environment")
	defer env.Cleanup()

	// Configure throttling for rate limiting
	err = env.Queue.SetThrottling(env.Ctx, queue.ThrottleParams{
		Enabled:              true,
		MaxMessagesPerSecond: 5,  // Only allow 5 messages per second
		MaxBurstSize:         10, // Allow a burst of up to 10 messages
	})
	require.NoError(t, err, "Failed to configure throttling")

	// Generate a unique consumer ID for this test
	consumerID := "throttle-consumer-" + uuid.New().String()[:8]

	// Enqueue many messages
	messageCount := 20
	for i := 0; i < messageCount; i++ {
		_, err := env.Queue.Enqueue(env.Ctx, queue.EnqueueParams{
			Payload: "Throttle test message " + uuid.New().String()[:8],
		})
		require.NoError(t, err, "Failed to enqueue message")
	}

	// Try to dequeue a large batch exceeding the rate limit
	// This should return some messages but fewer than requested
	batchSize := 15 // Exceeds our rate limit of 5 per second
	msgs, err := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
		ConsumerID: consumerID,
		BatchSize:  batchSize,
	})

	// The operation should succeed but return fewer messages
	assert.NoError(t, err, "Dequeue should succeed but return fewer messages")
	assert.True(t, len(msgs) <= 10, "Should get no more than burst size of messages")

	// Record how many we got
	firstBatchSize := len(msgs)

	// Try to dequeue another large batch immediately
	// This should either fail or return very few messages
	msgs2, err := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
		ConsumerID: consumerID,
		BatchSize:  batchSize,
	})

	if err == nil {
		// If it didn't error, we should get very few messages
		assert.True(t, len(msgs2) < firstBatchSize,
			"Second batch should be smaller than first due to rate limiting")
	}

	// Route-specific throttling
	routeKey := "throttled-route"
	err = env.Queue.SetThrottling(env.Ctx, queue.ThrottleParams{
		Enabled:              true,
		MaxMessagesPerSecond: 2, // Lower rate limit for specific route
		MaxBurstSize:         3,
		RouteKeys:            []string{routeKey},
	})
	require.NoError(t, err, "Failed to configure route-specific throttling")

	// Enqueue messages to the throttled route
	for i := 0; i < 5; i++ {
		_, err := env.Queue.Enqueue(env.Ctx, queue.EnqueueParams{
			Payload:  "Route-throttled message " + uuid.New().String()[:8],
			RouteKey: routeKey,
		})
		require.NoError(t, err, "Failed to enqueue message to throttled route")
	}

	// Try to dequeue more than allowed from the throttled route
	routeMsgs, err := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
		ConsumerID: consumerID + "-route",
		RouteKeys:  []string{routeKey},
		BatchSize:  10,
	})

	// Should get limited number of messages
	assert.NoError(t, err, "Dequeue from throttled route should succeed but be limited")
	assert.True(t, len(routeMsgs) <= 3,
		"Should get no more than route burst size of messages, got %d", len(routeMsgs))
}

// TestRequeueExpiredLocks tests that locked messages are requeued after their locks expire
func TestRequeueExpiredLocks(t *testing.T) {
	// Set up test environment with emulator
	env, err := SetupEmulatorEnv()
	require.NoError(t, err, "Failed to set up emulator environment")
	defer env.Cleanup()

	// Generate unique consumer IDs for this test
	consumer1 := "lock-consumer1-" + uuid.New().String()[:8]
	consumer2 := "lock-consumer2-" + uuid.New().String()[:8]

	// Enqueue a test message
	msgID, err := env.Queue.Enqueue(env.Ctx, queue.EnqueueParams{
		Payload: "Lock expiration test message",
	})
	require.NoError(t, err, "Failed to enqueue message")

	// Dequeue with consumer1 but don't acknowledge
	msgs, err := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
		ConsumerID: consumer1,
		BatchSize:  1,
	})
	require.NoError(t, err, "Failed to dequeue message")
	require.Len(t, msgs, 1, "Should have dequeued 1 message")
	assert.Equal(t, msgID, msgs[0].ID, "Dequeued message ID should match")

	// Try to dequeue with consumer2 immediately - should get nothing since message is locked
	msgs2, err := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
		ConsumerID: consumer2,
		BatchSize:  1,
	})
	require.NoError(t, err, "Failed to attempt second dequeue")
	assert.Len(t, msgs2, 0, "Consumer2 should not see any messages while locked by consumer1")

	// Set a short lock duration for testing
	shortTimeout := 5 * time.Second

	// Create a context with a short timeout for this operation
	ctx, cancel := context.WithTimeout(env.Ctx, shortTimeout+2*time.Second)
	defer cancel()

	// Call RequeueExpiredLocksWithTimeout with very short timeout for testing
	err = env.Queue.RequeueExpiredLocksWithTimeout(ctx, shortTimeout)
	require.NoError(t, err, "Failed to requeue expired locks")

	// Wait for the lock to expire
	time.Sleep(shortTimeout + time.Second)

	// Run requeue again - should find and requeue our expired lock
	err = env.Queue.RequeueExpiredLocksWithTimeout(ctx, shortTimeout)
	require.NoError(t, err, "Failed to requeue expired locks")

	// Now consumer2 should be able to see the message
	msgs2, err = env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
		ConsumerID: consumer2,
		BatchSize:  1,
	})
	require.NoError(t, err, "Failed to dequeue after lock expiration")
	require.Len(t, msgs2, 1, "Should have dequeued 1 message after lock expiration")
	assert.Equal(t, msgID, msgs2[0].ID, "Dequeued message ID should match")

	// Message should now have delivery attempts = 2
	assert.Equal(t, int64(2), msgs2[0].DeliveryAttempts, "Message should have 2 delivery attempts")

	// Acknowledge the message
	err = env.Queue.Acknowledge(env.Ctx, queue.UpdateParams{
		MessageID:  msgs2[0].ID,
		ConsumerID: consumer2,
	})
	require.NoError(t, err, "Failed to acknowledge message")
}

// TestReplayMessages tests the replay functionality
func TestReplayMessages(t *testing.T) {
	// Set up test environment with emulator
	env, err := SetupEmulatorEnv()
	require.NoError(t, err, "Failed to set up emulator environment")
	defer env.Cleanup()

	// Generate a unique consumer ID for this test
	consumerID := "replay-consumer-" + uuid.New().String()[:8]

	// Test routes and timestamps
	route1 := "notifications"
	route2 := "payments"

	startTime := time.Now()

	// Add a few messages with different routes
	numMessages := 6
	messagesPerRoute := numMessages / 2

	// Enqueue messages to both routes
	for i := 0; i < messagesPerRoute; i++ {
		// Route 1 messages
		_, err := env.Queue.Enqueue(env.Ctx, queue.EnqueueParams{
			Payload:  "Message for " + route1 + " " + uuid.New().String()[:8],
			RouteKey: route1,
		})
		require.NoError(t, err, "Failed to enqueue message for route1")

		// Route 2 messages
		_, err = env.Queue.Enqueue(env.Ctx, queue.EnqueueParams{
			Payload:  "Message for " + route2 + " " + uuid.New().String()[:8],
			RouteKey: route2,
		})
		require.NoError(t, err, "Failed to enqueue message for route2")
	}

	// Process and acknowledge all messages for route1
	for i := 0; i < messagesPerRoute; i++ {
		msgs, err := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
			ConsumerID: consumerID,
			RouteKeys:  []string{route1},
			BatchSize:  1,
		})
		require.NoError(t, err, "Failed to dequeue message")

		if len(msgs) > 0 {
			err = env.Queue.Acknowledge(env.Ctx, queue.UpdateParams{
				MessageID:  msgs[0].ID,
				ConsumerID: consumerID,
			})
			require.NoError(t, err, "Failed to acknowledge message")
		}
	}

	// Process but DO NOT acknowledge any route2 messages
	for i := 0; i < messagesPerRoute; i++ {
		msgs, err := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
			ConsumerID: consumerID,
			RouteKeys:  []string{route2},
			BatchSize:  1,
		})
		require.NoError(t, err, "Failed to dequeue message")
		require.Len(t, msgs, 1, "Should have dequeued 1 message")

		// Don't acknowledge these messages
	}

	endTime := time.Now()

	// Set up replay for all processed messages in our time range
	replayCount, err := env.Queue.ReplayMessages(env.Ctx, queue.ReplayParams{
		StartTime:             startTime,
		EndTime:               endTime,
		RouteKeys:             []string{route1, route2},
		ResetDeliveryAttempts: true,
	})
	require.NoError(t, err, "Failed to replay messages")

	// Should have replayed messagesPerRoute*2 total messages
	// (messagesPerRoute acknowledged for route1 + messagesPerRoute locked for route2)
	assert.Equal(t, int64(messagesPerRoute*2), replayCount, "Should have replayed all messages")

	// Test that we can now dequeue all messages again
	// Both route1 (previously acknowledged) and route2 (previously locked) should be available again
	for _, routeName := range map[int]string{1: route1, 2: route2} {
		// Try to dequeue all messages for this route
		for i := 0; i < messagesPerRoute; i++ {
			msgs, err := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
				ConsumerID: consumerID + "-replay",
				RouteKeys:  []string{routeName},
				BatchSize:  1,
			})
			require.NoError(t, err, "Failed to dequeue replayed message for route "+routeName)
			require.Len(t, msgs, 1, "Should have dequeued 1 replayed message for route "+routeName)

			// Verify delivery attempts were reset (should be 1 for first attempt after replay)
			assert.Equal(t, int64(1), msgs[0].DeliveryAttempts,
				"Delivery attempts should be reset to 1 for route %s message", routeName)

			// Acknowledge to clean up
			err = env.Queue.Acknowledge(env.Ctx, queue.UpdateParams{
				MessageID:  msgs[0].ID,
				ConsumerID: consumerID + "-replay",
			})
			require.NoError(t, err, "Failed to acknowledge replayed message")
		}
	}
}
