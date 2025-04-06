package tests

import (
	"testing"
	"time"

	"github.com/TFMV/ticker/queue"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBasicOperations tests basic enqueue, dequeue, and acknowledge operations
func TestBasicOperations(t *testing.T) {
	// Set up test environment with emulator
	env, err := SetupEmulatorEnv()
	require.NoError(t, err, "Failed to set up emulator environment")
	defer env.Cleanup()

	// Generate a unique consumer ID for this test
	consumerID := "test-consumer-" + uuid.New().String()[:8]

	// Test message payload
	testPayload := []byte("Hello, Ticker!")

	// 1. Test Enqueue
	msgID, err := env.Queue.Enqueue(env.Ctx, queue.EnqueueParams{
		Payload:  string(testPayload),
		Priority: queue.PriorityNormal,
	})
	require.NoError(t, err, "Failed to enqueue message")
	assert.NotEmpty(t, msgID, "Message ID should not be empty")

	// 2. Test Dequeue
	msgs, err := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
		ConsumerID: consumerID,
		BatchSize:  1,
	})
	require.NoError(t, err, "Failed to dequeue message")
	require.Len(t, msgs, 1, "Should have dequeued 1 message")

	// Verify message properties
	msg := msgs[0]
	assert.Equal(t, msgID, msg.ID, "Message ID should match")
	assert.Equal(t, testPayload, msg.Payload, "Message payload should match")
	assert.Equal(t, int64(queue.PriorityNormal), msg.Priority, "Message priority should match")
	assert.Equal(t, int64(1), msg.DeliveryAttempts, "Delivery attempts should be 1")
	assert.False(t, msg.Acknowledged, "Message should not be acknowledged")
	assert.NotZero(t, msg.EnqueueTime, "Enqueue time should be set")
	assert.True(t, msg.LockedAt.Valid, "Locked at should be valid")
	assert.True(t, msg.LockedBy.Valid, "Locked by should be valid")
	assert.Equal(t, consumerID, msg.LockedBy.StringVal, "Locked by should match consumer ID")

	// 3. Test Acknowledge
	err = env.Queue.Acknowledge(env.Ctx, queue.UpdateParams{
		MessageID:  msg.ID,
		ConsumerID: consumerID,
	})
	require.NoError(t, err, "Failed to acknowledge message")

	// 4. Verify message is no longer available
	msgs, err = env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
		ConsumerID: consumerID,
		BatchSize:  1,
	})
	require.NoError(t, err, "Failed to dequeue message")
	assert.Len(t, msgs, 0, "Should have no messages available after acknowledgment")
}

// TestRequeue tests requeing a message
func TestRequeue(t *testing.T) {
	// Set up test environment with emulator
	env, err := SetupEmulatorEnv()
	require.NoError(t, err, "Failed to set up emulator environment")
	defer env.Cleanup()

	// Generate a unique consumer ID for this test
	consumerID := "requeue-consumer-" + uuid.New().String()[:8]

	// Test message payload
	testPayload := []byte("Requeue Test Message")

	// 1. Enqueue a message
	msgID, err := env.Queue.Enqueue(env.Ctx, queue.EnqueueParams{
		Payload:  string(testPayload),
		Priority: queue.PriorityHigh,
	})
	require.NoError(t, err, "Failed to enqueue message")

	// 2. Dequeue the message
	msgs, err := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
		ConsumerID: consumerID,
		BatchSize:  1,
	})
	require.NoError(t, err, "Failed to dequeue message")
	require.Len(t, msgs, 1, "Should have dequeued 1 message")

	// 3. Requeue the message
	err = env.Queue.Requeue(env.Ctx, queue.UpdateParams{
		MessageID:  msgs[0].ID,
		ConsumerID: consumerID,
	})
	require.NoError(t, err, "Failed to requeue message")

	// 4. The message should be available again
	requeuedMsgs, err := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
		ConsumerID: consumerID,
		BatchSize:  1,
	})
	require.NoError(t, err, "Failed to dequeue requeued message")
	require.Len(t, requeuedMsgs, 1, "Should have dequeued 1 requeued message")

	// 5. Verify it's the same message
	assert.Equal(t, msgID, requeuedMsgs[0].ID, "Requeued message ID should match")
	assert.Equal(t, testPayload, requeuedMsgs[0].Payload, "Requeued message payload should match")
	assert.Equal(t, int64(queue.PriorityHigh), requeuedMsgs[0].Priority, "Requeued message priority should match")
	assert.Equal(t, int64(2), requeuedMsgs[0].DeliveryAttempts, "Delivery attempts should be 2")
}

// TestDeadLetter tests moving a message to the dead letter queue
func TestDeadLetter(t *testing.T) {
	// Set up test environment with emulator
	env, err := SetupEmulatorEnv()
	require.NoError(t, err, "Failed to set up emulator environment")
	defer env.Cleanup()

	// Generate a unique consumer ID for this test
	consumerID := "dlq-consumer-" + uuid.New().String()[:8]

	// Test message payload
	testPayload := []byte("Dead Letter Test Message")

	// 1. Enqueue a message
	_, err = env.Queue.Enqueue(env.Ctx, queue.EnqueueParams{
		Payload:  string(testPayload),
		Priority: queue.PriorityNormal,
	})
	require.NoError(t, err, "Failed to enqueue message")

	// 2. Dequeue the message
	msgs, err := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
		ConsumerID: consumerID,
		BatchSize:  1,
	})
	require.NoError(t, err, "Failed to dequeue message")
	require.Len(t, msgs, 1, "Should have dequeued 1 message")

	// 3. Move the message to the dead letter queue
	err = env.Queue.MoveToDeadLetter(env.Ctx, queue.UpdateParams{
		MessageID:  msgs[0].ID,
		ConsumerID: consumerID,
		Error:      "Test error - moving to dead letter queue",
	})
	require.NoError(t, err, "Failed to move message to dead letter queue")

	// 4. The message should not be available for normal dequeuing
	moreMsgs, err := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
		ConsumerID: consumerID,
		BatchSize:  1,
	})
	require.NoError(t, err, "Failed to check for messages")
	assert.Len(t, moreMsgs, 0, "Should have no more messages available for normal dequeue")

	// 5. Verify metrics were recorded for the dead letter operation
	metrics, err := env.Queue.GetMetrics(env.Ctx, queue.MetricDeadLettered, time.Now().Add(-1*time.Hour), time.Now())
	require.NoError(t, err, "Failed to get metrics")
	assert.NotEmpty(t, metrics, "Should have dead letter metrics")

	// Verify at least one dead letter metric exists with the correct message ID
	foundMetric := false
	for _, metric := range metrics {
		if metric["metric_name"] == string(queue.MetricDeadLettered) {
			foundMetric = true
			break
		}
	}
	assert.True(t, foundMetric, "Should have found a dead letter metric")
}

// TestDelayedDelivery tests delayed message delivery
func TestDelayedDelivery(t *testing.T) {
	// Set up test environment with emulator
	env, err := SetupEmulatorEnv()
	require.NoError(t, err, "Failed to set up emulator environment")
	defer env.Cleanup()

	// Generate a unique consumer ID for this test
	consumerID := "delay-consumer-" + uuid.New().String()[:8]

	// Test message payload
	testPayload := []byte("Delayed Test Message")

	// Set a short delay for testing (2 seconds)
	delay := 2 * time.Second
	visibleAfter := time.Now().Add(delay)

	// 1. Enqueue a message with delayed visibility
	msgID, err := env.Queue.Enqueue(env.Ctx, queue.EnqueueParams{
		Payload:      string(testPayload),
		Priority:     queue.PriorityNormal,
		VisibleAfter: visibleAfter,
	})
	require.NoError(t, err, "Failed to enqueue delayed message")

	// 2. Try to dequeue immediately (should get no messages)
	msgs, err := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
		ConsumerID: consumerID,
		BatchSize:  1,
	})
	require.NoError(t, err, "Failed to dequeue")
	assert.Len(t, msgs, 0, "Should have no messages available before delay expires")

	// 3. Wait for the delay to expire
	time.Sleep(delay + 500*time.Millisecond)

	// 4. Try to dequeue again (should get the message)
	msgs, err = env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
		ConsumerID: consumerID,
		BatchSize:  1,
	})
	require.NoError(t, err, "Failed to dequeue after delay")
	require.Len(t, msgs, 1, "Should have dequeued 1 message after delay expired")

	// 5. Verify it's the same message
	assert.Equal(t, msgID, msgs[0].ID, "Message ID should match")
	assert.Equal(t, testPayload, msgs[0].Payload, "Message payload should match")
}
