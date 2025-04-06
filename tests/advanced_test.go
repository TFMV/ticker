package tests

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/TFMV/ticker/queue"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMessagePriorities tests that messages are delivered in priority order
func TestMessagePriorities(t *testing.T) {
	// Set up test environment with emulator
	env, err := SetupEmulatorEnv()
	require.NoError(t, err, "Failed to set up emulator environment")
	defer env.Cleanup()

	// Generate a unique consumer ID for this test
	consumerID := "priority-consumer-" + uuid.New().String()[:8]

	// Define test priorities in reverse order (low to critical)
	priorities := []struct {
		priority queue.Priority
		name     string
	}{
		{queue.PriorityLow, "Low"},
		{queue.PriorityNormal, "Normal"},
		{queue.PriorityHigh, "High"},
		{queue.PriorityCritical, "Critical"},
	}

	// Enqueue messages with different priorities in reverse order
	var messageIDs []string
	for _, p := range priorities {
		msgID, err := env.Queue.Enqueue(env.Ctx, queue.EnqueueParams{
			Payload:  p.name + " priority message",
			Priority: p.priority,
		})
		require.NoError(t, err, "Failed to enqueue message with priority %s", p.name)
		messageIDs = append(messageIDs, msgID)
	}

	// Dequeue messages - they should come out in priority order (critical to low)
	expectedOrder := []queue.Priority{
		queue.PriorityCritical,
		queue.PriorityHigh,
		queue.PriorityNormal,
		queue.PriorityLow,
	}

	for i, expectedPriority := range expectedOrder {
		msgs, err := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
			ConsumerID: consumerID,
			BatchSize:  1,
		})
		require.NoError(t, err, "Failed to dequeue message")
		require.Len(t, msgs, 1, "Should have dequeued 1 message")

		// Verify priority matches expected order
		assert.Equal(t, int64(expectedPriority), msgs[0].Priority,
			"Message %d should have priority %d, got %d", i, expectedPriority, msgs[0].Priority)

		// Acknowledge the message
		err = env.Queue.Acknowledge(env.Ctx, queue.UpdateParams{
			MessageID:  msgs[0].ID,
			ConsumerID: consumerID,
		})
		require.NoError(t, err, "Failed to acknowledge message")
	}

	// Verify no more messages are available
	msgs, err := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
		ConsumerID: consumerID,
		BatchSize:  1,
	})
	require.NoError(t, err, "Failed to dequeue after all messages processed")
	assert.Len(t, msgs, 0, "Should have no more messages available")
}

// TestRoutingAndConsumerGroups tests message routing and consumer group restrictions
func TestRoutingAndConsumerGroups(t *testing.T) {
	// Set up test environment with emulator
	env, err := SetupEmulatorEnv()
	require.NoError(t, err, "Failed to set up emulator environment")
	defer env.Cleanup()

	// Define routes and consumer groups
	type routeConfig struct {
		routeKey      string
		consumerGroup string
		consumerID    string
	}

	routes := []routeConfig{
		{"payments", "payment-processors", "payment-worker-" + uuid.New().String()[:8]},
		{"notifications", "notification-handlers", "notification-worker-" + uuid.New().String()[:8]},
		{"analytics", "data-analysts", "analytics-worker-" + uuid.New().String()[:8]},
	}

	// Enqueue messages for each route/group
	for _, rc := range routes {
		msgID, err := env.Queue.Enqueue(env.Ctx, queue.EnqueueParams{
			Payload:       "Message for " + rc.routeKey,
			RouteKey:      rc.routeKey,
			ConsumerGroup: rc.consumerGroup,
		})
		require.NoError(t, err, "Failed to enqueue message for route %s", rc.routeKey)
		assert.NotEmpty(t, msgID, "Message ID should not be empty")
	}

	// Test that each consumer can only see messages for their route/group
	for _, rc := range routes {
		// First, verify this consumer can dequeue messages for its own route/group
		msgs, err := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
			ConsumerID:    rc.consumerID,
			RouteKeys:     []string{rc.routeKey},
			ConsumerGroup: rc.consumerGroup,
			BatchSize:     10,
		})
		require.NoError(t, err, "Failed to dequeue message for route %s", rc.routeKey)
		require.Len(t, msgs, 1, "Should have dequeued 1 message for route %s", rc.routeKey)

		// Verify message properties
		assert.Equal(t, "Message for "+rc.routeKey, string(msgs[0].Payload),
			"Message payload should match for route %s", rc.routeKey)

		// Now try to dequeue messages for a different route (should get none)
		for _, otherRC := range routes {
			if otherRC.routeKey == rc.routeKey {
				continue // Skip same route
			}

			wrongRouteMsgs, err := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
				ConsumerID:    rc.consumerID,
				RouteKeys:     []string{otherRC.routeKey},
				ConsumerGroup: rc.consumerGroup,
				BatchSize:     10,
			})
			require.NoError(t, err, "Failed to dequeue for wrong route")
			assert.Len(t, wrongRouteMsgs, 0,
				"Consumer for %s should not see messages for %s", rc.routeKey, otherRC.routeKey)
		}

		// Acknowledge the message
		err = env.Queue.Acknowledge(env.Ctx, queue.UpdateParams{
			MessageID:  msgs[0].ID,
			ConsumerID: rc.consumerID,
		})
		require.NoError(t, err, "Failed to acknowledge message for route %s", rc.routeKey)
	}
}

// TestBatchProcessing tests dequeuing and processing messages in batches
func TestBatchProcessing(t *testing.T) {
	// Set up test environment with emulator
	env, err := SetupEmulatorEnv()
	require.NoError(t, err, "Failed to set up emulator environment")
	defer env.Cleanup()

	// Generate a unique consumer ID for this test
	consumerID := "batch-consumer-" + uuid.New().String()[:8]

	// Enqueue multiple messages
	batchSize := 5
	messageIDs := make([]string, batchSize)

	for i := 0; i < batchSize; i++ {
		msgID, err := env.Queue.Enqueue(env.Ctx, queue.EnqueueParams{
			Payload:  "Batch message " + uuid.New().String()[:8],
			Priority: queue.PriorityNormal,
		})
		require.NoError(t, err, "Failed to enqueue batch message %d", i)
		messageIDs[i] = msgID
	}

	// Dequeue messages in a batch
	msgs, err := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
		ConsumerID: consumerID,
		BatchSize:  batchSize,
	})
	require.NoError(t, err, "Failed to dequeue batch")
	assert.Len(t, msgs, batchSize, "Should have dequeued %d messages", batchSize)

	// Verify all messages were dequeued and acknowledge them
	for _, msg := range msgs {
		// Acknowledge the message
		err = env.Queue.Acknowledge(env.Ctx, queue.UpdateParams{
			MessageID:  msg.ID,
			ConsumerID: consumerID,
		})
		require.NoError(t, err, "Failed to acknowledge message")
	}

	// Verify no more messages are available
	moreMsgs, err := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
		ConsumerID: consumerID,
		BatchSize:  batchSize,
	})
	require.NoError(t, err, "Failed to check for more messages")
	assert.Len(t, moreMsgs, 0, "Should have no more messages available")
}

// TestDeduplication tests message deduplication
func TestDeduplication(t *testing.T) {
	// Set up test environment with emulator
	env, err := SetupEmulatorEnv()
	require.NoError(t, err, "Failed to set up emulator environment")
	defer env.Cleanup()

	// Generate a unique consumer ID for this test
	consumerID := "dedup-consumer-" + uuid.New().String()[:8]

	// Generate a unique deduplication key
	dedupKey := "payment-" + uuid.New().String()[:8]

	// Enqueue first message with deduplication key
	msgID1, err := env.Queue.Enqueue(env.Ctx, queue.EnqueueParams{
		Payload:          "First message with dedup key: " + dedupKey,
		DeduplicationKey: dedupKey,
	})
	require.NoError(t, err, "Failed to enqueue first message")
	assert.NotEmpty(t, msgID1, "Message ID should not be empty")

	// Try to enqueue a second message with the same deduplication key
	msgID2, err := env.Queue.Enqueue(env.Ctx, queue.EnqueueParams{
		Payload:          "Second message with same dedup key (should be deduplicated)",
		DeduplicationKey: dedupKey,
	})
	require.NoError(t, err, "Failed to enqueue second message with same dedup key")

	// Verify we got the same message ID back
	assert.Equal(t, msgID1, msgID2, "Deduplication failed: got different IDs for messages with same dedup key")

	// Dequeue the message
	msgs, err := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
		ConsumerID: consumerID,
		BatchSize:  1,
	})
	require.NoError(t, err, "Failed to dequeue message")
	require.Len(t, msgs, 1, "Should have dequeued 1 message")

	// Verify it's the first message
	assert.Equal(t, msgID1, msgs[0].ID, "Dequeued message ID should match")

	// Acknowledge the message
	err = env.Queue.Acknowledge(env.Ctx, queue.UpdateParams{
		MessageID:  msgs[0].ID,
		ConsumerID: consumerID,
	})
	require.NoError(t, err, "Failed to acknowledge message")

	// Now enqueue another message with the same deduplication key (should work now that first is acknowledged)
	msgID3, err := env.Queue.Enqueue(env.Ctx, queue.EnqueueParams{
		Payload:          "Third message with same dedup key (after ack)",
		DeduplicationKey: dedupKey,
	})
	require.NoError(t, err, "Failed to enqueue third message")

	// ID should be different this time
	assert.NotEqual(t, msgID1, msgID3, "Should get a different ID after acknowledging previous message")

	// Dequeue the new message
	msgs, err = env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
		ConsumerID: consumerID,
		BatchSize:  1,
	})
	require.NoError(t, err, "Failed to dequeue third message")
	require.Len(t, msgs, 1, "Should have dequeued 1 message")
	assert.Equal(t, msgID3, msgs[0].ID, "Should have dequeued the third message")
}

// TestMessageMetadata tests storing and retrieving message metadata
func TestMessageMetadata(t *testing.T) {
	// Set up test environment with emulator
	env, err := SetupEmulatorEnv()
	require.NoError(t, err, "Failed to set up emulator environment")
	defer env.Cleanup()

	// Generate a unique consumer ID for this test
	consumerID := "metadata-consumer-" + uuid.New().String()[:8]

	// Define test metadata
	metadata := map[string]interface{}{
		"orderId":   "ORD-12345",
		"amount":    99.95,
		"currency":  "USD",
		"customer":  "CUS-678",
		"timestamp": time.Now().Unix(),
		"items": []map[string]interface{}{
			{
				"productId": "PROD-001",
				"quantity":  2,
				"price":     49.95,
			},
		},
	}

	// Enqueue message with metadata
	msgID, err := env.Queue.Enqueue(env.Ctx, queue.EnqueueParams{
		Payload:  "Message with metadata",
		Metadata: metadata,
	})
	require.NoError(t, err, "Failed to enqueue message with metadata")
	assert.NotEmpty(t, msgID, "Message ID should not be empty")

	// Dequeue the message
	msgs, err := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
		ConsumerID: consumerID,
		BatchSize:  1,
	})
	require.NoError(t, err, "Failed to dequeue message")
	require.Len(t, msgs, 1, "Should have dequeued 1 message")

	// Verify metadata is preserved
	msg := msgs[0]
	require.True(t, msg.Metadata.Valid, "Metadata should be valid")

	// Parse metadata JSON
	var receivedMetadata map[string]interface{}
	err = json.Unmarshal([]byte(msg.Metadata.String()), &receivedMetadata)
	require.NoError(t, err, "Failed to unmarshal metadata JSON")

	// Verify key metadata fields
	assert.Equal(t, "ORD-12345", receivedMetadata["orderId"], "Order ID should match")
	assert.Equal(t, "USD", receivedMetadata["currency"], "Currency should match")
	assert.Equal(t, "CUS-678", receivedMetadata["customer"], "Customer should match")

	// Verify nested items array exists
	items, ok := receivedMetadata["items"].([]interface{})
	require.True(t, ok, "Items should be an array")
	require.Len(t, items, 1, "Should have 1 item")

	item, ok := items[0].(map[string]interface{})
	require.True(t, ok, "Item should be an object")
	assert.Equal(t, "PROD-001", item["productId"], "Product ID should match")
}
