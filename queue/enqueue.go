package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
)

// SpannerQueue implements the Queue interface using Google Cloud Spanner.
type SpannerQueue struct {
	client              *spanner.Client
	tableName           string
	metricsTableName    string
	consumerHealthTable string
	queuesTable         string
	consumersTable      string
	nextSeqFunc         func() int64
	throttleConfig      *ThrottleParams
	circuitConfig       *CircuitBreakerConfig
	throttleState       *ThrottleState
}

// NewSpannerQueue creates a new queue backed by Spanner.
func NewSpannerQueue(client *spanner.Client, tableName string) *SpannerQueue {
	if tableName == "" {
		tableName = "messages"
	}

	// Initialize with a clock-based sequence ID generator
	var seqCounter int64
	nextSeqFunc := func() int64 {
		// Combine current time in nanos with a counter to ensure uniqueness
		// even for messages created in the same nanosecond
		now := time.Now().UnixNano()
		seqCounter++
		return now*1000 + seqCounter%1000
	}

	return &SpannerQueue{
		client:              client,
		tableName:           tableName,
		metricsTableName:    "queue_metrics",
		consumerHealthTable: "consumer_health",
		queuesTable:         "queues",
		consumersTable:      "consumers",
		nextSeqFunc:         nextSeqFunc,
	}
}

// SetSequenceGenerator allows customizing how sequence IDs are generated.
func (q *SpannerQueue) SetSequenceGenerator(nextSeqFunc func() int64) {
	q.nextSeqFunc = nextSeqFunc
}

// ensureQueueExists creates the queue record in the parent table if it doesn't exist
func (q *SpannerQueue) ensureQueueExists(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
	// Check if queue exists
	stmt := spanner.NewStatement(fmt.Sprintf(`
		SELECT queue_name FROM %s WHERE queue_name = @queueName LIMIT 1
	`, q.queuesTable))
	stmt.Params["queueName"] = q.tableName

	iter := txn.Query(ctx, stmt)
	defer iter.Stop()

	_, err := iter.Next()
	if err == nil {
		// Queue exists
		return nil
	} else if err != iterator.Done {
		// Error occurred
		return fmt.Errorf("error checking queue existence: %w", err)
	}

	// Queue doesn't exist, create it
	m := spanner.InsertMap(q.queuesTable, map[string]interface{}{
		"queue_name": q.tableName,
		"created_at": spanner.CommitTimestamp,
		"description": fmt.Sprintf("Queue automatically created at %s",
			time.Now().Format(time.RFC3339)),
	})

	if err := txn.BufferWrite([]*spanner.Mutation{m}); err != nil {
		return fmt.Errorf("failed to create queue record: %w", err)
	}

	return nil
}

// ensureConsumerExists creates the consumer record in the parent table if it doesn't exist
func (q *SpannerQueue) ensureConsumerExists(ctx context.Context, txn *spanner.ReadWriteTransaction, consumerID string) error {
	// Check if consumer exists
	stmt := spanner.NewStatement(fmt.Sprintf(`
		SELECT consumer_id FROM %s WHERE consumer_id = @consumerID LIMIT 1
	`, q.consumersTable))
	stmt.Params["consumerID"] = consumerID

	iter := txn.Query(ctx, stmt)
	defer iter.Stop()

	_, err := iter.Next()
	if err == nil {
		// Consumer exists
		return nil
	} else if err != iterator.Done {
		// Error occurred
		return fmt.Errorf("error checking consumer existence: %w", err)
	}

	// Consumer doesn't exist, create it
	m := spanner.InsertMap(q.consumersTable, map[string]interface{}{
		"consumer_id": consumerID,
		"created_at":  spanner.CommitTimestamp,
		"description": fmt.Sprintf("Consumer automatically created at %s",
			time.Now().Format(time.RFC3339)),
	})

	if err := txn.BufferWrite([]*spanner.Mutation{m}); err != nil {
		return fmt.Errorf("failed to create consumer record: %w", err)
	}

	return nil
}

// Enqueue adds a message to the queue with priority and routing support.
func (q *SpannerQueue) Enqueue(ctx context.Context, params EnqueueParams) (string, error) {
	// Generate a unique message ID
	messageID := uuid.New().String()

	// Generate a sequence ID for ordered processing
	sequenceID := q.nextSeqFunc()

	// Create values map for the message
	values := map[string]interface{}{
		"id":           messageID,
		"payload":      params.Payload,
		"sequence_id":  sequenceID,
		"enqueue_time": spanner.CommitTimestamp,
		"acknowledged": false,
		"priority":     int64(params.Priority),
		"dead_letter":  false,
	}

	// Add optional fields if provided
	if params.DeduplicationKey != "" {
		values["deduplication_key"] = params.DeduplicationKey
	}

	if !params.VisibleAfter.IsZero() {
		values["visible_after"] = params.VisibleAfter
	}

	if params.RouteKey != "" {
		values["route_key"] = params.RouteKey
	}

	if params.ConsumerGroup != "" {
		values["consumer_group"] = params.ConsumerGroup
	}

	if len(params.Metadata) > 0 {
		metadataJSON, err := json.Marshal(params.Metadata)
		if err != nil {
			return "", fmt.Errorf("failed to marshal metadata: %w", err)
		}
		values["metadata"] = json.RawMessage(metadataJSON)
	}

	// Create the message mutation
	m := spanner.InsertMap(q.tableName, values)

	// Check throttling before enqueuing
	if q.throttleState != nil && q.throttleConfig != nil && q.throttleConfig.Enabled {
		allowed := true
		q.throttleState.mu.RLock()

		// Check global limiter
		if !q.throttleState.limiter.Allow() {
			allowed = false
		}

		// Check route-specific limiter if applicable
		if allowed && params.RouteKey != "" {
			if limiter, exists := q.throttleState.routeLimiters[params.RouteKey]; exists {
				if !limiter.Allow() {
					allowed = false
				}
			}
		}

		// Check consumer group-specific limiter if applicable
		if allowed && params.ConsumerGroup != "" {
			if limiter, exists := q.throttleState.groupLimiters[params.ConsumerGroup]; exists {
				if !limiter.Allow() {
					allowed = false
				}
			}
		}

		q.throttleState.mu.RUnlock()

		if !allowed {
			return "", fmt.Errorf("message enqueue throttled")
		}
	}

	_, err := q.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		// Ensure the queue parent record exists
		if err := q.ensureQueueExists(ctx, txn); err != nil {
			return err
		}

		// Check for existing message with same deduplication key if provided
		if params.DeduplicationKey != "" {
			stmt := spanner.NewStatement(fmt.Sprintf(`
				SELECT id FROM %s 
				WHERE deduplication_key = @dedupKey
				AND acknowledged = false
				LIMIT 1
			`, q.tableName))
			stmt.Params["dedupKey"] = params.DeduplicationKey

			iter := txn.Query(ctx, stmt)
			defer iter.Stop()

			row, err := iter.Next()
			if err == nil {
				// Found existing message, don't insert a new one
				var existingID string
				if err := row.Columns(&existingID); err != nil {
					return fmt.Errorf("error scanning deduplication result: %w", err)
				}
				// Return existing ID instead of inserting
				messageID = existingID
				return nil
			} else if err != iterator.Done {
				// Real error occurred
				return fmt.Errorf("error checking deduplication: %w", err)
			}
			// No existing message found, continue with insert
		}

		// Record metric for enqueue in the same transaction
		metricMutation := spanner.InsertMap(q.metricsTableName, map[string]interface{}{
			"queue_name":   q.tableName,
			"metric_name":  string(MetricEnqueued),
			"metric_value": 1,
			"timestamp":    spanner.CommitTimestamp,
			"labels": json.RawMessage(fmt.Sprintf(`{
				"priority": %d,
				"route_key": %q,
				"consumer_group": %q
			}`, params.Priority, params.RouteKey, params.ConsumerGroup)),
		})

		// Insert the message and record the metric
		return txn.BufferWrite([]*spanner.Mutation{m, metricMutation})
	})

	if err != nil {
		return "", fmt.Errorf("failed to enqueue message: %w", err)
	}

	return messageID, nil
}

// Close releases resources used by the queue.
func (q *SpannerQueue) Close() error {
	return nil // The Spanner client should be closed by the caller
}
