package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
	"golang.org/x/time/rate"
	"google.golang.org/api/iterator"
)

// SpannerQueue implements the Queue interface using Google Cloud Spanner.
type SpannerQueue struct {
	client              *spanner.Client
	tableName           string
	metricsTableName    string
	consumerHealthTable string
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
		nextSeqFunc:         nextSeqFunc,
	}
}

// SetSequenceGenerator allows customizing how sequence IDs are generated.
func (q *SpannerQueue) SetSequenceGenerator(nextSeqFunc func() int64) {
	q.nextSeqFunc = nextSeqFunc
}

// Enqueue adds a message to the queue with priority and routing support.
func (q *SpannerQueue) Enqueue(ctx context.Context, params EnqueueParams) (string, error) {
	// Apply throttling if configured
	if q.throttleConfig != nil && q.throttleConfig.Enabled {
		// Initialize the throttling state if it doesn't exist
		if q.throttleState == nil {
			q.throttleState = &ThrottleState{
				limiter:       rate.NewLimiter(rate.Limit(q.throttleConfig.MaxMessagesPerSecond), q.throttleConfig.MaxBurstSize),
				routeLimiters: make(map[string]*rate.Limiter),
				groupLimiters: make(map[string]*rate.Limiter),
				mu:            sync.RWMutex{},
			}

			// Initialize route-specific limiters
			for _, route := range q.throttleConfig.RouteKeys {
				q.throttleState.routeLimiters[route] = rate.NewLimiter(
					rate.Limit(q.throttleConfig.MaxMessagesPerSecond),
					q.throttleConfig.MaxBurstSize,
				)
			}

			// Initialize consumer group-specific limiters
			for _, group := range q.throttleConfig.ConsumerGroups {
				q.throttleState.groupLimiters[group] = rate.NewLimiter(
					rate.Limit(q.throttleConfig.MaxMessagesPerSecond),
					q.throttleConfig.MaxBurstSize,
				)
			}
		}

		// Apply rate limiting
		q.throttleState.mu.RLock()

		// Check global limiter
		if !q.throttleState.limiter.Allow() {
			q.throttleState.mu.RUnlock()
			return "", fmt.Errorf("rate limit exceeded, try again later")
		}

		// Check route-specific limiter if applicable
		if params.RouteKey != "" {
			if limiter, exists := q.throttleState.routeLimiters[params.RouteKey]; exists {
				if !limiter.Allow() {
					q.throttleState.mu.RUnlock()
					return "", fmt.Errorf("rate limit exceeded for route %s, try again later", params.RouteKey)
				}
			}
		}

		// Check consumer group-specific limiter if applicable
		if params.ConsumerGroup != "" {
			if limiter, exists := q.throttleState.groupLimiters[params.ConsumerGroup]; exists {
				if !limiter.Allow() {
					q.throttleState.mu.RUnlock()
					return "", fmt.Errorf("rate limit exceeded for consumer group %s, try again later", params.ConsumerGroup)
				}
			}
		}

		q.throttleState.mu.RUnlock()
	}

	messageID := uuid.New().String()

	// Generate sequence ID for FIFO ordering
	sequenceID := q.nextSeqFunc()

	// Create map for the mutation
	values := map[string]interface{}{
		"id":                messageID,
		"sequence_id":       sequenceID,
		"payload":           params.Payload,
		"enqueue_time":      spanner.CommitTimestamp,
		"acknowledged":      false,
		"delivery_attempts": 0,
		"priority":          int64(params.Priority),
		"dead_letter":       false,
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

	if params.Metadata != nil {
		metadataBytes, err := json.Marshal(params.Metadata)
		if err != nil {
			return "", fmt.Errorf("failed to marshal metadata: %w", err)
		}
		values["metadata"] = metadataBytes
	}

	// Create the mutation with all values
	m := spanner.InsertOrUpdateMap(q.tableName, values)

	// Start a transaction to handle deduplication if needed
	_, err := q.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
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
