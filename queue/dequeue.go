package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"golang.org/x/time/rate"
	"google.golang.org/api/iterator"
)

// Dequeue retrieves and locks messages from the queue with support for
// priorities, routing, and the circuit breaker pattern.
func (q *SpannerQueue) Dequeue(ctx context.Context, params DequeueParams) ([]*Message, error) {
	if params.BatchSize <= 0 {
		params.BatchSize = 1
	}

	if params.LockDuration <= 0 {
		params.LockDuration = 5 * time.Minute
	}

	if params.ConsumerID == "" {
		return nil, fmt.Errorf("consumer ID is required")
	}

	// Check the consumer's health status before processing
	// If circuit breaker is enabled and the circuit is open for this consumer,
	// we should not allow it to process more messages
	if q.circuitConfig != nil && q.circuitConfig.Enabled {
		health, err := q.GetConsumerHealth(ctx, params.ConsumerID)
		if err == nil && health != nil && health.Status == string(ConsumerStatusCircuitOpen) {
			return nil, fmt.Errorf("circuit breaker is open for consumer %s", params.ConsumerID)
		}
	}

	var messages []*Message
	var consumerHealth *ConsumerHealth

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

		// Check the global rate limiter first
		q.throttleState.mu.RLock()
		allowedCount := params.BatchSize

		// Apply global throttling
		if !q.throttleState.limiter.AllowN(time.Now(), params.BatchSize) {
			// Determine how many we can allow
			for allowedCount > 0 {
				if !q.throttleState.limiter.Allow() {
					break
				}
				allowedCount--
			}
		}

		// Apply route-specific throttling if applicable
		if allowedCount > 0 && len(params.RouteKeys) > 0 {
			for _, routeKey := range params.RouteKeys {
				if limiter, exists := q.throttleState.routeLimiters[routeKey]; exists {
					routeAllowed := allowedCount
					for routeAllowed > 0 {
						if !limiter.Allow() {
							break
						}
						routeAllowed--
					}

					if routeAllowed < allowedCount {
						allowedCount = routeAllowed
					}
				}
			}
		}

		// Apply consumer group throttling if applicable
		if allowedCount > 0 && params.ConsumerGroup != "" {
			if limiter, exists := q.throttleState.groupLimiters[params.ConsumerGroup]; exists {
				groupAllowed := allowedCount
				for groupAllowed > 0 {
					if !limiter.Allow() {
						break
					}
					groupAllowed--
				}

				if groupAllowed < allowedCount {
					allowedCount = groupAllowed
				}
			}
		}
		q.throttleState.mu.RUnlock()

		// If we can't process any messages due to throttling, return an error
		if allowedCount <= 0 {
			return nil, fmt.Errorf("rate limit exceeded for consumer %s, try again later", params.ConsumerID)
		}

		// Adjust batch size to respect the throttling limits
		params.BatchSize = allowedCount
	}

	// Construct the query with priority, routing, and consumer group support
	var whereClauses []string
	whereClauses = append(whereClauses, "acknowledged = false")
	whereClauses = append(whereClauses, "(locked_by IS NULL OR locked_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE))")
	whereClauses = append(whereClauses, "(visible_after IS NULL OR visible_after <= CURRENT_TIMESTAMP())")
	whereClauses = append(whereClauses, "dead_letter = false")

	// Add routing filter if specified
	routeFilter := ""
	if len(params.RouteKeys) > 0 {
		placeholders := make([]string, len(params.RouteKeys))
		for i := range params.RouteKeys {
			placeholders[i] = fmt.Sprintf("@routeKey%d", i)
		}
		routeFilter = fmt.Sprintf("(route_key IN (%s) OR route_key IS NULL)", strings.Join(placeholders, ", "))
		whereClauses = append(whereClauses, routeFilter)
	}

	// Add consumer group filter if specified
	if params.ConsumerGroup != "" {
		whereClauses = append(whereClauses, "(consumer_group = @consumerGroup OR consumer_group IS NULL)")
	}

	// Add priority filter if specified
	if params.MinPriority > 0 {
		whereClauses = append(whereClauses, "priority >= @minPriority")
	}

	// Convert where clauses to SQL
	whereClause := strings.Join(whereClauses, " AND ")

	// Use a read-write transaction to ensure consistency
	_, err := q.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		// Get or create consumer health record
		var err error
		consumerHealth, err = q.getOrCreateConsumerHealth(ctx, txn, params.ConsumerID)
		if err != nil {
			return fmt.Errorf("failed to get or create consumer health: %w", err)
		}

		// If consumer is in degraded status, reduce batch size to avoid overwhelming it
		if consumerHealth.Status == string(ConsumerStatusDegraded) && params.BatchSize > 1 {
			params.BatchSize = params.BatchSize / 2
			if params.BatchSize < 1 {
				params.BatchSize = 1
			}
		}

		// Query for available messages
		stmt := spanner.NewStatement(fmt.Sprintf(`
			SELECT id, enqueue_time, sequence_id, payload, 
			       deduplication_key, delivery_attempts, visible_after,
			       priority, route_key, consumer_group, metadata
			FROM %s
			WHERE %s
			ORDER BY priority DESC, sequence_id ASC
			LIMIT @batchSize
		`, q.tableName, whereClause))

		stmt.Params["batchSize"] = params.BatchSize

		// Add route key parameters if specified
		for i, routeKey := range params.RouteKeys {
			stmt.Params[fmt.Sprintf("routeKey%d", i)] = routeKey
		}

		// Add consumer group parameter if specified
		if params.ConsumerGroup != "" {
			stmt.Params["consumerGroup"] = params.ConsumerGroup
		}

		// Add priority parameter if specified
		if params.MinPriority > 0 {
			stmt.Params["minPriority"] = int64(params.MinPriority)
		}

		iter := txn.Query(ctx, stmt)
		defer iter.Stop()

		// Collect message IDs to lock
		var messageIDs []string
		for {
			row, err := iter.Next()
			if err != nil {
				if err == iterator.Done {
					break
				}
				return fmt.Errorf("error reading message row: %w", err)
			}

			var msg Message
			if err := row.ToStruct(&msg); err != nil {
				return fmt.Errorf("error parsing message: %w", err)
			}

			messageIDs = append(messageIDs, msg.ID)
			messages = append(messages, &msg)
		}

		if len(messageIDs) == 0 {
			// No messages available
			return nil
		}

		// Lock all messages found
		for _, id := range messageIDs {
			// Create an update statement that increments delivery_attempts
			stmt := spanner.Statement{
				SQL: fmt.Sprintf(`UPDATE %s SET 
					locked_by = @lockedBy, 
					locked_at = @lockedAt, 
					delivery_attempts = delivery_attempts + 1
				WHERE id = @id`, q.tableName),
				Params: map[string]interface{}{
					"lockedBy": params.ConsumerID,
					"lockedAt": time.Now(),
					"id":       id,
				},
			}

			// Execute the update directly
			_, err := txn.Update(ctx, stmt)
			if err != nil {
				return fmt.Errorf("failed to lock message %s: %w", id, err)
			}
		}

		// Record metrics for dequeue operation
		metricMutation := spanner.InsertMap(q.metricsTableName, map[string]interface{}{
			"queue_name":   q.tableName,
			"metric_name":  string(MetricDequeued),
			"metric_value": len(messageIDs),
			"timestamp":    spanner.CommitTimestamp,
			"labels": json.RawMessage(fmt.Sprintf(`{
				"consumer_id": %q,
				"consumer_group": %q,
				"batch_size": %d,
				"consumer_status": %q
			}`, params.ConsumerID, params.ConsumerGroup, len(messageIDs), consumerHealth.Status)),
		})

		// Update consumer health to indicate activity
		healthMutation := spanner.UpdateMap(q.consumerHealthTable, map[string]interface{}{
			"consumer_id": params.ConsumerID,
			"last_seen":   spanner.CommitTimestamp,
		})

		// Apply the metric and health mutations
		return txn.BufferWrite([]*spanner.Mutation{metricMutation, healthMutation})
	})

	if err != nil {
		return nil, fmt.Errorf("failed to dequeue messages: %w", err)
	}

	// If we found no messages, return an empty slice
	if len(messages) == 0 {
		return []*Message{}, nil
	}

	// Update the locked_by and locked_at fields in our returned messages
	for _, msg := range messages {
		msg.LockedBy = spanner.NullString{StringVal: params.ConsumerID, Valid: true}
		msg.LockedAt = spanner.NullTime{Time: time.Now(), Valid: true}
		msg.DeliveryAttempts++
	}

	return messages, nil
}

// getOrCreateConsumerHealth retrieves or creates a consumer health record.
func (q *SpannerQueue) getOrCreateConsumerHealth(ctx context.Context, txn *spanner.ReadWriteTransaction, consumerID string) (*ConsumerHealth, error) {
	// Try to get existing health record
	stmt := spanner.NewStatement(fmt.Sprintf(`
		SELECT consumer_id, last_seen, status, success_count, error_count, 
		       consecutive_errors, avg_processing_time, metadata
		FROM %s
		WHERE consumer_id = @consumerID
	`, q.consumerHealthTable))
	stmt.Params["consumerID"] = consumerID

	iter := txn.Query(ctx, stmt)
	defer iter.Stop()

	row, err := iter.Next()
	if err == nil {
		// Found existing record
		var health ConsumerHealth
		if err := row.ToStruct(&health); err != nil {
			return nil, fmt.Errorf("error parsing consumer health: %w", err)
		}
		return &health, nil
	} else if err != iterator.Done {
		// Real error occurred
		return nil, fmt.Errorf("error checking consumer health: %w", err)
	}

	// No record found, create a new one
	health := &ConsumerHealth{
		ConsumerID:        consumerID,
		LastSeen:          time.Now(),
		Status:            string(ConsumerStatusHealthy),
		SuccessCount:      0,
		ErrorCount:        0,
		ConsecutiveErrors: 0,
	}

	m := spanner.InsertMap(q.consumerHealthTable, map[string]interface{}{
		"consumer_id":        health.ConsumerID,
		"last_seen":          health.LastSeen,
		"status":             health.Status,
		"success_count":      health.SuccessCount,
		"error_count":        health.ErrorCount,
		"consecutive_errors": health.ConsecutiveErrors,
	})

	if err := txn.BufferWrite([]*spanner.Mutation{m}); err != nil {
		return nil, fmt.Errorf("failed to create consumer health: %w", err)
	}

	return health, nil
}
