package queue

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

// Acknowledge marks a message as successfully processed.
func (q *SpannerQueue) Acknowledge(ctx context.Context, params UpdateParams) error {
	if params.MessageID == "" {
		return fmt.Errorf("message ID is required")
	}

	if params.ConsumerID == "" {
		return fmt.Errorf("consumer ID is required")
	}

	var processingTime int64
	if params.ProcessingTime > 0 {
		processingTime = params.ProcessingTime.Milliseconds()
	}

	// Execute the update as a read-write transaction to ensure consistency
	_, err := q.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		// First, get the message to make sure it exists and is locked by this consumer
		msgStmt := spanner.NewStatement(fmt.Sprintf(`
			SELECT id, priority, route_key, consumer_group
			FROM %s
			WHERE id = @id
			AND locked_by = @lockedBy
		`, q.tableName))
		msgStmt.Params["id"] = params.MessageID
		msgStmt.Params["lockedBy"] = params.ConsumerID

		msgIter := txn.Query(ctx, msgStmt)
		defer msgIter.Stop()

		// Check if the message exists and is locked by this consumer
		msgRow, err := msgIter.Next()
		if err != nil {
			if err == iterator.Done {
				return fmt.Errorf("no message found with ID %s locked by %s",
					params.MessageID, params.ConsumerID)
			}
			return fmt.Errorf("error querying message: %w", err)
		}

		var msgID string
		var priority int64
		var routeKey spanner.NullString
		var consumerGroup spanner.NullString

		if err := msgRow.Columns(&msgID, &priority, &routeKey, &consumerGroup); err != nil {
			return fmt.Errorf("error scanning message: %w", err)
		}

		// Now update the message status
		ackStmt := spanner.Statement{
			SQL: fmt.Sprintf(`
				UPDATE %s
				SET acknowledged = true,
				    processing_time = @processingTime
				WHERE id = @id
				AND locked_by = @lockedBy
			`, q.tableName),
			Params: map[string]interface{}{
				"id":             params.MessageID,
				"lockedBy":       params.ConsumerID,
				"processingTime": processingTime,
			},
		}

		rowCount, err := txn.Update(ctx, ackStmt)
		if err != nil {
			return fmt.Errorf("failed to acknowledge message: %w", err)
		}

		if rowCount == 0 {
			return fmt.Errorf("no message found with ID %s locked by %s",
				params.MessageID, params.ConsumerID)
		}

		// Update consumer health to track success
		healthStmt := spanner.Statement{
			SQL: fmt.Sprintf(`
				UPDATE %s
				SET success_count = success_count + 1,
				    consecutive_errors = 0,
				    last_seen = CURRENT_TIMESTAMP(),
				    status = CASE 
				               WHEN status = '%s' THEN '%s'
				               ELSE status 
				             END,
				    avg_processing_time = CASE
				                           WHEN avg_processing_time IS NULL THEN @processingTime
				                           ELSE (avg_processing_time * 0.8 + @processingTime * 0.2)
				                         END
				WHERE consumer_id = @consumerID
			`, q.consumerHealthTable,
				ConsumerStatusCircuitOpen, ConsumerStatusHealthy),
			Params: map[string]interface{}{
				"consumerID":     params.ConsumerID,
				"processingTime": processingTime,
			},
		}

		_, err = txn.Update(ctx, healthStmt)
		if err != nil {
			return fmt.Errorf("failed to update consumer health: %w", err)
		}

		// Record metric for acknowledgment
		metricMutation := spanner.InsertMap(q.metricsTableName, map[string]interface{}{
			"queue_name":   q.tableName,
			"metric_name":  string(MetricAcknowledged),
			"metric_value": 1,
			"timestamp":    spanner.CommitTimestamp,
			"labels": json.RawMessage(fmt.Sprintf(`{
				"consumer_id": %q,
				"priority": %d,
				"route_key": %q,
				"consumer_group": %q,
				"processing_time_ms": %d
			}`, params.ConsumerID, priority,
				routeKey.StringVal, consumerGroup.StringVal, processingTime)),
		})

		return txn.BufferWrite([]*spanner.Mutation{metricMutation})
	})

	if err != nil {
		return err
	}

	return nil
}

// Requeue releases a message lock without acknowledging the message,
// allowing it to be processed again.
func (q *SpannerQueue) Requeue(ctx context.Context, params UpdateParams) error {
	if params.MessageID == "" {
		return fmt.Errorf("message ID is required")
	}

	if params.ConsumerID == "" {
		return fmt.Errorf("consumer ID is required")
	}

	// Check if there's an error provided
	isError := params.Error != ""

	// Execute the update as a read-write transaction to ensure consistency
	_, err := q.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		// First, get the message to make sure it exists and is locked by this consumer
		msgStmt := spanner.NewStatement(fmt.Sprintf(`
			SELECT id, priority, route_key, consumer_group, delivery_attempts
			FROM %s
			WHERE id = @id
			AND locked_by = @lockedBy
		`, q.tableName))
		msgStmt.Params["id"] = params.MessageID
		msgStmt.Params["lockedBy"] = params.ConsumerID

		msgIter := txn.Query(ctx, msgStmt)
		defer msgIter.Stop()

		// Check if the message exists and is locked by this consumer
		msgRow, err := msgIter.Next()
		if err != nil {
			if err == iterator.Done {
				return fmt.Errorf("no message found with ID %s locked by %s",
					params.MessageID, params.ConsumerID)
			}
			return fmt.Errorf("error querying message: %w", err)
		}

		var msgID string
		var priority int64
		var routeKey spanner.NullString
		var consumerGroup spanner.NullString
		var deliveryAttempts int64

		if err := msgRow.Columns(&msgID, &priority, &routeKey, &consumerGroup, &deliveryAttempts); err != nil {
			return fmt.Errorf("error scanning message: %w", err)
		}

		// Build update parameters
		updateParams := map[string]interface{}{
			"id":       params.MessageID,
			"lockedBy": params.ConsumerID,
		}

		// Build SQL that conditionally includes error information
		var requeueSQL string
		if isError {
			requeueSQL = fmt.Sprintf(`
				UPDATE %s
				SET locked_by = NULL,
				    locked_at = NULL,
				    last_error = @error
				WHERE id = @id
				AND locked_by = @lockedBy
			`, q.tableName)
			updateParams["error"] = params.Error
		} else {
			requeueSQL = fmt.Sprintf(`
				UPDATE %s
				SET locked_by = NULL,
				    locked_at = NULL
				WHERE id = @id
				AND locked_by = @lockedBy
			`, q.tableName)
		}

		requeueStmt := spanner.Statement{
			SQL:    requeueSQL,
			Params: updateParams,
		}

		rowCount, err := txn.Update(ctx, requeueStmt)
		if err != nil {
			return fmt.Errorf("failed to requeue message: %w", err)
		}

		if rowCount == 0 {
			return fmt.Errorf("no message found with ID %s locked by %s",
				params.MessageID, params.ConsumerID)
		}

		mutations := []*spanner.Mutation{}

		// If error was provided, update consumer health to track the error
		if isError {
			// Get the current health status
			healthStmt := spanner.NewStatement(fmt.Sprintf(`
				SELECT consecutive_errors, error_count
				FROM %s
				WHERE consumer_id = @consumerID
			`, q.consumerHealthTable))
			healthStmt.Params["consumerID"] = params.ConsumerID

			healthIter := txn.Query(ctx, healthStmt)
			defer healthIter.Stop()

			var consecutiveErrors, errorCount int64
			healthRow, err := healthIter.Next()
			if err == nil {
				if err := healthRow.Columns(&consecutiveErrors, &errorCount); err != nil {
					return fmt.Errorf("error scanning health: %w", err)
				}
			} else if err != iterator.Done {
				return fmt.Errorf("error querying health: %w", err)
			}

			// Increment error metrics
			consecutiveErrors++
			errorCount++

			// Determine new status based on circuit breaker config
			newStatus := string(ConsumerStatusHealthy)
			if consecutiveErrors >= 5 && consecutiveErrors < 10 {
				newStatus = string(ConsumerStatusDegraded)
			} else if consecutiveErrors >= 10 && consecutiveErrors < 20 {
				newStatus = string(ConsumerStatusFailing)
			} else if consecutiveErrors >= 20 {
				// If circuit breaker is enabled and we've hit the threshold, open the circuit
				if q.circuitConfig != nil && q.circuitConfig.Enabled &&
					consecutiveErrors >= int64(q.circuitConfig.ConsecutiveFailuresThreshold) {
					newStatus = string(ConsumerStatusCircuitOpen)
				} else {
					newStatus = string(ConsumerStatusFailing)
				}
			}

			healthMutation := spanner.UpdateMap(q.consumerHealthTable, map[string]interface{}{
				"consumer_id":        params.ConsumerID,
				"error_count":        errorCount,
				"consecutive_errors": consecutiveErrors,
				"status":             newStatus,
				"last_seen":          spanner.CommitTimestamp,
			})

			mutations = append(mutations, healthMutation)

			// If it's a circuit breaker state change, record a metric
			if newStatus == string(ConsumerStatusCircuitOpen) {
				circuitMetric := spanner.InsertMap(q.metricsTableName, map[string]interface{}{
					"queue_name":   q.tableName,
					"metric_name":  string(MetricCircuitBreaker),
					"metric_value": 1,
					"timestamp":    spanner.CommitTimestamp,
					"labels": json.RawMessage(fmt.Sprintf(`{
						"consumer_id": %q,
						"status": "OPEN",
						"consecutive_errors": %d
					}`, params.ConsumerID, consecutiveErrors)),
				})
				mutations = append(mutations, circuitMetric)
			}
		}

		// Record metric for requeue operation
		requeueMetric := spanner.InsertMap(q.metricsTableName, map[string]interface{}{
			"queue_name":   q.tableName,
			"metric_name":  string(MetricRequeued),
			"metric_value": 1,
			"timestamp":    spanner.CommitTimestamp,
			"labels": json.RawMessage(fmt.Sprintf(`{
				"consumer_id": %q,
				"is_error": %t,
				"priority": %d,
				"route_key": %q,
				"consumer_group": %q,
				"delivery_attempts": %d
			}`, params.ConsumerID, isError, priority,
				routeKey.StringVal, consumerGroup.StringVal, deliveryAttempts)),
		})
		mutations = append(mutations, requeueMetric)

		return txn.BufferWrite(mutations)
	})

	if err != nil {
		return err
	}

	return nil
}

// MoveToDeadLetter moves a message to the dead letter queue.
func (q *SpannerQueue) MoveToDeadLetter(ctx context.Context, params UpdateParams) error {
	if params.MessageID == "" {
		return fmt.Errorf("message ID is required")
	}
	if params.ConsumerID == "" {
		return fmt.Errorf("consumer ID is required")
	}

	_, err := q.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		// Ensure queue parent record exists for metrics
		if err := q.ensureQueueExists(ctx, txn); err != nil {
			return err
		}

		// Ensure consumer parent record exists for health updates
		if err := q.ensureConsumerExists(ctx, txn, params.ConsumerID); err != nil {
			return err
		}

		// Get message details for metrics
		var priority int64
		var routeKey, consumerGroup spanner.NullString

		detailsStmt := spanner.NewStatement(fmt.Sprintf(`
			SELECT priority, route_key, consumer_group
			FROM %s
			WHERE id = @id
		`, q.tableName))
		detailsStmt.Params["id"] = params.MessageID

		iter := txn.Query(ctx, detailsStmt)
		defer iter.Stop()

		row, err := iter.Next()
		if err != nil {
			if err == iterator.Done {
				return fmt.Errorf("message not found: %s", params.MessageID)
			}
			return fmt.Errorf("error reading message details: %w", err)
		}

		if err := row.Columns(&priority, &routeKey, &consumerGroup); err != nil {
			return fmt.Errorf("error parsing message details: %w", err)
		}

		// Move the message to DLQ
		dlqStmt := spanner.Statement{
			SQL: fmt.Sprintf(`
				UPDATE %s
				SET dead_letter = true,
				    dead_letter_reason = @reason,
				    locked_by = NULL,
				    locked_at = NULL
				WHERE id = @id
				AND locked_by = @lockedBy
			`, q.tableName),
			Params: map[string]interface{}{
				"id":       params.MessageID,
				"lockedBy": params.ConsumerID,
				"reason":   params.Error,
			},
		}

		rowCount, err := txn.Update(ctx, dlqStmt)
		if err != nil {
			return fmt.Errorf("failed to move message to DLQ: %w", err)
		}

		if rowCount == 0 {
			return fmt.Errorf("no message found with ID %s locked by %s",
				params.MessageID, params.ConsumerID)
		}

		// Record metric for DLQ operation
		dlqMetric := spanner.InsertMap(q.metricsTableName, map[string]interface{}{
			"queue_name":   q.tableName,
			"metric_name":  string(MetricDeadLettered),
			"metric_value": 1,
			"timestamp":    spanner.CommitTimestamp,
			"labels": json.RawMessage(fmt.Sprintf(`{
				"consumer_id": %q,
				"priority": %d,
				"route_key": %q,
				"consumer_group": %q,
				"error": %q
			}`, params.ConsumerID, priority,
				routeKey.StringVal, consumerGroup.StringVal, params.Error)),
		})

		// Update consumer health to track the error
		healthUpdate := spanner.Statement{
			SQL: fmt.Sprintf(`
				UPDATE %s
				SET error_count = error_count + 1,
				    last_seen = CURRENT_TIMESTAMP()
				WHERE consumer_id = @consumerID
			`, q.consumerHealthTable),
			Params: map[string]interface{}{
				"consumerID": params.ConsumerID,
			},
		}

		_, err = txn.Update(ctx, healthUpdate)
		if err != nil {
			return fmt.Errorf("failed to update consumer health: %w", err)
		}

		return txn.BufferWrite([]*spanner.Mutation{dlqMetric})
	})

	if err != nil {
		return err
	}

	return nil
}

// GetConsumerHealth retrieves health information for a specific consumer.
func (q *SpannerQueue) GetConsumerHealth(ctx context.Context, consumerID string) (*ConsumerHealth, error) {
	if consumerID == "" {
		return nil, fmt.Errorf("consumer ID is required")
	}

	stmt := spanner.NewStatement(fmt.Sprintf(`
		SELECT consumer_id, last_seen, status, success_count, error_count, 
		       consecutive_errors, avg_processing_time, metadata
		FROM %s
		WHERE consumer_id = @consumerID
	`, q.consumerHealthTable))
	stmt.Params["consumerID"] = consumerID

	var health *ConsumerHealth

	err := q.client.Single().Query(ctx, stmt).Do(func(row *spanner.Row) error {
		health = &ConsumerHealth{}
		return row.ToStruct(health)
	})

	if err == iterator.Done {
		return nil, nil // Consumer not found
	} else if err != nil {
		return nil, fmt.Errorf("error retrieving consumer health: %w", err)
	}

	return health, nil
}
