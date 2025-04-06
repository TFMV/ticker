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
)

// ReplayMessages reprocesses messages based on specific criteria.
// This is useful for disaster recovery or when logic errors need to be reprocessed.
func (q *SpannerQueue) ReplayMessages(ctx context.Context, params ReplayParams) (int64, error) {
	// Validate the parameters
	if params.StartTime.IsZero() {
		return 0, fmt.Errorf("start time is required")
	}

	if params.EndTime.IsZero() {
		// Default to current time if not specified
		params.EndTime = time.Now()
	}

	if params.EndTime.Before(params.StartTime) {
		return 0, fmt.Errorf("end time must be after start time")
	}

	// Construct the query with time constraints and optional filters
	var whereClauses []string
	whereClauses = append(whereClauses, "enqueue_time >= @startTime")
	whereClauses = append(whereClauses, "enqueue_time <= @endTime")

	// Whether to include acknowledged messages
	if !params.IncludeAcknowledged {
		whereClauses = append(whereClauses, "acknowledged = false")
	}

	// Add routing filter if specified
	if len(params.RouteKeys) > 0 {
		placeholders := make([]string, len(params.RouteKeys))
		for i := range params.RouteKeys {
			placeholders[i] = fmt.Sprintf("@routeKey%d", i)
		}
		routeFilter := fmt.Sprintf("route_key IN (%s)", strings.Join(placeholders, ", "))
		whereClauses = append(whereClauses, routeFilter)
	}

	// Add consumer group filter if specified
	if params.ConsumerGroup != "" {
		whereClauses = append(whereClauses, "consumer_group = @consumerGroup")
	}

	// Convert where clauses to SQL
	whereClause := strings.Join(whereClauses, " AND ")

	// Count how many messages will be replayed
	var replayCount int64
	countStmt := spanner.NewStatement(fmt.Sprintf(`
		SELECT COUNT(*) 
		FROM %s
		WHERE %s
	`, q.tableName, whereClause))

	// Add parameters
	countStmt.Params = map[string]interface{}{
		"startTime": params.StartTime,
		"endTime":   params.EndTime,
	}

	// Add route key parameters if specified
	for i, routeKey := range params.RouteKeys {
		countStmt.Params[fmt.Sprintf("routeKey%d", i)] = routeKey
	}

	// Add consumer group parameter if specified
	if params.ConsumerGroup != "" {
		countStmt.Params["consumerGroup"] = params.ConsumerGroup
	}

	// Execute the count query
	err := q.client.Single().Query(ctx, countStmt).Do(func(row *spanner.Row) error {
		return row.Column(0, &replayCount)
	})

	if err != nil {
		return 0, fmt.Errorf("failed to count replay messages: %w", err)
	}

	if replayCount == 0 {
		// No messages to replay
		return 0, nil
	}

	// Now build the update statement
	updateSQL := fmt.Sprintf(`
		UPDATE %s
		SET locked_by = NULL,
		    locked_at = NULL,
		    delivery_attempts = CASE WHEN @resetAttempts THEN 0 ELSE delivery_attempts END,
		    visible_after = NULL,
		    dead_letter = false
		WHERE %s
	`, q.tableName, whereClause)

	updateStmt := spanner.NewStatement(updateSQL)

	// Add parameters (same as count query, plus reset flag)
	updateStmt.Params = countStmt.Params
	updateStmt.Params["resetAttempts"] = params.ResetDeliveryAttempts

	// Execute the update
	_, err = q.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		rowCount, err := txn.Update(ctx, updateStmt)
		if err != nil {
			return fmt.Errorf("failed to update messages for replay: %w", err)
		}

		// Record metric for replay operation
		metricMutation := spanner.InsertMap(q.metricsTableName, map[string]interface{}{
			"queue_name":   q.tableName,
			"metric_name":  "message_replayed",
			"metric_value": rowCount,
			"timestamp":    spanner.CommitTimestamp,
			"labels": json.RawMessage(fmt.Sprintf(`{
				"start_time": %q,
				"end_time": %q,
				"include_acknowledged": %t,
				"reset_delivery_attempts": %t
			}`, params.StartTime.Format(time.RFC3339),
				params.EndTime.Format(time.RFC3339),
				params.IncludeAcknowledged,
				params.ResetDeliveryAttempts)),
		})

		return txn.BufferWrite([]*spanner.Mutation{metricMutation})
	})

	if err != nil {
		return 0, err
	}

	return replayCount, nil
}

// ThrottleState maintains the state of throttling for the queue.
type ThrottleState struct {
	limiter       *rate.Limiter
	routeLimiters map[string]*rate.Limiter
	groupLimiters map[string]*rate.Limiter
	mu            sync.RWMutex
}

// SetThrottling configures rate limiting for queue operations.
func (q *SpannerQueue) SetThrottling(ctx context.Context, params ThrottleParams) error {
	// Validate parameters
	if params.MaxMessagesPerSecond < 0 {
		return fmt.Errorf("max messages per second cannot be negative")
	}

	if params.MaxBurstSize <= 0 {
		// Default to 2x the per-second rate for reasonable bursting
		params.MaxBurstSize = params.MaxMessagesPerSecond * 2
		if params.MaxBurstSize <= 0 {
			params.MaxBurstSize = 1
		}
	}

	// Create or update throttle config
	if q.throttleConfig == nil {
		q.throttleConfig = &ThrottleParams{}
	}

	*q.throttleConfig = params

	// Record the throttling configuration in the database
	_, err := q.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		// Record metric for throttling configuration
		metricMutation := spanner.InsertMap(q.metricsTableName, map[string]interface{}{
			"queue_name":   q.tableName,
			"metric_name":  "throttling_configured",
			"metric_value": 1,
			"timestamp":    spanner.CommitTimestamp,
			"labels": json.RawMessage(fmt.Sprintf(`{
				"enabled": %t,
				"max_messages_per_second": %d,
				"max_burst_size": %d,
				"route_keys_count": %d,
				"consumer_groups_count": %d
			}`, params.Enabled,
				params.MaxMessagesPerSecond,
				params.MaxBurstSize,
				len(params.RouteKeys),
				len(params.ConsumerGroups))),
		})

		return txn.BufferWrite([]*spanner.Mutation{metricMutation})
	})

	return err
}

// ConfigureCircuitBreaker sets up the circuit breaker for consumer failure detection.
func (q *SpannerQueue) ConfigureCircuitBreaker(ctx context.Context, config CircuitBreakerConfig) error {
	// Validate parameters
	if config.ErrorThreshold < 0 || config.ErrorThreshold > 1.0 {
		return fmt.Errorf("error threshold must be between 0 and 1.0")
	}

	if config.ConsecutiveFailuresThreshold <= 0 {
		config.ConsecutiveFailuresThreshold = 5 // Reasonable default
	}

	if config.ResetTimeout <= 0 {
		config.ResetTimeout = 5 * time.Minute // Reasonable default
	}

	// Create or update circuit breaker config
	if q.circuitConfig == nil {
		q.circuitConfig = &CircuitBreakerConfig{}
	}

	*q.circuitConfig = config

	// Record the circuit breaker configuration in the database
	_, err := q.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		// Record metric for circuit breaker configuration
		metricMutation := spanner.InsertMap(q.metricsTableName, map[string]interface{}{
			"queue_name":   q.tableName,
			"metric_name":  "circuit_breaker_configured",
			"metric_value": 1,
			"timestamp":    spanner.CommitTimestamp,
			"labels": json.RawMessage(fmt.Sprintf(`{
				"enabled": %t,
				"error_threshold": %.2f,
				"consecutive_failures_threshold": %d,
				"reset_timeout_seconds": %d
			}`, config.Enabled,
				config.ErrorThreshold,
				config.ConsecutiveFailuresThreshold,
				int(config.ResetTimeout.Seconds()))),
		})

		return txn.BufferWrite([]*spanner.Mutation{metricMutation})
	})

	// If circuit breaker is enabled, start monitoring for expired circuits
	if config.Enabled && err == nil {
		go q.monitorCircuitBreaker(ctx, config.ResetTimeout)
	}

	return err
}

// monitorCircuitBreaker periodically checks for circuit breakers that should be reset.
func (q *SpannerQueue) monitorCircuitBreaker(ctx context.Context, resetTimeout time.Duration) {
	ticker := time.NewTicker(resetTimeout / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Find circuits that have been open longer than the reset timeout
			stmt := spanner.NewStatement(fmt.Sprintf(`
				UPDATE %s
				SET status = '%s',
				    consecutive_errors = 0
				WHERE status = '%s'
				AND last_seen < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL %d SECOND)
			`, q.consumerHealthTable,
				ConsumerStatusHealthy,
				ConsumerStatusCircuitOpen,
				int(resetTimeout.Seconds())))

			_, err := q.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
				rowCount, err := txn.Update(ctx, stmt)
				if err != nil {
					return err
				}

				if rowCount > 0 {
					// Record metric for circuit breaker resets
					metricMutation := spanner.InsertMap(q.metricsTableName, map[string]interface{}{
						"queue_name":   q.tableName,
						"metric_name":  string(MetricCircuitBreaker),
						"metric_value": rowCount,
						"timestamp":    spanner.CommitTimestamp,
						"labels": json.RawMessage(fmt.Sprintf(`{
							"status": "RESET",
							"count": %d
						}`, rowCount)),
					})
					return txn.BufferWrite([]*spanner.Mutation{metricMutation})
				}
				return nil
			})

			if err != nil {
				// Log the error but continue
				fmt.Printf("Error resetting circuit breakers: %v\n", err)
			}
		}
	}
}

// GetMetrics retrieves queue metrics for a specific time range.
func (q *SpannerQueue) GetMetrics(ctx context.Context, metricType MetricType, start, end time.Time) ([]map[string]interface{}, error) {
	if start.IsZero() {
		// Default to 1 hour ago
		start = time.Now().Add(-1 * time.Hour)
	}

	if end.IsZero() {
		// Default to current time
		end = time.Now()
	}

	stmt := spanner.NewStatement(fmt.Sprintf(`
		SELECT queue_name, metric_name, metric_value, timestamp, labels
		FROM %s
		WHERE queue_name = @queueName
		AND metric_name = @metricName
		AND timestamp >= @startTime
		AND timestamp <= @endTime
		ORDER BY timestamp DESC
	`, q.metricsTableName))

	stmt.Params = map[string]interface{}{
		"queueName":  q.tableName,
		"metricName": string(metricType),
		"startTime":  start,
		"endTime":    end,
	}

	var results []map[string]interface{}

	err := q.client.Single().Query(ctx, stmt).Do(func(row *spanner.Row) error {
		var queueName, metricName string
		var metricValue int64
		var timestamp time.Time
		var labelsJSON json.RawMessage

		if err := row.Columns(&queueName, &metricName, &metricValue, &timestamp, &labelsJSON); err != nil {
			return err
		}

		metric := map[string]interface{}{
			"queue_name":   queueName,
			"metric_name":  metricName,
			"metric_value": metricValue,
			"timestamp":    timestamp,
		}

		if len(labelsJSON) > 0 {
			var labelsMap map[string]interface{}
			if err := json.Unmarshal(labelsJSON, &labelsMap); err == nil {
				metric["labels"] = labelsMap
			}
		}

		results = append(results, metric)
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to retrieve metrics: %w", err)
	}

	return results, nil
}
