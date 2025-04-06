package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
)

// RequeueExpiredLocks finds and releases locks that have expired,
// making the messages available for processing again.
func (q *SpannerQueue) RequeueExpiredLocks(ctx context.Context) error {
	// Set a default visibility timeout if not provided
	visibilityTimeout := 30 * time.Minute

	// Calculate cutoff time for expired locks
	cutoffTime := time.Now().Add(-visibilityTimeout)

	stmt := spanner.Statement{
		SQL: fmt.Sprintf(`
			UPDATE %s
			SET locked_by = NULL,
			    locked_at = NULL
			WHERE acknowledged = false
			AND locked_by IS NOT NULL
			AND locked_at < @cutoffTime
			AND dead_letter = false
		`, q.tableName),
		Params: map[string]interface{}{
			"cutoffTime": cutoffTime,
		},
	}

	var rowCount int64
	_, err := q.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		count, err := txn.Update(ctx, stmt)
		if err != nil {
			return fmt.Errorf("failed to requeue expired locks: %w", err)
		}
		rowCount = count

		if rowCount > 0 {
			// Record metric for requeue operation
			metricMutation := spanner.InsertMap(q.metricsTableName, map[string]interface{}{
				"queue_name":   q.tableName,
				"metric_name":  string(MetricRequeued),
				"metric_value": rowCount,
				"timestamp":    spanner.CommitTimestamp,
				"labels": json.RawMessage(fmt.Sprintf(`{
					"reason": "expired_lock",
					"cutoff_time": %q,
					"count": %d
				}`, cutoffTime.Format(time.RFC3339), rowCount)),
			})

			return txn.BufferWrite([]*spanner.Mutation{metricMutation})
		}

		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

// RequeueExpiredLocksWithTimeout finds and releases locks that have expired,
// making the messages available for processing again, with a custom timeout.
func (q *SpannerQueue) RequeueExpiredLocksWithTimeout(ctx context.Context, visibilityTimeout time.Duration) error {
	if visibilityTimeout <= 0 {
		return fmt.Errorf("visibility timeout must be positive")
	}

	// Calculate cutoff time for expired locks
	cutoffTime := time.Now().Add(-visibilityTimeout)

	stmt := spanner.Statement{
		SQL: fmt.Sprintf(`
			UPDATE %s
			SET locked_by = NULL,
			    locked_at = NULL
			WHERE acknowledged = false
			AND locked_by IS NOT NULL
			AND locked_at < @cutoffTime
			AND dead_letter = false
		`, q.tableName),
		Params: map[string]interface{}{
			"cutoffTime": cutoffTime,
		},
	}

	var rowCount int64
	_, err := q.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		count, err := txn.Update(ctx, stmt)
		if err != nil {
			return fmt.Errorf("failed to requeue expired locks: %w", err)
		}
		rowCount = count

		if rowCount > 0 {
			// Record metric for requeue operation
			metricMutation := spanner.InsertMap(q.metricsTableName, map[string]interface{}{
				"queue_name":   q.tableName,
				"metric_name":  string(MetricRequeued),
				"metric_value": rowCount,
				"timestamp":    spanner.CommitTimestamp,
				"labels": json.RawMessage(fmt.Sprintf(`{
					"reason": "expired_lock_custom_timeout",
					"timeout_seconds": %d,
					"cutoff_time": %q,
					"count": %d
				}`, int(visibilityTimeout.Seconds()), cutoffTime.Format(time.RFC3339), rowCount)),
			})

			return txn.BufferWrite([]*spanner.Mutation{metricMutation})
		}

		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

// StartRequeueWorker starts a background goroutine that periodically
// requeues expired message locks.
func (q *SpannerQueue) StartRequeueWorker(ctx context.Context, interval time.Duration) chan struct{} {
	if interval <= 0 {
		interval = 1 * time.Minute
	}

	done := make(chan struct{})
	ticker := time.NewTicker(interval)

	// Also collect queue depth metrics at the same interval
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// Requeue expired locks
				if err := q.RequeueExpiredLocks(ctx); err != nil {
					// In a production system, this should use proper logging
					fmt.Printf("Error requeuing expired locks: %v\n", err)
				}

				// Collect queue depth metrics
				q.collectQueueDepthMetrics(ctx)

			case <-ctx.Done():
				close(done)
				return
			case <-done:
				return
			}
		}
	}()

	return done
}

// collectQueueDepthMetrics collects current queue depth metrics
func (q *SpannerQueue) collectQueueDepthMetrics(ctx context.Context) {
	// Query for current queue depth
	stmt := spanner.NewStatement(fmt.Sprintf(`
		SELECT
			COUNT(*) AS total_messages,
			SUM(CASE WHEN acknowledged = false AND locked_by IS NULL AND (visible_after IS NULL OR visible_after <= CURRENT_TIMESTAMP()) THEN 1 ELSE 0 END) AS available_messages,
			SUM(CASE WHEN acknowledged = false AND locked_by IS NOT NULL THEN 1 ELSE 0 END) AS in_flight_messages,
			SUM(CASE WHEN acknowledged = true THEN 1 ELSE 0 END) AS completed_messages,
			SUM(CASE WHEN dead_letter = true THEN 1 ELSE 0 END) AS dead_letter_messages
		FROM %s
	`, q.tableName))

	var totalCount, availableCount, inFlightCount, completedCount, deadLetterCount int64

	err := q.client.Single().Query(ctx, stmt).Do(func(row *spanner.Row) error {
		return row.Columns(&totalCount, &availableCount, &inFlightCount, &completedCount, &deadLetterCount)
	})

	if err != nil {
		// In a production system, this should use proper logging
		fmt.Printf("Error collecting queue depth metrics: %v\n", err)
		return
	}

	// Record metrics
	_, err = q.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		metricMutation := spanner.InsertMap(q.metricsTableName, map[string]interface{}{
			"queue_name":   q.tableName,
			"metric_name":  string(MetricQueueDepth),
			"metric_value": totalCount,
			"timestamp":    spanner.CommitTimestamp,
			"labels": json.RawMessage(fmt.Sprintf(`{
				"total": %d,
				"available": %d,
				"in_flight": %d,
				"completed": %d,
				"dead_letter": %d
			}`, totalCount, availableCount, inFlightCount, completedCount, deadLetterCount)),
		})

		return txn.BufferWrite([]*spanner.Mutation{metricMutation})
	})

	if err != nil {
		fmt.Printf("Error recording queue depth metrics: %v\n", err)
	}
}
