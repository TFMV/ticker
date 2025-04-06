// Package queue provides a durable, exactly-once delivery queue system
// backed by Google Cloud Spanner.
package queue

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"
)

// Priority defines message processing priority levels.
type Priority int64

const (
	// PriorityLow indicates messages that can be processed with lower urgency.
	PriorityLow Priority = 0

	// PriorityNormal is the default priority level.
	PriorityNormal Priority = 100

	// PriorityHigh indicates messages that should be processed before normal messages.
	PriorityHigh Priority = 200

	// PriorityCritical indicates messages that should be processed immediately.
	PriorityCritical Priority = 300
)

// ConsumerStatus represents the health status of a consumer.
type ConsumerStatus string

const (
	// ConsumerStatusHealthy indicates a properly functioning consumer.
	ConsumerStatusHealthy ConsumerStatus = "HEALTHY"

	// ConsumerStatusDegraded indicates a consumer with some errors but still functional.
	ConsumerStatusDegraded ConsumerStatus = "DEGRADED"

	// ConsumerStatusFailing indicates a consumer with persistent errors.
	ConsumerStatusFailing ConsumerStatus = "FAILING"

	// ConsumerStatusCircuitOpen indicates a consumer that has been temporarily disabled.
	ConsumerStatusCircuitOpen ConsumerStatus = "CIRCUIT_OPEN"
)

// Message represents a queue message in the Spanner database.
type Message struct {
	// ID is the unique identifier for the message
	ID string `spanner:"id"`

	// EnqueueTime is when the message was added to the queue
	EnqueueTime time.Time `spanner:"enqueue_time"`

	// SequenceID ensures FIFO ordering of messages
	SequenceID int64 `spanner:"sequence_id"`

	// Payload is the message content
	Payload string `spanner:"payload"`

	// DeduplicationKey is an optional field for ensuring idempotent enqueues
	DeduplicationKey spanner.NullString `spanner:"deduplication_key"`

	// LockedBy identifies which consumer has the message locked
	LockedBy spanner.NullString `spanner:"locked_by"`

	// LockedAt is when the message was locked by a consumer
	LockedAt spanner.NullTime `spanner:"locked_at"`

	// Acknowledged indicates whether the message has been successfully processed
	Acknowledged bool `spanner:"acknowledged"`

	// DeliveryAttempts tracks how many times the message has been dequeued
	DeliveryAttempts int64 `spanner:"delivery_attempts"`

	// VisibleAfter allows for delayed processing of messages
	VisibleAfter spanner.NullTime `spanner:"visible_after"`

	// Priority determines the order in which messages are processed
	Priority int64 `spanner:"priority"`

	// RouteKey is used for message routing to specific consumers
	RouteKey spanner.NullString `spanner:"route_key"`

	// ConsumerGroup specifies which group of consumers can process this message
	ConsumerGroup spanner.NullString `spanner:"consumer_group"`

	// DeadLetter indicates if the message has been moved to the dead letter queue
	DeadLetter bool `spanner:"dead_letter"`

	// DeadLetterReason provides context for why the message was moved to DLQ
	DeadLetterReason spanner.NullString `spanner:"dead_letter_reason"`

	// LastError stores the most recent error encountered during processing
	LastError spanner.NullString `spanner:"last_error"`

	// ProcessingTime records how long processing took in milliseconds
	ProcessingTime spanner.NullInt64 `spanner:"processing_time"`

	// Metadata contains additional arbitrary message attributes
	Metadata spanner.NullJSON `spanner:"metadata"`
}

// MessageMetadata represents arbitrary message attributes.
type MessageMetadata map[string]interface{}

// EnqueueParams contains parameters for enqueuing a new message.
type EnqueueParams struct {
	// Payload is the message content
	Payload string

	// DeduplicationKey is an optional field for ensuring idempotent enqueues
	DeduplicationKey string

	// VisibleAfter allows delaying when the message becomes available
	VisibleAfter time.Time

	// Priority determines message processing order (higher values = higher priority)
	Priority Priority

	// RouteKey directs the message to specific consumers
	RouteKey string

	// ConsumerGroup restricts which consumer group can process this message
	ConsumerGroup string

	// Metadata contains additional arbitrary message attributes
	Metadata MessageMetadata
}

// DequeueParams contains parameters for dequeuing messages.
type DequeueParams struct {
	// ConsumerID identifies the consumer requesting the message
	ConsumerID string

	// LockDuration specifies how long the message should be locked
	LockDuration time.Duration

	// BatchSize is the number of messages to dequeue at once
	BatchSize int

	// MaxWaitTime is how long to wait for a message if none are immediately available
	MaxWaitTime time.Duration

	// RouteKeys is a list of route keys this consumer is interested in
	RouteKeys []string

	// ConsumerGroup identifies which group this consumer belongs to
	ConsumerGroup string

	// MinPriority filters for messages with at least this priority
	MinPriority Priority

	// MaxBatchingDelay is the maximum time to wait while batching messages
	MaxBatchingDelay time.Duration
}

// UpdateParams contains parameters for updating message state.
type UpdateParams struct {
	// MessageID identifies the message to update
	MessageID string

	// ConsumerID is the consumer that has the message locked
	ConsumerID string

	// ProcessingTime records how long processing took
	ProcessingTime time.Duration

	// Error contains details of any error that occurred during processing
	Error string
}

// ReplayParams contains parameters for replaying messages.
type ReplayParams struct {
	// StartTime specifies the beginning time boundary for replay
	StartTime time.Time

	// EndTime specifies the ending time boundary for replay
	EndTime time.Time

	// RouteKeys filters messages by route key
	RouteKeys []string

	// ConsumerGroup filters messages by consumer group
	ConsumerGroup string

	// IncludeAcknowledged determines whether to include already processed messages
	IncludeAcknowledged bool

	// ResetDeliveryAttempts resets the delivery counter for replayed messages
	ResetDeliveryAttempts bool
}

// ThrottleParams contains parameters for throttling queue operations.
type ThrottleParams struct {
	// MaxMessagesPerSecond limits the rate of message processing
	MaxMessagesPerSecond int

	// MaxBurstSize allows temporary bursts above the rate limit
	MaxBurstSize int

	// Enabled turns throttling on or off
	Enabled bool

	// RouteKeys specifies which routes to apply throttling to
	RouteKeys []string

	// ConsumerGroups specifies which consumer groups to apply throttling to
	ConsumerGroups []string
}

// CircuitBreakerConfig contains configuration for the circuit breaker pattern.
type CircuitBreakerConfig struct {
	// ErrorThreshold is the percentage of errors that triggers the circuit breaker
	ErrorThreshold float64

	// ConsecutiveFailuresThreshold is the number of consecutive failures before opening
	ConsecutiveFailuresThreshold int

	// ResetTimeout is how long the circuit stays open before trying again
	ResetTimeout time.Duration

	// Enabled turns the circuit breaker on or off
	Enabled bool
}

// ConsumerHealth represents the health status of a message consumer.
type ConsumerHealth struct {
	// ConsumerID uniquely identifies the consumer
	ConsumerID string `spanner:"consumer_id"`

	// LastSeen records when the consumer was last active
	LastSeen time.Time `spanner:"last_seen"`

	// Status represents the consumer's current health status
	Status string `spanner:"status"`

	// SuccessCount tallies successful message processing
	SuccessCount int64 `spanner:"success_count"`

	// ErrorCount tallies failed message processing attempts
	ErrorCount int64 `spanner:"error_count"`

	// ConsecutiveErrors counts consecutive failures
	ConsecutiveErrors int64 `spanner:"consecutive_errors"`

	// AvgProcessingTime is the rolling average processing time in milliseconds
	AvgProcessingTime spanner.NullInt64 `spanner:"avg_processing_time"`

	// Metadata contains additional consumer information
	Metadata spanner.NullJSON `spanner:"metadata"`
}

// MetricType defines the types of metrics that can be collected.
type MetricType string

const (
	// MetricEnqueued counts messages added to the queue
	MetricEnqueued MetricType = "enqueued"

	// MetricDequeued counts messages removed from the queue
	MetricDequeued MetricType = "dequeued"

	// MetricAcknowledged counts successfully processed messages
	MetricAcknowledged MetricType = "acknowledged"

	// MetricRequeued counts messages put back in the queue
	MetricRequeued MetricType = "requeued"

	// MetricDeadLettered counts messages sent to DLQ
	MetricDeadLettered MetricType = "dead_lettered"

	// MetricProcessingTime measures message processing duration
	MetricProcessingTime MetricType = "processing_time"

	// MetricQueueDepth measures current queue size
	MetricQueueDepth MetricType = "queue_depth"

	// MetricConsumerErrors counts consumer processing errors
	MetricConsumerErrors MetricType = "consumer_errors"

	// MetricCircuitBreaker tracks circuit breaker state changes
	MetricCircuitBreaker MetricType = "circuit_breaker"
)

// Queue represents a message queue.
type Queue interface {
	// Enqueue adds a message to the queue
	Enqueue(ctx context.Context, params EnqueueParams) (string, error)

	// Dequeue retrieves and locks a message from the queue
	Dequeue(ctx context.Context, params DequeueParams) ([]*Message, error)

	// Acknowledge marks a message as successfully processed
	Acknowledge(ctx context.Context, params UpdateParams) error

	// Requeue releases a message lock, allowing it to be processed again
	Requeue(ctx context.Context, params UpdateParams) error

	// RequeueExpiredLocks finds and releases expired message locks
	RequeueExpiredLocks(ctx context.Context) error

	// MoveToDeadLetter moves a message to the dead letter queue
	MoveToDeadLetter(ctx context.Context, params UpdateParams) error

	// ReplayMessages reprocesses messages based on specific criteria
	ReplayMessages(ctx context.Context, params ReplayParams) (int64, error)

	// SetThrottling configures rate limiting for queue operations
	SetThrottling(ctx context.Context, params ThrottleParams) error

	// ConfigureCircuitBreaker sets up the circuit breaker for consumer failure detection
	ConfigureCircuitBreaker(ctx context.Context, config CircuitBreakerConfig) error

	// GetConsumerHealth retrieves health information for a specific consumer
	GetConsumerHealth(ctx context.Context, consumerID string) (*ConsumerHealth, error)

	// GetMetrics retrieves queue metrics for a specific time range
	GetMetrics(ctx context.Context, metricType MetricType, start, end time.Time) ([]map[string]interface{}, error)

	// Close releases resources used by the queue
	Close() error
}
