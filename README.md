# Ticker

A production-grade queue system written in Go, using Google Cloud Spanner as the backend.

## Features

### Core Functionality

- Global FIFO ordering of messages
- Exactly-once delivery guarantee
- Durable storage without external brokers
- Multi-region awareness via Spanner's native consistency
- Written entirely in Go

### Advanced Features

- **Message Priority Levels**: Support for Low, Normal, High, and Critical priorities
- **Message Routing**: Route messages to specific consumers using route keys
- **Consumer Groups**: Restrict message visibility to specific consumer groups
- **Batch Processing**: Configurable batch size for dequeuing multiple messages at once
- **Circuit Breaker Pattern**: Automatically detect and disable failing consumers
- **Dead Letter Queue (DLQ)**: Move problematic messages to a separate queue for analysis
- **Message Replay**: Reprocess messages from specific time periods or with specific routing keys
- **Deduplication**: Optional message deduplication using custom keys
- **Delayed Delivery**: Schedule messages to become visible at a future time
- **Comprehensive Telemetry**: Detailed metrics for all operations

### Operational Controls

- **Throttling**: Configurable rate limits for queue operations at multiple levels:
  - Queue-wide rate limiting
  - Route-specific rate limiting
  - Consumer group-specific rate limiting
- **Consumer Health Monitoring**: Track success rates, errors, and processing times
- **Visibility Timeout**: Configurable message lock duration

## Architecture

Ticker uses Google Cloud Spanner to provide a consistent, globally distributed queue:

- Messages are stored in a Spanner table with sequence IDs for ordering
- Atomic transactions ensure no message is lost or processed twice
- Lock-based visibility control prevents duplicate processing
- Automatic requeuing of expired locks ensures no message gets stuck
- Circuit breaker implementation protects system stability during failures

## Configuration Options

Ticker can be configured with the following options:

- **Queue name**: Default is "messages"
- **Lock duration**: How long a message remains locked during processing (default: 5 minutes)
- **Requeue interval**: How often to check for expired locks (default: 1 minute)
- **Circuit breaker settings**: Error thresholds, reset timeouts
- **Throttling settings**: Rate limits for different operation types
- **Batch size**: Number of messages to dequeue at once
- **Message priority**: Four priority levels from Low to Critical

## License

[MIT](LICENSE)
