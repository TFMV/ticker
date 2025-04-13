# Ticker Queue Optimization Plan

This document outlines the performance optimizations implemented in the Ticker queue system to improve data locality, reduce latency, and enhance scalability using Cloud Spanner's interleaved tables and other best practices.

## Implemented Optimizations

### 1. Interleaved Tables for Improved Locality

#### Queue Metrics Interleaving

We've implemented interleaving of the `queue_metrics` table with a parent `queues` table to optimize metrics retrieval by queue name. This provides several benefits:

- Data for a specific queue and its metrics are physically co-located in Spanner storage
- Reduced latency when querying metrics for a specific queue
- Improved performance for time-series metrics analysis
- Cascade deletion of metrics when a queue is deleted

```sql
CREATE TABLE queues (
  queue_name STRING(MAX) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  description STRING(MAX),
  config JSON
) PRIMARY KEY (queue_name);

CREATE TABLE queue_metrics (
  queue_name STRING(MAX) NOT NULL,
  metric_name STRING(MAX) NOT NULL,
  metric_value INT64 NOT NULL,
  timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  labels JSON
) PRIMARY KEY (queue_name, metric_name, timestamp),
  INTERLEAVE IN PARENT queues ON DELETE CASCADE;
```

#### Consumer Health Interleaving

Similarly, we've interleaved the `consumer_health` table with a parent `consumers` table for optimal data locality:

- Health records are physically co-located with consumer metadata
- Improved performance for health monitoring and circuit breaker operations
- Simplified management of consumer lifecycle

```sql
CREATE TABLE consumers (
  consumer_id STRING(MAX) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  description STRING(MAX),
  metadata JSON
) PRIMARY KEY (consumer_id);

CREATE TABLE consumer_health (
  consumer_id STRING(MAX) NOT NULL,
  last_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  status STRING(50) NOT NULL,
  success_count INT64 NOT NULL DEFAULT 0,
  error_count INT64 NOT NULL DEFAULT 0,
  consecutive_errors INT64 NOT NULL DEFAULT 0,
  avg_processing_time INT64,
  metadata JSON
) PRIMARY KEY (consumer_id),
  INTERLEAVE IN PARENT consumers ON DELETE CASCADE;
```

### 2. Automatic Parent Record Creation

We've implemented automatic creation of parent records when child data is written:

- `ensureQueueExists`: Creates a queue record if it doesn't exist
- `ensureConsumerExists`: Creates a consumer record if it doesn't exist

This ensures referential integrity and simplifies API usage by transparently handling parent-child relationships.

### 3. Optimized Messages Table

For the messages table, we've chosen to keep it as a top-level table to maintain flexibility, but with optimizations:

- Using randomized UUIDs for message IDs to ensure good key distribution
- Optimized indexes for efficient querying patterns
- Sequence ID generation for FIFO ordering when needed

```sql
CREATE TABLE messages (
  id STRING(36) NOT NULL,
  enqueue_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  sequence_id INT64 NOT NULL,
  payload STRING(MAX) NOT NULL,
  -- other fields...
) PRIMARY KEY (id);
```

## Performance Implications

### Query Performance Improvements

#### Metrics Queries

Before:

```sql
SELECT * FROM queue_metrics WHERE queue_name = 'my-queue' ORDER BY timestamp DESC LIMIT 100;
```

- Required scanning potentially across multiple splits

After:

```sql
SELECT * FROM queue_metrics WHERE queue_name = 'my-queue' ORDER BY timestamp DESC LIMIT 100;
```

- Same query, but now reads only from the specific interleaved table split for 'my-queue'
- Significantly reduces the amount of data scanned
- Minimizes network traffic between Spanner nodes

#### Consumer Health Queries

Similar improvements for consumer health queries:

- Faster retrieval of health status for circuit breaker operations
- More efficient updates to consumer health stats

### Scalability Benefits

- Reduced hotspotting through randomized UUIDs
- Better parallelization of queue operations across multiple Spanner splits
- Each queue's metrics are isolated in their own physical storage area

### Other Optimizations

- Added DEFAULT values in schema to simplify insertions
- More descriptive constraints for better data integrity
- Better indexing strategies for common query patterns

## Implementation Notes

The optimization has been implemented in a backward-compatible way:

1. The schema creates new parent tables while maintaining existing child tables
2. Code has been updated to ensure parent records exist when needed
3. Queries leverage the new co-location benefits automatically

## Future Optimizations

Potential further optimizations include:

1. Shard-based partitioning for ultra-high throughput queues
2. Time-based partitioning for archival of older messages
3. Secondary indexes on high-cardinality fields for specialized query patterns
