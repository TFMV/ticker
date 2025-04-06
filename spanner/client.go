// Package spanner provides utilities for interacting with Google Cloud Spanner.
package spanner

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	dbadmin "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// Client wraps a Google Cloud Spanner client with additional functionality.
type Client struct {
	*spanner.Client
	admin *dbadmin.DatabaseAdminClient
}

// Config holds configuration options for the Spanner client.
type Config struct {
	ProjectID  string
	InstanceID string
	DatabaseID string
	// Optional client options
	Options []option.ClientOption
}

// NewClient creates a new Spanner client.
func NewClient(ctx context.Context, config Config) (*Client, error) {
	database := fmt.Sprintf("projects/%s/instances/%s/databases/%s",
		config.ProjectID, config.InstanceID, config.DatabaseID)

	client, err := spanner.NewClient(ctx, database, config.Options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Spanner client: %w", err)
	}

	admin, err := dbadmin.NewDatabaseAdminClient(ctx, config.Options...)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to create Spanner admin client: %w", err)
	}

	return &Client{
		Client: client,
		admin:  admin,
	}, nil
}

// Close closes the Spanner client and admin client.
func (c *Client) Close() {
	if c.Client != nil {
		c.Client.Close()
	}
	if c.admin != nil {
		c.admin.Close()
	}
}

// EnsureSchema ensures that the necessary schema for the queue exists.
// This will create tables if they don't exist.
func (c *Client) EnsureSchema(ctx context.Context) error {
	op, err := c.admin.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database: c.Client.DatabaseName(),
		Statements: []string{
			`CREATE TABLE IF NOT EXISTS messages (
				id STRING(36) NOT NULL,
				enqueue_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
				sequence_id INT64 NOT NULL,
				payload STRING(MAX) NOT NULL,
				deduplication_key STRING(MAX),
				locked_by STRING(MAX),
				locked_at TIMESTAMP,
				acknowledged BOOL NOT NULL DEFAULT false,
				delivery_attempts INT64 NOT NULL DEFAULT 0,
				visible_after TIMESTAMP,
				priority INT64 NOT NULL DEFAULT 0,
				route_key STRING(MAX),
				consumer_group STRING(MAX),
				dead_letter BOOL NOT NULL DEFAULT false,
				dead_letter_reason STRING(MAX),
				last_error STRING(MAX),
				processing_time INT64, 
				metadata JSON
			) PRIMARY KEY (id)`,
			`CREATE INDEX IF NOT EXISTS messages_by_status ON messages (
				acknowledged, 
				locked_by, 
				visible_after, 
				sequence_id
			)`,
			`CREATE INDEX IF NOT EXISTS messages_by_deduplication ON messages (
				deduplication_key
			) WHERE deduplication_key IS NOT NULL`,
			`CREATE INDEX IF NOT EXISTS messages_by_priority ON messages (
				priority DESC,
				acknowledged,
				locked_by,
				visible_after,
				enqueue_time
			) WHERE acknowledged = false`,
			`CREATE INDEX IF NOT EXISTS messages_by_route ON messages (
				route_key,
				acknowledged,
				locked_by,
				visible_after,
				priority DESC,
				enqueue_time
			) WHERE route_key IS NOT NULL AND acknowledged = false`,
			`CREATE INDEX IF NOT EXISTS messages_by_consumer_group ON messages (
				consumer_group,
				acknowledged,
				locked_by,
				visible_after,
				priority DESC,
				enqueue_time
			) WHERE consumer_group IS NOT NULL AND acknowledged = false`,
			`CREATE INDEX IF NOT EXISTS messages_by_dlq ON messages (
				dead_letter,
				enqueue_time,
				route_key
			) WHERE dead_letter = true`,
			`CREATE TABLE IF NOT EXISTS queue_metrics (
				queue_name STRING(MAX) NOT NULL,
				metric_name STRING(MAX) NOT NULL,
				metric_value INT64 NOT NULL,
				timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
				labels JSON,
			) PRIMARY KEY (queue_name, metric_name, timestamp)`,
			`CREATE TABLE IF NOT EXISTS consumer_health (
				consumer_id STRING(MAX) NOT NULL,
				last_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
				status STRING(50) NOT NULL,
				success_count INT64 NOT NULL DEFAULT 0,
				error_count INT64 NOT NULL DEFAULT 0,
				consecutive_errors INT64 NOT NULL DEFAULT 0,
				avg_processing_time INT64,
				metadata JSON,
			) PRIMARY KEY (consumer_id)`,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to update database DDL: %w", err)
	}

	// Wait for the schema update to complete
	if err := op.Wait(ctx); err != nil {
		return fmt.Errorf("failed to wait for schema update: %w", err)
	}

	return nil
}

// PingWithTimeout attempts to ping the Spanner database with a timeout.
func (c *Client) PingWithTimeout(ctx context.Context, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Execute a simple query to check connectivity
	iter := c.Client.Single().Read(ctx, "messages", spanner.Key{}, []string{"id"})
	defer iter.Stop()

	// We don't care about results, just checking if we can connect
	_, err := iter.Next()
	if err != nil && err != iterator.Done {
		return fmt.Errorf("failed to ping Spanner: %w", err)
	}

	return nil
}
