// Package spanner provides utilities for interacting with Google Cloud Spanner.
package spanner

import (
	"context"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// Config holds the configuration for a Spanner client.
type Config struct {
	ProjectID  string
	InstanceID string
	DatabaseID string

	// Optional: override emulator host manually (if empty, env var is used)
	EmulatorHost string

	// Optional client options (rarely needed now)
	Options []option.ClientOption
}

// Client wraps a Spanner client with additional functionality.
type Client struct {
	Client *spanner.Client
	admin  *database.DatabaseAdminClient
	config Config

	isEmulator bool
}

// NewClient creates a new client for Spanner.
func NewClient(ctx context.Context, config Config) (*Client, error) {
	dbPath := fmt.Sprintf(
		"projects/%s/instances/%s/databases/%s",
		config.ProjectID,
		config.InstanceID,
		config.DatabaseID,
	)

	// -----------------------------
	// Emulator detection (core change)
	// -----------------------------
	emulatorHost := config.EmulatorHost
	if emulatorHost == "" {
		emulatorHost = os.Getenv("SPANNER_EMULATOR_HOST")
	}

	var clientOpts []option.ClientOption
	var adminOpts []option.ClientOption

	isEmulator := emulatorHost != ""

	if isEmulator {
		// Required for emulator: disable auth
		clientOpts = append(clientOpts, option.WithoutAuthentication())
		adminOpts = append(adminOpts, option.WithoutAuthentication())
	}

	// Merge user-provided options last (allow overrides if needed)
	clientOpts = append(clientOpts, config.Options...)
	adminOpts = append(adminOpts, config.Options...)

	// -----------------------------
	// Create Spanner client
	// -----------------------------
	client, err := spanner.NewClient(ctx, dbPath, clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create spanner client: %w", err)
	}

	// -----------------------------
	// Create admin client
	// -----------------------------
	admin, err := database.NewDatabaseAdminClient(ctx, adminOpts...)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to create admin client: %w", err)
	}

	return &Client{
		Client:     client,
		admin:      admin,
		config:     config,
		isEmulator: isEmulator,
	}, nil
}

// Close closes the client and releases any resources.
func (c *Client) Close() {
	if c.Client != nil {
		c.Client.Close()
	}
	if c.admin != nil {
		c.admin.Close()
	}
}

// EnsureSchema creates necessary tables and indexes if they don't exist.
func (c *Client) EnsureSchema(ctx context.Context) error {
	op, err := c.admin.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database: c.Client.DatabaseName(),
		Statements: []string{
			`CREATE TABLE IF NOT EXISTS queues (
				queue_name STRING(MAX) NOT NULL,
				created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
				description STRING(MAX),
				config JSON
			) PRIMARY KEY (queue_name)`,

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
				labels JSON
			) PRIMARY KEY (queue_name, metric_name, timestamp),
			  INTERLEAVE IN PARENT queues ON DELETE CASCADE`,

			`CREATE TABLE IF NOT EXISTS consumers (
				consumer_id STRING(MAX) NOT NULL,
				created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
				description STRING(MAX),
				metadata JSON
			) PRIMARY KEY (consumer_id)`,

			`CREATE TABLE IF NOT EXISTS consumer_health (
				consumer_id STRING(MAX) NOT NULL,
				last_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
				status STRING(50) NOT NULL,
				success_count INT64 NOT NULL DEFAULT 0,
				error_count INT64 NOT NULL DEFAULT 0,
				consecutive_errors INT64 NOT NULL DEFAULT 0,
				avg_processing_time INT64,
				metadata JSON
			) PRIMARY KEY (consumer_id),
			  INTERLEAVE IN PARENT consumers ON DELETE CASCADE`,
		},
	})

	if err != nil {
		return fmt.Errorf("failed to update database DDL: %w", err)
	}

	if err := op.Wait(ctx); err != nil {
		return fmt.Errorf("failed to wait for schema update: %w", err)
	}

	return nil
}

// PingWithTimeout attempts to ping the Spanner database with a timeout.
func (c *Client) PingWithTimeout(ctx context.Context, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	iter := c.Client.Single().Read(ctx, "messages", spanner.Key{}, []string{"id"})
	defer iter.Stop()

	_, err := iter.Next()
	if err != nil && err != iterator.Done {
		return fmt.Errorf("failed to ping Spanner: %w", err)
	}

	return nil
}
