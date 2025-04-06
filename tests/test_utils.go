package tests

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"github.com/google/uuid"
	"google.golang.org/api/option"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	instancepb "google.golang.org/genproto/googleapis/spanner/admin/instance/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/TFMV/ticker/queue"
)

const (
	// Default emulator connection settings
	emulatorHost     = "localhost:9010"
	emulatorProject  = "emulator-project"
	emulatorInstance = "emulator-instance"
)

// TestEnv encapsulates the test environment for Ticker tests
type TestEnv struct {
	ProjectID     string
	InstanceID    string
	DatabaseID    string
	SpannerClient *spanner.Client
	Queue         *queue.SpannerQueue
	Ctx           context.Context
	Cancel        context.CancelFunc
	EmulatorCmd   *exec.Cmd
}

// SetupEmulatorEnv sets up the Cloud Spanner Emulator and returns a TestEnv
// Make sure to call Cleanup() when done
func SetupEmulatorEnv() (*TestEnv, error) {
	// Check if emulator is already running via environment variable
	emulatorAddr := os.Getenv("SPANNER_EMULATOR_HOST")
	if emulatorAddr == "" {
		emulatorAddr = emulatorHost
		os.Setenv("SPANNER_EMULATOR_HOST", emulatorAddr)
	}

	// Create cancelable context
	ctx, cancel := context.WithCancel(context.Background())

	// Create unique database ID for test isolation
	databaseID := fmt.Sprintf("ticker-test-db-%s", uuid.New().String()[:8])

	// Create gRPC client options for connecting to emulator (no authentication)
	opts := []option.ClientOption{
		option.WithEndpoint(emulatorAddr),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	}

	// Create instance admin client
	instanceAdmin, err := instance.NewInstanceAdminClient(ctx, opts...)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create instance admin client: %v", err)
	}
	defer instanceAdmin.Close()

	// Create instance if it doesn't exist
	instancePath := fmt.Sprintf("projects/%s/instances/%s", emulatorProject, emulatorInstance)
	_, err = instanceAdmin.GetInstance(ctx, &instancepb.GetInstanceRequest{
		Name: instancePath,
	})

	if err != nil {
		// Instance doesn't exist, create it
		createReq := &instancepb.CreateInstanceRequest{
			Parent:     fmt.Sprintf("projects/%s", emulatorProject),
			InstanceId: emulatorInstance,
			Instance: &instancepb.Instance{
				Config:      fmt.Sprintf("projects/%s/instanceConfigs/emulator-config", emulatorProject),
				DisplayName: "Test Instance",
				NodeCount:   1,
			},
		}
		op, err := instanceAdmin.CreateInstance(ctx, createReq)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("failed to create instance: %v", err)
		}
		_, err = op.Wait(ctx)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("failed to wait for instance creation: %v", err)
		}
	}

	// Create database admin client
	databaseAdmin, err := database.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create database admin client: %v", err)
	}
	defer databaseAdmin.Close()

	// Create database with schema
	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", emulatorProject, emulatorInstance, databaseID)
	createDbReq := &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", emulatorProject, emulatorInstance),
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", databaseID),
		ExtraStatements: []string{
			// Create messages table
			`CREATE TABLE messages (
				id STRING(36) NOT NULL,
				payload BYTES(MAX) NOT NULL,
				sequence_id INT64 NOT NULL,
				enqueue_time TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
				acknowledged BOOL NOT NULL,
				locked_by STRING(MAX),
				locked_at TIMESTAMP,
				delivery_attempts INT64 NOT NULL,
				visible_after TIMESTAMP,
				dead_letter BOOL NOT NULL,
				deduplication_key STRING(MAX),
				priority INT64 NOT NULL,
				route_key STRING(MAX),
				consumer_group STRING(MAX),
				metadata JSON,
				expiration_time TIMESTAMP,
			) PRIMARY KEY (id)`,
			// Create index on sequence_id for FIFO ordering
			`CREATE INDEX messages_by_sequence_id ON messages(sequence_id)`,
			// Create index for finding locked messages
			`CREATE INDEX messages_by_locked_at ON messages(locked_at)`,
			// Create index for deduplication
			`CREATE NULL_FILTERED INDEX messages_by_deduplication_key ON messages(deduplication_key)`,
			// Create index for consumer group filtering
			`CREATE NULL_FILTERED INDEX messages_by_consumer_group ON messages(consumer_group)`,
			// Create index for route key filtering
			`CREATE NULL_FILTERED INDEX messages_by_route_key ON messages(route_key)`,
			// Create index for visibility time filtering
			`CREATE NULL_FILTERED INDEX messages_by_visible_after ON messages(visible_after)`,
			// Create index for dead letter messages
			`CREATE INDEX messages_by_dead_letter ON messages(dead_letter)`,
			// Create metrics table
			`CREATE TABLE queue_metrics (
				queue_name STRING(MAX) NOT NULL,
				metric_name STRING(MAX) NOT NULL,
				metric_value INT64 NOT NULL,
				timestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
				labels JSON,
			) PRIMARY KEY (queue_name, metric_name, timestamp)`,
			// Create consumer health table
			`CREATE TABLE consumer_health (
				consumer_id STRING(MAX) NOT NULL,
				last_seen TIMESTAMP NOT NULL,
				status STRING(MAX) NOT NULL,
				success_count INT64 NOT NULL,
				error_count INT64 NOT NULL,
				consecutive_errors INT64 NOT NULL,
				avg_processing_time INT64,
				metadata JSON,
			) PRIMARY KEY (consumer_id)`,
		},
	}

	op, err := databaseAdmin.CreateDatabase(ctx, createDbReq)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create database: %v", err)
	}

	_, err = op.Wait(ctx)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to wait for database creation: %v", err)
	}

	// Create Spanner client
	client, err := spanner.NewClient(ctx, dbPath, opts...)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create spanner client: %v", err)
	}

	// Create queue instance
	q := queue.NewSpannerQueue(client, "messages")

	return &TestEnv{
		ProjectID:     emulatorProject,
		InstanceID:    emulatorInstance,
		DatabaseID:    databaseID,
		SpannerClient: client,
		Queue:         q,
		Ctx:           ctx,
		Cancel:        cancel,
	}, nil
}

// Cleanup releases all resources in the test environment
func (env *TestEnv) Cleanup() {
	if env.SpannerClient != nil {
		env.SpannerClient.Close()
	}

	if env.Cancel != nil {
		env.Cancel()
	}

	// If we started the emulator, stop it
	if env.EmulatorCmd != nil && env.EmulatorCmd.Process != nil {
		err := env.EmulatorCmd.Process.Kill()
		if err != nil {
			log.Printf("Failed to kill emulator process: %v", err)
		}
	}
}

// WaitForEmulatorReady waits for the emulator to be ready
func WaitForEmulatorReady(endpoint string) error {
	maxRetries := 10
	retryDelay := 500 * time.Millisecond

	for i := 0; i < maxRetries; i++ {
		conn, err := grpc.Dial(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(retryDelay)
	}

	return fmt.Errorf("emulator not ready after %d attempts", maxRetries)
}
