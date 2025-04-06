# Ticker Examples and Tests

## Examples

- **[basic_usage.go](../examples/basic/basic_usage.go)**: Shows how to set up a basic queue, enqueue messages, dequeue and process messages, and acknowledge completion.

- **[advanced_features.go](../examples/advanced/advanced_features.go)**: Demonstrates advanced features including message priorities, routing, consumer groups, batch processing, delayed delivery, and message metadata.

## Tests

The `tests` directory contains comprehensive tests that demonstrate and validate Ticker's functionality:

### Core Functionality Tests

- **[basic_test.go](tests/basic_test.go)**: Tests basic queue operations (enqueue, dequeue, acknowledge)
- **[advanced_test.go](tests/advanced_test.go)**: Tests advanced features like priorities, routing, consumer groups, etc.
- **[reliability_test.go](tests/reliability_test.go)**: Tests reliability features including circuit breakers, throttling, and handling expired locks

### Setup for Testing

- **[test_utils.go](tests/test_utils.go)**: Utilities for setting up tests with the Cloud Spanner Emulator

## Running the Examples

### Prerequisites

1. Go 1.16 or higher
2. Google Cloud Spanner Emulator or a Spanner instance (required for tests)

### Running Basic Examples

```bash
# Run the basic usage example
go run basic_usage.go

# Run the advanced features example
go run advanced_features.go
```

### Running Tests with Spanner Emulator

The tests use the [Cloud Spanner Emulator](https://github.com/GoogleCloudPlatform/cloud-spanner-emulator) which allows for local testing without a real Spanner instance.

1. Install the emulator:

```bash
gcloud components install cloud-spanner-emulator
# Or with Docker:
# docker pull gcr.io/cloud-spanner-emulator/emulator
```

2. Run the tests:

```bash
cd tests
go test -v
```
