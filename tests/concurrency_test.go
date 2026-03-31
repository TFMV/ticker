package tests

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TFMV/ticker/queue"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExactlyOnceDelivery_SingleConsumer(t *testing.T) {
	env, err := SetupEmulatorEnv()
	require.NoError(t, err, "Failed to set up emulator environment")
	defer env.Cleanup()

	consumerID := "exactly-once-consumer-" + uuid.New().String()[:8]

	msgID, err := env.Queue.Enqueue(env.Ctx, queue.EnqueueParams{
		Payload: "Exactly-once test message",
	})
	require.NoError(t, err, "Failed to enqueue message")

	var ackCount atomic.Int32
	var mu sync.Mutex
	ackIDs := make(map[string]bool)

	for i := 0; i < 10; i++ {
		msgs, err := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
			ConsumerID: consumerID,
			BatchSize:  1,
		})

		if err != nil {
			continue
		}

		if len(msgs) == 0 {
			continue
		}

		if msgs[0].ID == msgID {
			mu.Lock()
			if !ackIDs[msgID] {
				err := env.Queue.Acknowledge(env.Ctx, queue.UpdateParams{
					MessageID:  msgID,
					ConsumerID: consumerID,
				})
				if err == nil {
					ackIDs[msgID] = true
					ackCount.Add(1)
				}
			}
			mu.Unlock()
		}
	}

	assert.Equal(t, int32(1), ackCount.Load(), "Message should be acknowledged exactly once")

	msgs, _ := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
		ConsumerID: consumerID + "-check",
		BatchSize:  1,
	})
	assert.Len(t, msgs, 0, "No more messages should be available after ack")
}

func TestExactlyOnceDelivery_MultipleConsumers(t *testing.T) {
	env, err := SetupEmulatorEnv()
	require.NoError(t, err, "Failed to set up emulator environment")
	defer env.Cleanup()

	numConsumers := 10
	numMessages := 20
	var wg sync.WaitGroup
	var processedCount atomic.Int32
	var mu sync.Mutex
	processedIDs := make(map[string]bool)

	consumerIDs := make([]string, numConsumers)
	for i := 0; i < numConsumers; i++ {
		consumerIDs[i] = "concurrent-consumer-" + uuid.New().String()[:8]
	}

	for i := 0; i < numMessages; i++ {
		_, err := env.Queue.Enqueue(env.Ctx, queue.EnqueueParams{
			Payload: fmt.Sprintf("Message %d", i),
		})
		require.NoError(t, err, "Failed to enqueue message")
	}

	for _, consumerID := range consumerIDs {
		wg.Add(1)
		go func(consumer string) {
			defer wg.Done()
			for attempt := 0; attempt < 20; attempt++ {
				msgs, err := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
					ConsumerID:  consumer,
					BatchSize:   1,
					MaxWaitTime: 100 * time.Millisecond,
				})
				if err != nil || len(msgs) == 0 {
					time.Sleep(10 * time.Millisecond)
					continue
				}

				msg := msgs[0]
				mu.Lock()
				if !processedIDs[msg.ID] {
					err := env.Queue.Acknowledge(env.Ctx, queue.UpdateParams{
						MessageID:  msg.ID,
						ConsumerID: consumer,
					})
					if err == nil {
						processedIDs[msg.ID] = true
						processedCount.Add(1)
					}
				}
				mu.Unlock()

				time.Sleep(10 * time.Millisecond)
			}
		}(consumerID)
	}

	wg.Wait()

	assert.Equal(t, int32(numMessages), processedCount.Load(), "All messages should be processed exactly once")

	metrics, _ := env.Queue.GetMetrics(env.Ctx, queue.MetricAcknowledged, time.Now().Add(-1*time.Hour), time.Now())
	var totalAck int64
	for _, m := range metrics {
		totalAck += m["metric_value"].(int64)
	}
	assert.Equal(t, int64(numMessages), totalAck, "Metrics should show exactly-once delivery")
}

func TestNoLostMessages_ConcurrentEnqueueDequeue(t *testing.T) {
	env, err := SetupEmulatorEnv()
	require.NoError(t, err, "Failed to set up emulator environment")
	defer env.Cleanup()

	numMessages := 100
	numConsumers := 5

	var wg sync.WaitGroup

	var enqueuedCount atomic.Int32
	var processedCount atomic.Int32
	var mu sync.Mutex
	processedIDs := make(map[string]bool)

	for i := 0; i < numMessages; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, err := env.Queue.Enqueue(env.Ctx, queue.EnqueueParams{
				Payload: fmt.Sprintf("Message %d", idx),
			})
			if err == nil {
				enqueuedCount.Add(1)
			}
		}(i)
	}

	wg.Wait()

	consumerIDs := make([]string, numConsumers)
	for i := 0; i < numConsumers; i++ {
		consumerIDs[i] = "no-lost-consumer-" + uuid.New().String()[:8]
	}

	var consumerWG sync.WaitGroup
	for _, consumerID := range consumerIDs {
		consumerWG.Add(1)
		go func(consumer string) {
			defer consumerWG.Done()
			for {
				msgs, err := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
					ConsumerID:  consumer,
					BatchSize:   10,
					MaxWaitTime: 100 * time.Millisecond,
				})
				if err != nil || len(msgs) == 0 {
					break
				}

				for _, msg := range msgs {
					mu.Lock()
					if !processedIDs[msg.ID] {
						err := env.Queue.Acknowledge(env.Ctx, queue.UpdateParams{
							MessageID:  msg.ID,
							ConsumerID: consumer,
						})
						if err == nil {
							processedIDs[msg.ID] = true
							processedCount.Add(1)
						}
					}
					mu.Unlock()
				}
			}
		}(consumerID)
	}

	consumerWG.Wait()

	assert.Equal(t, int32(numMessages), enqueuedCount.Load(), "All messages should be enqueued")
	assert.Equal(t, int32(numMessages), processedCount.Load(), "All messages should be processed")

	allProcessed := true
	for {
		msgs, _ := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
			ConsumerID: "final-check-" + uuid.New().String()[:8],
			BatchSize:  1,
		})
		if len(msgs) == 0 {
			break
		}
		for _, msg := range msgs {
			mu.Lock()
			if !processedIDs[msg.ID] {
				allProcessed = false
			}
			mu.Unlock()
		}
	}
	assert.True(t, allProcessed, "No messages should be lost")
}

func TestGlobalFIFOOrdering(t *testing.T) {
	env, err := SetupEmulatorEnv()
	require.NoError(t, err, "Failed to set up emulator environment")
	defer env.Cleanup()

	consumerID := "fifo-consumer-" + uuid.New().String()[:8]

	numMessages := 50
	for i := 0; i < numMessages; i++ {
		_, err := env.Queue.Enqueue(env.Ctx, queue.EnqueueParams{
			Payload:  fmt.Sprintf("Message %d", i),
			Priority: queue.PriorityNormal,
		})
		require.NoError(t, err, "Failed to enqueue message")
	}

	var sequenceIDs []int64
	var mu sync.Mutex

	for i := 0; i < numMessages; i++ {
		msgs, err := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
			ConsumerID: consumerID,
			BatchSize:  1,
		})
		require.NoError(t, err, "Failed to dequeue message")
		require.Len(t, msgs, 1, "Should have dequeued 1 message")

		mu.Lock()
		sequenceIDs = append(sequenceIDs, msgs[0].SequenceID)
		mu.Unlock()

		err = env.Queue.Acknowledge(env.Ctx, queue.UpdateParams{
			MessageID:  msgs[0].ID,
			ConsumerID: consumerID,
		})
		require.NoError(t, err, "Failed to acknowledge message")
	}

	for i := 1; i < len(sequenceIDs); i++ {
		assert.GreaterOrEqual(t, sequenceIDs[i], sequenceIDs[i-1],
			"Sequence IDs should be in non-decreasing order (FIFO)")
	}
}

func TestPriorityPreemptsFIFO(t *testing.T) {
	env, err := SetupEmulatorEnv()
	require.NoError(t, err, "Failed to set up emulator environment")
	defer env.Cleanup()

	consumerID := "priority-fifo-consumer-" + uuid.New().String()[:8]

	lowPriorityIDs := make([]string, 10)
	for i := 0; i < 10; i++ {
		id, err := env.Queue.Enqueue(env.Ctx, queue.EnqueueParams{
			Payload:  fmt.Sprintf("Low priority %d", i),
			Priority: queue.PriorityLow,
		})
		require.NoError(t, err)
		lowPriorityIDs[i] = id
		time.Sleep(1 * time.Millisecond)
	}

	highPriorityIDs := make([]string, 10)
	for i := 0; i < 10; i++ {
		id, err := env.Queue.Enqueue(env.Ctx, queue.EnqueueParams{
			Payload:  fmt.Sprintf("High priority %d", i),
			Priority: queue.PriorityHigh,
		})
		require.NoError(t, err)
		highPriorityIDs[i] = id
		time.Sleep(1 * time.Millisecond)
	}

	var priorities []int64
	var mu sync.Mutex

	for i := 0; i < 20; i++ {
		msgs, err := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
			ConsumerID: consumerID,
			BatchSize:  1,
		})
		require.NoError(t, err, "Failed to dequeue")
		require.Len(t, msgs, 1, "Should get 1 message")

		mu.Lock()
		priorities = append(priorities, msgs[0].Priority)
		mu.Unlock()

		err = env.Queue.Acknowledge(env.Ctx, queue.UpdateParams{
			MessageID:  msgs[0].ID,
			ConsumerID: consumerID,
		})
		require.NoError(t, err)
	}

	highSeen := false
	for _, p := range priorities {
		if p == int64(queue.PriorityHigh) {
			highSeen = true
		} else if p == int64(queue.PriorityLow) && !highSeen {
			assert.Fail(t, "Low priority message should not appear before all high priority messages")
		}
	}

	var highCount, lowCount int
	for _, p := range priorities {
		if p == int64(queue.PriorityHigh) {
			highCount++
		} else if p == int64(queue.PriorityLow) {
			lowCount++
		}
	}
	assert.Equal(t, 10, highCount, "Should have 10 high priority messages")
	assert.Equal(t, 10, lowCount, "Should have 10 low priority messages")
}

func TestLockExpiration_ConcurrentRequeue(t *testing.T) {
	env, err := SetupEmulatorEnv()
	require.NoError(t, err, "Failed to set up emulator environment")
	defer env.Cleanup()

	consumer1 := "lock-expiry-consumer1-" + uuid.New().String()[:8]
	consumer2 := "lock-expiry-consumer2-" + uuid.New().String()[:8]

	msgID, err := env.Queue.Enqueue(env.Ctx, queue.EnqueueParams{
		Payload: "Lock expiration test",
	})
	require.NoError(t, err)

	msgs, err := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
		ConsumerID:   consumer1,
		BatchSize:    1,
		LockDuration: 500 * time.Millisecond,
	})
	require.NoError(t, err)
	require.Len(t, msgs, 1)

	time.Sleep(600 * time.Millisecond)

	err = env.Queue.RequeueExpiredLocksWithTimeout(env.Ctx, 500*time.Millisecond)
	require.NoError(t, err)

	consumer1Processed := false
	consumer2Processed := false
	var wg sync.WaitGroup
	var mu sync.Mutex

	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			msgs, err := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
				ConsumerID: consumer1,
				BatchSize:  1,
			})
			if err == nil && len(msgs) > 0 && msgs[0].ID == msgID {
				mu.Lock()
				consumer1Processed = true
				mu.Unlock()
				_ = env.Queue.Acknowledge(env.Ctx, queue.UpdateParams{
					MessageID:  msgID,
					ConsumerID: consumer1,
				})
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			msgs, err := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
				ConsumerID: consumer2,
				BatchSize:  1,
			})
			if err == nil && len(msgs) > 0 && msgs[0].ID == msgID {
				mu.Lock()
				consumer2Processed = true
				mu.Unlock()
				_ = env.Queue.Acknowledge(env.Ctx, queue.UpdateParams{
					MessageID:  msgID,
					ConsumerID: consumer2,
				})
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	wg.Wait()

	processedCount := 0
	if consumer1Processed {
		processedCount++
	}
	if consumer2Processed {
		processedCount++
	}

	assert.Equal(t, 1, processedCount, "Message should be processed by exactly one consumer")
}

func TestRequeueAfterAcknowledgeRace(t *testing.T) {
	env, err := SetupEmulatorEnv()
	require.NoError(t, err, "Failed to set up emulator environment")
	defer env.Cleanup()

	consumer := "requeue-race-consumer-" + uuid.New().String()[:8]

	for iteration := 0; iteration < 20; iteration++ {
		msgID, err := env.Queue.Enqueue(env.Ctx, queue.EnqueueParams{
			Payload: fmt.Sprintf("Race test %d", iteration),
		})
		require.NoError(t, err)

		msgs, err := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
			ConsumerID: consumer,
			BatchSize:  1,
		})
		require.NoError(t, err)
		require.Len(t, msgs, 1)

		var wg sync.WaitGroup
		var ackErr, requeueErr error

		wg.Add(2)
		go func() {
			defer wg.Done()
			ackErr = env.Queue.Acknowledge(env.Ctx, queue.UpdateParams{
				MessageID:  msgID,
				ConsumerID: consumer,
			})
		}()

		go func() {
			defer wg.Done()
			requeueErr = env.Queue.Requeue(env.Ctx, queue.UpdateParams{
				MessageID:  msgID,
				ConsumerID: consumer,
			})
		}()

		wg.Wait()

		assert.True(t, (ackErr == nil) != (requeueErr == nil),
			"Exactly one of ack or requeue should succeed, got ackErr=%v, requeueErr=%v", ackErr, requeueErr)
	}
}

func TestConcurrentDeduplication(t *testing.T) {
	env, err := SetupEmulatorEnv()
	require.NoError(t, err, "Failed to set up emulator environment")
	defer env.Cleanup()

	dedupKey := "concurrent-dedup-" + uuid.New().String()[:8]
	var wg sync.WaitGroup
	var firstID string
	var mu sync.Mutex
	idCount := 0

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := env.Queue.Enqueue(env.Ctx, queue.EnqueueParams{
				Payload:          "Deduplicated message",
				DeduplicationKey: dedupKey,
			})
			if err == nil {
				mu.Lock()
				if firstID == "" {
					firstID = id
				}
				idCount++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	assert.Equal(t, 1, idCount, "Only one message should be created with same dedup key")

	consumerID := "dedup-verify-" + uuid.New().String()[:8]
	msgs, err := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
		ConsumerID: consumerID,
		BatchSize:  1,
	})
	require.NoError(t, err)
	require.Len(t, msgs, 1)

	assert.Equal(t, firstID, msgs[0].ID, "Should get the first inserted message")
}

func TestHighConcurrency_100Workers(t *testing.T) {
	env, err := SetupEmulatorEnv()
	require.NoError(t, err, "Failed to set up emulator environment")
	defer env.Cleanup()

	numWorkers := 100
	numMessages := 200

	for i := 0; i < numMessages; i++ {
		_, err := env.Queue.Enqueue(env.Ctx, queue.EnqueueParams{
			Payload: fmt.Sprintf("High concurrency message %d", i),
		})
		require.NoError(t, err)
	}

	var wg sync.WaitGroup
	var processed atomic.Int32
	var mu sync.Mutex
	processedIDs := make(map[string]bool)

	workerCtx, cancel := context.WithCancel(env.Ctx)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			consumerID := fmt.Sprintf("worker-%d-%s", workerID, uuid.New().String()[:8])

			for {
				select {
				case <-workerCtx.Done():
					return
				default:
				}

				msgs, err := env.Queue.Dequeue(env.Ctx, queue.DequeueParams{
					ConsumerID:  consumerID,
					BatchSize:   5,
					MaxWaitTime: 50 * time.Millisecond,
				})
				if err != nil || len(msgs) == 0 {
					if ctxErr := workerCtx.Err(); ctxErr != nil {
						return
					}
					time.Sleep(10 * time.Millisecond)
					continue
				}

				for _, msg := range msgs {
					mu.Lock()
					if !processedIDs[msg.ID] {
						err := env.Queue.Acknowledge(env.Ctx, queue.UpdateParams{
							MessageID:  msg.ID,
							ConsumerID: consumerID,
						})
						if err == nil {
							processedIDs[msg.ID] = true
							processed.Add(1)
						}
					}
					mu.Unlock()
				}
			}
		}(i)
	}

	time.Sleep(5 * time.Second)
	cancel()
	wg.Wait()

	assert.Equal(t, int32(numMessages), processed.Load(), "All messages should be processed exactly once")
}
