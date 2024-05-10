package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"sync"
	"testing"

	"github.com/customerio/homework/stream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Mock for the stream processor
type MockStreamProcessor struct {
	mock.Mock
}

func (m *MockStreamProcessor) Process(ctx context.Context, data io.Reader) (<-chan *stream.Record, error) {
	args := m.Called(ctx, data)
	return args.Get(0).(<-chan *stream.Record), args.Error(1)
}

func MockChannel() <-chan *stream.Record {
	ch := make(chan *stream.Record, 1)
	ch <- &stream.Record{ID: "1", UserID: "123", Type: "event"}
	close(ch)
	return ch
}

// Test file operations
func TestFileOperations(t *testing.T) {
	// Test opening a file successfully
	_, err := os.Open("data/messages.1.data")
	assert.NoError(t, err, "Should open file without error")

	// Test file open failure scenario
	_, err = os.Open("data/messagess.1.data")
	assert.Error(t, err, "Should return an error for non-existent files")
}

// Test stream processing
func TestStreamProcessing(t *testing.T) {
	mockStream := &MockStreamProcessor{}
	mockStream.On("Process", mock.Anything, mock.Anything).Return(MockChannel(), nil)

	ctx := context.Background()
	data := bytes.NewReader([]byte("user data"))
	records, err := mockStream.Process(ctx, data)
	assert.NoError(t, err, "Stream processing should not error out")

	for record := range records {
		assert.NotNil(t, record, "Record should not be nil")
	}
}

// Test concurrency and goroutines
func TestConcurrency(t *testing.T) {
	users := make(map[string]*UserData)
	records := make(chan *stream.Record, 2)

	records <- &stream.Record{ID: "1", UserID: "user1", Type: "login"}
	records <- &stream.Record{ID: "2", UserID: "user2", Type: "logout"}
	close(records)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for rec := range records {
			processRecord(users, rec)
		}
	}()
	wg.Wait()

	assert.Equal(t, 2, len(users), "There should be two users processed")
	assert.Contains(t, users, "user1", "User1 should be present in the map")
	assert.Contains(t, users, "user2", "User2 should be present in the map")
}

// TestConcurrencyWithShards tests the concurrency logic with sharded map access.
/* func TestConcurrencyWithShards(t *testing.T) {
	var shards [numShards]map[string]*UserData
	var mutexes [numShards]sync.Mutex

	// Initialize each shard
	for i := 0; i < numShards; i++ {
		shards[i] = make(map[string]*UserData)
	}

	// Simulate concurrent access
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ { // 100 goroutines
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			shardIndex := id % numShards // Determine the shard index
			mutexes[shardIndex].Lock()
			// Simulate processing
			shards[shardIndex]["key"] = &UserData{ID: "user123", Type: "update"}
			mutexes[shardIndex].Unlock()
		}(i)
	}
	wg.Wait()

	// Test to ensure data integrity and no race conditions
	// Additional checks can be added here
} */
