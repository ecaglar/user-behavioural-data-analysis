package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/customerio/homework/stream"
)

const (
	EVENT      = "event"
	ATTRIBUTES = "attributes"
)

const numShards = 32 // Number of shards

type UserDataShard struct {
	m    map[string]*UserData
	lock sync.Mutex
}

func main() {
	start := time.Now()

	inputFilePath := flag.String("infile", "data/messages.1.data", "Path to the input data file")
	outputFilePath := flag.String("outfile", "summary_output.csv", "Path to the output CSV file")
	flag.Parse()

	file, err := os.Open(*inputFilePath)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		cancel()
	}()

	ch, err := stream.Process(ctx, file)
	if err != nil {
		log.Fatal(err)
	}

	shards := make([]*UserDataShard, numShards)
	for i := 0; i < numShards; i++ {
		shards[i] = &UserDataShard{m: make(map[string]*UserData)}
	}

	recordChannel := make(chan *stream.Record, 100)
	var wg sync.WaitGroup

	numWorkers := runtime.NumCPU()
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for rec := range recordChannel {
				if rec == nil {
					continue
				}
				shard := shards[hash(rec.UserID)%numShards]
				shard.lock.Lock()
				processRecord(shard.m, rec)
				shard.lock.Unlock()
			}
		}(i)
	}

	go func() {
		for rec := range ch {
			recordChannel <- rec
		}
		close(recordChannel)
	}()

	wg.Wait()

	if err := ctx.Err(); err != nil {
		log.Fatal(err)
	}

	if err := output(shards, *outputFilePath); err != nil {
		log.Fatalf("Failed to write output: %v", err)
	}

	elapsed := time.Since(start)
	fmt.Printf("Execution time: %s\n", elapsed)

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Allocated memory: %.2f KB (%.2f MB)\n", float64(m.Alloc)/1024, float64(m.Alloc)/1024/1024)
	fmt.Printf("Total allocated memory: %.2f KB (%.2f MB)\n", float64(m.TotalAlloc)/1024, float64(m.TotalAlloc)/1024/1024)
	fmt.Printf("System memory: %.2f KB (%.2f MB)\n", float64(m.Sys)/1024, float64(m.Sys)/1024/1024)

}

func hash(s string) int {
	h := 0
	for i := 0; i < len(s); i++ {
		h = 31*h + int(s[i])
	}
	return h
}
