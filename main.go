package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/datastore"
	"golang.org/x/net/context"
	"golang.org/x/time/rate"
)

const (
	kindSingleCounter = "single_counter"
	keySingleCounter  = "single_counter_key"
)

var (
	singleCounterKey = datastore.NameKey(kindSingleCounter, keySingleCounter, nil)
)

const (
	ratePerSec = 100000
	execCount  = 1000
)

type singleCounter struct {
	Count int
}

func main() {
	client, err := datastore.NewClient(context.Background(), os.Getenv("GOOGLE_CLOUD_PROJECT"))
	if err != nil {
		panic(err)
	}
	defer client.Close()

	if _, err := client.Put(context.Background(), singleCounterKey, &singleCounter{}); err != nil {
		panic(err)
	}

	countBySingleCounter(client)
}

func sayHello() {
	newRateLimitExecuter(ratePerSec, execCount, func() {
		fmt.Println("OK!!")
	}).execute(context.Background())
}

func countBySingleCounter(client *datastore.Client) {
	var (
		finish = make(chan struct{}, execCount)
		ctx    = context.Background()
	)

	go func() {
		fmt.Printf("0/%d", execCount)
		var finishCount int
		for range finish {
			finishCount++
			fmt.Printf("\r%d/%d", finishCount, execCount)
		}
		fmt.Println()
	}()
	newRateLimitExecuterWithConcurrent(ratePerSec, execCount, func() {
		defer func() { finish <- struct{}{} }()

		var counter singleCounter
		if err := client.Get(ctx, singleCounterKey, &counter); err != nil {
			log.Printf("failed to get conter: %v", err)
			return
		}
		counter.Count++
		if _, err := client.Put(ctx, singleCounterKey, &counter); err != nil {
			log.Printf("failed to put conter: %v", err)
			return
		}
	}).execute(ctx)
	close(finish)

	var counter singleCounter
	if err := client.Get(ctx, singleCounterKey, &counter); err != nil {
		log.Printf("failed to get conter: %v", err)
		return
	}
	fmt.Printf("%v", counter)
}

func countBySingleCounterWithTx(client *datastore.Client) {
	var (
		finish = make(chan struct{}, execCount)
		ctx    = context.Background()
	)

	go func() {
		fmt.Printf("0/%d", execCount)
		var finishCount int
		for range finish {
			finishCount++
			fmt.Printf("\r%d/%d", finishCount, execCount)
		}
		fmt.Println()
	}()
	newRateLimitExecuter(ratePerSec, execCount, func() {
		defer func() { finish <- struct{}{} }()

		tx, err := client.NewTransaction(ctx)
		if err != nil {
			log.Printf("failed to start transaction: %v", err)
			return
		}

		var counter singleCounter
		if err := tx.Get(singleCounterKey, &counter); err != nil {
			log.Printf("failed to get conter: %v", err)
			return
		}
		counter.Count++
		if _, err := tx.Put(singleCounterKey, &counter); err != nil {
			log.Printf("failed to put conter: %v", err)
			return
		}
		if _, err := tx.Commit(); err != nil {
			log.Printf("failed to commit: %v", err)
			return
		}
	}).execute(ctx)
	close(finish)

	var counter singleCounter
	if err := client.Get(ctx, singleCounterKey, &counter); err != nil {
		log.Printf("failed to get conter: %v", err)
		return
	}
	fmt.Printf("%v", counter)
}

type rateLimitExecuter struct {
	ratePerSec int
	execCount  int
	f          func()
}

func newRateLimitExecuter(ratePerSec, execCount int, f func()) *rateLimitExecuter {
	return &rateLimitExecuter{
		ratePerSec: ratePerSec,
		execCount:  execCount,
		f:          f,
	}
}

func (r *rateLimitExecuter) execute(ctx context.Context) {
	var (
		limit   = rate.Every(time.Second / time.Duration(r.ratePerSec))
		limiter = rate.NewLimiter(limit, r.ratePerSec)
	)

	for i := 0; i < r.execCount; i++ {
		if err := limiter.Wait(ctx); err != nil {
			panic(err)
		}
		r.f()
	}
}

type rateLimitExecuterWithConcurrent struct {
	ratePerSec int
	execCount  int
	f          func()
}

func newRateLimitExecuterWithConcurrent(ratePerSec, execCount int, f func()) *rateLimitExecuterWithConcurrent {
	return &rateLimitExecuterWithConcurrent{
		ratePerSec: ratePerSec,
		execCount:  execCount,
		f:          f,
	}
}

func (r *rateLimitExecuterWithConcurrent) execute(ctx context.Context) {
	var (
		limit   = rate.Every(time.Second / time.Duration(r.ratePerSec))
		limiter = rate.NewLimiter(limit, r.ratePerSec)
		wg      = new(sync.WaitGroup)
	)

	for i := 0; i < r.execCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := limiter.Wait(ctx); err != nil {
				panic(err)
			}
			r.f()
		}()
	}
	wg.Wait()
}
