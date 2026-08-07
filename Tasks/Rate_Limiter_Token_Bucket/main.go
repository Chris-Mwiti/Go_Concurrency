package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"
)

type TokenBucket struct {
	maxCapacity int
	rem int
	mu sync.Mutex
	cond *sync.Cond
}

type Node struct {
	tokenBucket *TokenBucket
	client *http.Client
	logger *slog.Logger
}

func NewTokenBucket(maxcapacity int) TokenBucket{
	tb := TokenBucket{
		maxCapacity: maxcapacity,
		rem: maxcapacity,
	}
	tb.cond = sync.NewCond(&tb.mu)
	
	return tb
}

func (n *Node) acquireToken(ctx context.Context) (error) {

	n.tokenBucket.cond.L.Lock()
	defer n.tokenBucket.cond.L.Unlock()

	//create an after ctx func that will trigger a wake up to all other goroutines to check for ctx.Err
	stopCtx := context.AfterFunc(ctx, func() {
		n.tokenBucket.cond.L.Lock()
		n.tokenBucket.cond.Broadcast()
		n.tokenBucket.cond.L.Unlock()
	})
	defer stopCtx()

	for n.tokenBucket.rem <= 0 {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("context already done and completed")
		}
		n.tokenBucket.cond.Wait()
	}

	//here we are going to add another check to check if the the context is already complete before deducting the token
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context already done. canceled or timedout")
	}

	//deduct the token buck rem tokens
	n.tokenBucket.rem--

	return nil
}

func (n *Node) DoReq(ctx context.Context, url string) (error) {
	if err := n.acquireToken(ctx); err != nil {
		return fmt.Errorf("req cannot acquire token")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {

		slog.ErrorContext(ctx, "error while formatting request", "err", err)
		return fmt.Errorf("error while making request")
	}

	resp, err := n.client.Do(req)
	if err != nil{
		slog.ErrorContext(ctx, "error while submitting request", "err", err)
		return fmt.Errorf("error while submitting request")
	}
	defer resp.Body.Close()

	return nil
}

func (n *Node) RefilBuck(ctx context.Context, interval time.Duration, tokenRefil int)(error) {
	//create an method ticker for the refil bucket
	//this is crucial since a shared timer can affect how the signal is submitted 
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context already done")

		case <-ticker.C:
			n.tokenBucket.cond.L.Lock()
			if n.tokenBucket.rem < n.tokenBucket.maxCapacity {
				n.tokenBucket.rem = min(n.tokenBucket.maxCapacity, (n.tokenBucket.rem + tokenRefil))
				//unblock other goroutines awaiting the response
				n.tokenBucket.cond.Broadcast()
			}
			n.tokenBucket.cond.L.Unlock()
		}	
	}
}

func NewNode(maxCapacity int) Node {
	buck := NewTokenBucket(maxCapacity)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{}))
	return Node{
		tokenBucket: &buck,
		client: &http.Client{
			Timeout: 10 * time.Second,	
		},
		logger: logger,
	}
}

func main() {
	
	node := NewNode(3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go node.RefilBuck(ctx, 500 * time.Millisecond, 1)

	testURLs := []string{
		"https://httpbin.org/get",
		"https://jsonplaceholder.typicode.com/posts/1",
		"https://httpbin.org/delay/2",
		"https://httpbin.org/get",
		"https://httpbin.org/status/429",
	}

	var wg sync.WaitGroup

	// Fire 10 concurrent requests to exhaust the bucket immediately
	for i := 0; i < 10; i++ {
		wg.Add(1)
		id := i + 1
		url := testURLs[i%len(testURLs)]

		go func(reqID int, reqURL string) {
			defer wg.Done()

			// Create a 3-second context per request
			reqCtx, reqCancel := context.WithTimeout(ctx, 3*time.Second)
			defer reqCancel()

			start := time.Now()
			fmt.Printf("[%s] Req #%d requesting token for %s...\n", start.Format("15:04:05.000"), reqID, reqURL)

			err := node.DoReq(reqCtx, reqURL)
			elapsed := time.Since(start)

			if err != nil {
				fmt.Printf("[%s] Req #%d FAILED after %v: %v\n", time.Now().Format("15:04:05.000"), reqID, elapsed, err)
			} else {
				fmt.Printf("[%s] Req #%d SUCCESS after %v\n", time.Now().Format("15:04:05.000"), reqID, elapsed)
			}
		}(id, url)
	}

	wg.Wait()
}
