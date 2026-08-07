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

	n.tokenBucket.mu.Lock()
	defer n.tokenBucket.mu.Unlock()

	//create an after ctx func that will trigger a wake up to all other goroutines to check for ctx.Err
	stopCtx := context.AfterFunc(ctx, func() {
		n.tokenBucket.mu.Lock()
		n.tokenBucket.cond.Broadcast()
		n.tokenBucket.mu.Unlock()
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
			n.tokenBucket.mu.Lock()
			if n.tokenBucket.rem < n.tokenBucket.maxCapacity {
				n.tokenBucket.rem = min(n.tokenBucket.maxCapacity, (n.tokenBucket.rem + tokenRefil))
				//unblock other goroutines awaiting the response
				n.tokenBucket.cond.Broadcast()
			}
			n.tokenBucket.mu.Unlock()
		}	
	}
}

func NewNode(bucket *TokenBucket, maxCapacity int, client *http.Client) Node {
	buck := NewTokenBucket(maxCapacity)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{}))
	return Node{
		tokenBucket: &buck,
		client: http.DefaultClient,
		logger: logger,
	}
}

func main() {
	requests := []string {
		"https://google.com",
		"https://",
	}
}
