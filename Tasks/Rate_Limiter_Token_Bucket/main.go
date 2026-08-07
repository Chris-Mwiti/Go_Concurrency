package main

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type TokenBucket struct {
	maxCapacity int
	rem int
	refiling bool
	ticker time.Ticker
}

type Node struct {
	tokenBucket TokenBucket
}

func (n *Node) DoReq(ctx context.Context, url string, cond *sync.Cond) (error) {
	select {
	case <-ctx.Done():
		return fmt.Errorf("context timeout")
	default:
		cond.L.Lock()
		for n.tokenBucket.rem <= 0 || n.tokenBucket.refiling{
			cond.Wait()
		}
		n.tokenBucket.rem--
		cond.L.Unlock()

		time.Sleep(3 * time.Second)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)

		if err != nil {
			return err
		}

		_, err = http.DefaultClient.Do(req)
		if err != nil {
			return err
		} 

		return nil

	}

}

func (n *Node) RefilBuck(ctx context.Context, cond *sync.Cond)(error) {

	select {
	case <-ctx.Done():
		return fmt.Errorf("context done")

	case <-n.tokenBucket.ticker.C:
		cond.L.Lock()
		defer cond.L.Unlock()
		if n.tokenBucket.rem < n.tokenBucket.maxCapacity {
			n.tokenBucket.refiling = true
			n.tokenBucket.rem += 2
			cond.Broadcast()
		} else {
			return fmt.Errorf("token bucket already full")
		}
	}

	return nil
}

func main() {
	requests := []string {
		"https://google.com",
		"https://",
	}
}
