package main

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"time"
)

func TestClient(id int, duration time.Duration, wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := net.Dial("tcp", "127.0.0.1:3224")
	if err != nil {
		fmt.Printf("[Client %d] Connection error: %v\n", id, err)
		return
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)
	deadline := time.Now().Add(duration)

	msgCount := 0
	for time.Now().Before(deadline) {
		msgCount++
		payload := fmt.Sprintf("Client-%d Message #%d timestamp=%d", id, msgCount, time.Now().UnixNano())

		start := time.Now()
		_, err := conn.Write([]byte(payload))
		if err != nil {
			fmt.Printf("[Client %d] Write error: %v\n", id, err)
			return
		}

		// Read ACK back from group commit server
		ack, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("[Client %d] Read error: %v\n", id, err)
			return
		}

		fmt.Printf("[Client %d] Received %s in %v\n", id, ack[:len(ack)-1], time.Since(start))
		time.Sleep(100 * time.Millisecond)
	}
}

func main() {
	var wg sync.WaitGroup
	numClients := 5
	testDuration := 6 * time.Second

	fmt.Printf("Starting %d concurrent TCP client workers...\n", numClients)
	for i := 1; i <= numClients; i++ {
		wg.Add(1)
		go TestClient(i, testDuration, &wg)
	}

	wg.Wait()
	fmt.Println("Test run complete. Check sample.txt for batched outputs.")
}

