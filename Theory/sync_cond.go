package main

import (
	"fmt"
	"sync"
	"time"
)

var done bool

func read(c *sync.Cond, name string){
	fmt.Println("reading process has began")
	//here we are locking since we have a critical section which is done var
	c.L.Lock()
	for !done {
		c.Wait()
	}
	time.Sleep(2 * time.Second)
	fmt.Printf("%s has finished reading\n", name)
	c.L.Unlock()
}

func write(c *sync.Cond, name string){
	fmt.Printf("writing process has began: %s\n", name)

	c.L.Lock()
	time.Sleep(3 * time.Second)
	done = true
	c.L.Unlock()

	fmt.Println("broadcasting the event to sleeping goroutines")
	c.Broadcast()
}

func main() {
	var m sync.Mutex

	cond := sync.NewCond(&m)

	go read(cond, "Reader One")
	go read(cond, "Reader Two")
	go read(cond, "Reader Three")

	write(cond, "Writer One")

	time.Sleep(7 * time.Second)
}
