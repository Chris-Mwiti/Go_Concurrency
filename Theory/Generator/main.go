package main

import "fmt"

func main() {
	ch := generator()

	for i := 0; i < 5; i++{
		num := <-ch

		fmt.Printf("received: %d\n", num)
	}

	fmt.Println("program already finished")
}

func generator() <-chan int {
	ch := make(chan int)

	go func() {
		for i := 0; ; i++ {
			ch <- i
			fmt.Println("sent number")
		}
	}()

	return ch
}
