package main

import (
	"fmt"
	"sync"
)

func main() {
	var x = []int{1, 2, 3, 4, 5, 6}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for i, v := range x {
			if v == 4 {
				x = append(x[0:i-1], x[i:]...)
				break
			}
		}
		wg.Done()
	}()
	wg.Wait()
	fmt.Println(x)
}
