package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"time"
)

func main() {
	go func() {
		for {
			_ = make([]byte, 1e7) // 10m
			time.Sleep(time.Second)
		}
	}()
	http.ListenAndServe("0.0.0.0:10000", nil)
}

func main1() {
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
