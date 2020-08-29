package timingwheel

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestTimingWheel(t *testing.T) {
	var deviation int64
	var num int64 = 100
	sleeptime := time.Millisecond * 100

	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			start := time.Now().UnixNano()
			<-After(sleeptime)
			end := time.Now().UnixNano()
			duration := end - start - int64(sleeptime)
			if duration < 0 {
				deviation -= duration
			} else {
				deviation += duration
			}
			wg.Done()
		}(i)
		time.Sleep(time.Millisecond * 100)
	}
	wg.Wait()
	fmt.Println(float64(deviation) / float64(num*int64(sleeptime)))
	fmt.Println(time.Duration(deviation) / 100)
}

//BenchmarkTimeWheel benchmarks timewheel.
func BenchmarkTimeWheel(b *testing.B) {
	for i := 0; i < b.N; i++ {
		After(time.Millisecond * time.Duration(i%100000+1))
	}
}

//BenchmarkTimeBase benchmark origin timer.
func BenchmarkTimeBase(b *testing.B) {
	for i := 0; i < b.N; i++ {
		time.After(time.Millisecond * 100)
	}
}
