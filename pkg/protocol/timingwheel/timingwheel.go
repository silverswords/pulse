package timingwheel

import (
	"sync"
	"time"
)

var (
	timerMap map[time.Duration]*TimingWheel
	mapLock  = &sync.Mutex{}
	accuracy = 20
)

func init() {
	timerMap = make(map[time.Duration]*TimingWheel)
}

// After return a channel like time.After
func After(t time.Duration) <-chan struct{} {
	mapLock.Lock()
	defer mapLock.Unlock()
	if v, ok := timerMap[t]; ok {
		return v.After(t)
	}
	timerMap[t] = NewTimingWheel(t/time.Duration(accuracy), accuracy+1)
	return timerMap[t].After(t)

}

func SetAccuracy(a int) { accuracy = a }

type TimingWheel struct {
	lock sync.Mutex
	t    time.Duration
	maxT time.Duration

	ticker      *time.Ticker
	timingWheel []chan struct{}
	currPos     int
}

// NewTimingWheel -
func NewTimingWheel(t time.Duration, size int) *TimingWheel {
	tw := &TimingWheel{
		t:           t,
		maxT:        t * time.Duration(size),
		timingWheel: make([]chan struct{}, size),
		ticker:      time.NewTicker(t),
	}

	// init channel
	for i := range tw.timingWheel {
		tw.timingWheel[i] = make(chan struct{})
	}

	go tw.run()
	return tw
}

// Stop stop timingWheel's time.ticker
func (tw *TimingWheel) Stop() {
	tw.ticker.Stop()
}

// After like time.After
func (tw *TimingWheel) After(timeout time.Duration) (c <-chan struct{}) {
	if timeout > tw.maxT {
		panic("cannot handle this timeout")
	}

	pos := int(timeout / tw.t)
	if pos > 0 {
		pos--
	}

	tw.lock.Lock()
	c = tw.timingWheel[(tw.currPos+pos)%len(tw.timingWheel)]
	tw.lock.Unlock()
	return
}

func (tw *TimingWheel) run() {
	for range tw.ticker.C {
		tw.lock.Lock()
		oldestC := tw.timingWheel[tw.currPos]
		tw.timingWheel[tw.currPos] = make(chan struct{})
		tw.currPos = (tw.currPos + 1) % len(tw.timingWheel)
		tw.lock.Unlock()
		close(oldestC)
	}
}
