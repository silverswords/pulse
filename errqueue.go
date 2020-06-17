package whisper

import "log"

type ErrorQueue struct {
	queue *Queue
	closed bool
}

func NewErrorQueue() *ErrorQueue {
	return &ErrorQueue{
		queue: NewQueue(),
	}
}

func (e *ErrorQueue) Close() {
	e.closed = true
}

func (e *ErrorQueue) execution()  {
	go func() {
		for !e.closed {
			err,ok := e.queue.Pop().(Handler)
			if !ok {
				log.Println("an error is not Handler")
				continue
			}
			err.Do(e)
		}
	}()
}

func (e *ErrorQueue) Add(h Handler) {
	e.queue.Append(h)
}