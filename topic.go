package whisper
// how to use, new a Executor with MQ driver. and append new message to executor queue.
import "errors"

var (
	RetryError = errors.New("please retry")
	DriverError = errors.New("interface{] isn't driver")
	HandlerError = errors.New("interface{} isn't handler")
)

type Handler interface {
	Do(driver interface{}) error
}

type Executor struct {
	*Queue
	Driver interface{} // use for every handler to do the thing.
}

func NewExecutor(driver interface{}) *Executor {
	return &Executor{
		Queue:  NewQueue(),
		Driver: driver,
	}
}

// by binding channel to send message. binding channel is for  queue.
// use action to send message handler enhanced logic.
// block fifo queue  so:
// go func(){
//		for e.execution() == nil {}
//		}
func (e *Executor) execution() error {
	action, ok := e.Queue.Pop().(Handler)
	if !ok {
		return HandlerError
	}

	return action.Do(e.Driver)
}

// Message has imply Handler interface.
type Message struct {
	Header Header
	Body   []byte
	ACK    uint64
}

func (m *Message) Do(driver interface{}) error {
	d, ok := driver.(Driver)
	if !ok {
		return DriverError
	}
	if err := d.Pub(m.Header.Get("topic"), m); err != nil {
		return err
	}
	return nil
}

type Driver interface {
	Pub(topic string, msg *Message) error
	Sub(topic string, handler func(*Message) ) error
}

type simpleDriver struct {
}

func (d *simpleDriver) Pub(topic string, msg *Message) error {
	return nil
}

func (d *simpleDriver) Sub(topic string, handler func(*Message) ) error {
	return nil
}


// example for retry, ratelimit, ACK and store
func Retry(handler Handler) Handler {
	return &RetryHandler{retrytime: 3, origin: handler}
}

type RetryHandler struct {
	retrytime int
	origin    Handler
}

func (h *RetryHandler) Do(driver interface{}) error {
	d, ok := driver.(Driver)
	if !ok {
		return DriverError
	}
	for {
		if err := h.origin.Do(d); err != nil {
			if err == RetryError {
				h.retrytime -= 1
				if h.retrytime > 0 {
					continue
				}
			} else {
				return err
			}
		}
	}
	return nil
}
