package whisper

import "errors"

var (
	RetryError = errors.New("please retry")
)

type Handler interface {
	Do(driver interface{}) error
}

type Executor struct {
	*Queue
	Driver
}

func NewExecutor() *Executor {
	return &Executor{
		Queue:  NewQueue(),
		Driver: &simpleDriver{},
	}
}

// by binding channel to send message. binding channel is for  queue.
// use action to send message handler enhanced logic.
func (e *Executor) execution() error {
	action, ok := e.Queue.Pop().(Handler)
	if !ok {
		return errors.New("Not Handler!")
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
		return errors.New("error driver ing")
	}
	if err := d.Pub(m.Header.Get("topic"), m); err != nil {
		return err
	}
	return nil
}

type Driver interface {
	Pub(topic string, msg *Message) error
	Sub(topic string, Handler func()) error
}

type simpleDriver struct {
}

func (d *simpleDriver) Pub(topic string, msg *Message) error {
	return nil
}

func (d *simpleDriver) Sub(topic string, Handler func()) error {
	return nil
}

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
		return errors.New("error driver ing")
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
