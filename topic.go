package whisper

// how to use, new a Executor with MQ driver. and append new message to executor queue.
import (
	"errors"
	"github.com/silverswords/whisper/pubsub/loopback"
	"io"
	"io/ioutil"
	"log"
)

var (
	RetryError   = errors.New("please retry")
	DriverError  = errors.New("interface{] isn't driver")
	HandlerError = errors.New("interface{} isn't handler")
)

var Loopback = NewExecutor(loopback.Loopback)

type Sender struct {
	*Executor
}

func NewSender(driver Driver) *Sender {
	s := &Sender{
		Executor: NewExecutor(driver),
	}
	go func() {
		for {
			err := s.Executor.sendroutine()
			if err != nil {
				if err == RetryError {
					// do retry thing or just drop
					continue
				}
			} else {
				//	other error handle.

				//pretend to stop this sender here
				log.Println("because of the ", err, " stop the sender")
				// notify the Sender gracefully stop
				s.GracefulStop()
				return
			}
		}
	}()
	return s
}

func (s *Sender) GracefulStop() {

}

func LoopPublish(m *Message) { Loopback.Append(m) }

// Executor just send messages from Executor queue to driver or receive from driver to queue.
// Only one thing can do.
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
func (e *Executor) sendroutine() error {
	action, ok := e.Queue.Pop().(Handler)
	if !ok {
		return HandlerError
	}

	return action.Do(e.Driver)
}

type Handler interface {
	Do(driver interface{}) error
}

// Message has imply Handler interface.
type Message struct {
	MsgID  uint64
	Header Header
	Body   []byte
	//ACK    uint64
}

func NewMessage(topic string, body io.Reader) (*Message, error) {
	rc, ok := body.(io.ReadCloser)
	if !ok && body != nil {
		rc = ioutil.NopCloser(body)
	}

	message := &Message{
		Header: make(Header),
		Body:   make([]byte, 256),
	}

	rc.Read(message.Body)
	return message, nil
}

func (m *Message) Do(driver interface{}) error {
	d, ok := driver.(Driver)
	if !ok {
		panic("MQ: unknown driver (forgotten import?) ")
	}
	if err := d.Pub(m.Header.GetTopic(), m); err != nil {
		return err
	}
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
