package client

// how to use, new a Executor with MQ driver. and append new message to executor queue.
import (
	"errors"
	"github.com/silverswords/whisper"
	"github.com/silverswords/whisper/driver/loopback"
	"github.com/silverswords/whisper/message"
	"io"
	"io/ioutil"
	"log"
)

var (
	RetryError           = errors.New("please retry")
	RetryTimeRunOutError = errors.New("message has already been retry")
	DriverError          = errors.New("interface{] isn't driver")
	HandlerError         = errors.New("interface{} isn't handler")
)

var Loopback = NewSender(loopback.LoopbackDriver)

func LoopPublish(m *Message) { Loopback.Send(m) }

// Sender only to send every message with the driver.
type QueueSender struct {
	q      chan *Message
	driver interface{} // use for every handler to do the thing.

	closed chan struct{}
}

func NewSender(driver whisper.Driver) *QueueSender {
	s := &QueueSender{
		q:      make(chan *Message, 100),
		driver: driver,
		closed: make(chan struct{}, 1),
	}
	go s.execution()
	return s
}

//Should Retry from grpc-go/stream.go
//https://sourcegraph.com/github.com/grpc/grpc-go@9a465503579e4f97b81d4e2ddafdd1daef80aa93/-/blob/stream.go#L466:25

// send message in Sender until colsed.
func (s *QueueSender) execution() {
	for {
		select {
		case msg := <-s.q:
			{
				log.Println("sender send: ", msg)
				//_ = msg.Do(s.driver)

				if err := msg.Do(s.driver); err != nil {
					if err := msg.ShouldRetry(err); err != nil {
						//	handle error
						continue
					}
					s.Send(msg)
				}
			}
		case _ = <-s.closed:
			return
		}
	}
}

func Decorator(f func(interface{})error) (func(interface{}) error) {
	retryTimes := 3
	wrapper := func (driver interface{}) error{
		if err := f(driver); err != nil {
			if err == RetryError {
				retryTimes --
				if retryTimes < 0 {
					return RetryTimeRunOutError
				}
				
			}
			return err
		}
		return nil
	}

	return wrapper
}

// Send Would Block when channel full of Message
func (s *QueueSender) Send(msg *Message) {
	s.q <- msg
}

func (s *QueueSender) Close() {
	s.closed <- struct{}{}
}

type Handler interface {
	Do(driver interface{}) error
}

// Message has imply Handler interface.
type Message struct {
	MsgID  uint64
	Header message.Header
	Body   []byte

	retrytime int
}

func NewMessage(topic string, body io.Reader) (*Message, error) {
	rc, ok := body.(io.ReadCloser)
	if !ok && body != nil {
		rc = ioutil.NopCloser(body)
	}

	message := &Message{
		Header: make(message.Header),
		Body:   make([]byte, 256),
	}

	rc.Read(message.Body)
	return message, nil
}

func (m *Message) Do(driver interface{}) error {
	d, ok := driver.(whisper.Driver)
	if !ok {
		panic("MQ: unknown driver (forgotten import?) ")
	}
	if err := d.Pub(m.Header.GetTopic(), m); err != nil {
		return err
	}
	return nil
}

// Below are two ways to realize retry logic

// if should retry return nil.
func (m *Message) ShouldRetry(lasterr error) error {
	//if lasterr != RetryError {
	//	return NotRetryError
	//}
	//if !throttle.GetToken() {
	//	return OverTokenError
	//}
	if m.retrytime <= 0 {
		return RetryTimeRunOutError
	}
	m.retrytime--
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
	d, ok := driver.(whisper.Driver)
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
