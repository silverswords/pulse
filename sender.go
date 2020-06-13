package whisper

// how to use, new a Executor with MQ driver. and append new message to executor queue.
import (
	"errors"
	"io"
	"io/ioutil"
	"log"
)

var (
	RetryError   = errors.New("please retry")
	DriverError  = errors.New("interface{] isn't driver")
	HandlerError = errors.New("interface{} isn't handler")
)

var Loopback = NewSender(LoopbackDriver)

func LoopPublish(m *Message) { Loopback.Send(m) }

// Sender only to send every message with the driver.
type Sender struct {
	q chan *Message
	driver interface{} // use for every handler to do the thing.

	closed chan struct{}
}

func NewSender(driver Driver) *Sender {
	s := &Sender{
		q: make(chan *Message,100),
		driver: driver,
		closed: make(chan struct{},1),
	}
	go s.execution()
	return s
}

// send message in Sender until colsed.
func (s *Sender) execution() {
	for {
		select {
		case msg := <-s.q:
			{
				log.Println("sender get and send: ",msg)
				msg.Do(s.driver)
			}
		case _ = <-s.closed:
			return
		}
	}
}

// Send Would Block when channel full of Message
func(s *Sender) Send(msg *Message) {
	s.q <- msg
}

func (s *Sender) Stop() {
	s.closed <- struct{}{}
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
