package whisper

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"sync"
)
// be functionally
const ACKchannel = "sys/ack"
// func Do() error ---> Handler , http.Handler

var (
	HeaderOptionHandlers map[string]Handler

	RetryError = errors.New("The Condition Need Retry")

)

func Desc(h ...Handler) Handler {

}

func Retry(handler Handler) Handler{
	// do own thing
	return func (){
		handler.Do()
	}}
}

type Handler interface{
	Do(interface{}) error
}

type Actions []Handler

// Pop only for one goroutine
func (a *Actions) Pop() Handler{
	for len(*a) == 0 {}
	handler := (*a)[0]
	*a = (*a)[1:]
	return handler
}

func (a *Actions) Push(handler Handler) {
	*a = append(*a, handler)
}

type Topic struct {
	Driver
	Queue Actions
	opts topicOptions
}

func (t *Topic) sendRoutine() error {
	handler := t.Queue.Pop()

	if err := handler.Do(t.Driver); err != nil {
		if err ==RetryError {
		//	retryThrottler(retry())
		}
		return err
		// Retry or not
	}
	return nil
}

// Message has imply Handler interface.
type Message struct {
	Header Header
	Body []byte
	ACK uint64
}

func (m *Message) Do(driver Driver) error {
	if err := driver.Pub(m.Header.Get("topic"),m); err != nil {
		return err
	}
	return nil
}
// Send send msg to target topic
func (c *Client) Send(topic string, msg Message) error {
	if c.closed {
		return errors.New("client closed")
	}
	ctx := context.WithValue(context.Background(), "topic", topic)
	if err := c.opts.endpoint(ctx, &msg); err != nil {
		return err
	}
	return nil
}

// Receive is a blocked method until get a message
func (c *Client) Receive(topic string) (*Message, error) {
	if c.closed {
		return nil, errors.New("client closed")
	}

	subs, err := c.conn.Sub(topic)
	if err != nil {
		return nil, err
	}
	raw := subs.Receive()
	var msg Message
	Decode(raw, &msg)

	ctx := context.WithValue(context.Background(), "topic", topic)
	if err := c.opts.subE(ctx, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

// ReceiveChan open a goroutine to process message with endpoint. it may cause error and just fmt.Println(err)
// notice to handle the hidden errors
func (c *Client) ReceiveChan(topic string) (chan *Message, error) {
	if c.closed {
		return nil, errors.New("client closed")
	}
	subs, err := c.conn.Sub(topic)
	if err != nil {
		return nil, err
	}

	var ch  = make(chan *Message,64)
	go func() {
		raw := subs.Receive()
		var msg Message
		Decode(raw, &msg)

		ch <- &msg

		ctx := context.WithValue(context.Background(), "topic", topic)
		if err := c.opts.subE(ctx, &msg); err != nil {
			fmt.Println(err)
		}
	}()
	return ch,nil
}

// Close graceful all the conn
func (c *Client) Close() {
	c.closeCh <- struct{}{}
	c.closed = true
}



func chainEndpoint(c *Client) {
	c.opts.endpoint = Chain(c.waitACKM(), c.opts.middleware...)(c.sendH)
	c.opts.subE = Chain(c.ACKM(), c.opts.subM...)(Nop)
}

func (c *Client) ACKM() Middleware {
	return func(next Endpoint) Endpoint {
		return func(ctx context.Context, msg *Message) error {
			// ack
			defer func() {
				c.sendACK(msg.ACK)
			}()
			next(ctx, msg)
			return nil
		}
	}
}
func (c *Client) waitACKM() Middleware {
	return func(next Endpoint) Endpoint {
		return func(ctx context.Context, msg *Message) error {
			// ack
			defer func() {
				if !c.opts.ack {
					return
				}
				c.Lock()
				c.ackmap[msg.ACK] = true
				c.queue = append(c.queue, msg)
				c.Unlock()
				fmt.Println("Debug: current wait ack msg: ", c.queue)
			}()
			next(ctx, msg)
			return nil
		}
	}
}

func (c *Client) sendH(ctx context.Context, msg *Message) error {
	topic, ok := ctx.Value("topic").(string)
	if !ok {
		return errors.New("don't have topic to send to")
	}

	raw, err := Encode(msg)
	if err != nil {
		return err
	}

	// all in endpoint
	err = c.conn.Pub(topic, raw)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) sendACK(ack uint64) error {
	raw, err := Encode(Message{ACK: ack})
	if err != nil {
		return err
	}
	return c.conn.Pub(ACKchannel, raw)
}

type topicOptions struct {
	// pubsub mq set
	url string
	pubsubOptions interface{}

	// send topic options
	endpoint   Endpoint
	middleware []Middleware

	// receive topic options
	subE Endpoint
	subM []Middleware

	// ack means if have ack to drop the msg in client
	ack        bool
}

// ClientOption supply the setup of Client
type ClientOption interface {
	apply(*topicOptions)
}

type funcClientOption struct {
	f func(*topicOptions)
}

func (fdo *funcClientOption) apply(do *topicOptions) {
	fdo.f(do)
}

func defaultOptions() topicOptions {
	return topicOptions{}
}

func newFuncClientOption(f func(*topicOptions)) *funcClientOption {
	return &funcClientOption{f: f}
}

// ACKsend config if need ack
func ACKsend(b bool) ClientOption {
	return newFuncClientOption(func(o *topicOptions) {
		o.ack = b
	})
}

func WithURL(url string) ClientOption{
	return newFuncClientOption(func(o *topicOptions){
		o.url = url
	})
}

// Middleware supply
func WithPrintMsg()ClientOption {
	return newFuncClientOption(func (o *topicOptions) {
		o.middleware = append(o.middleware, func(next Endpoint) Endpoint {
			return func(ctx context.Context, msg *Message) error {
				// ack
				defer func() {
					log.Println("Send: " , msg)
				}()
				next(ctx, msg)
				return nil
			}
		})
	})
}

// from go-kit: https://github.com/go-kit/kit/blob/master/endpoint/endpoint.go

// Endpoint is the fundamental building block of servers and clients.
// It represents a single RPC method.
type Endpoint func(ctx context.Context, request *Message) error

// Nop is an endpoint that does nothing and returns a nil error.
// Useful for tests.
func Nop(context.Context, *Message) error { return nil }

// Middleware is a chainable behavior modifier for endpoints.
type Middleware func(Endpoint) Endpoint

// NopM is an Middleware that does nothing and returns a nil error.
// Useful for tests.
func NopM(next Endpoint) Endpoint {
	return func(ctx context.Context, req *Message) error {
		return next(ctx, req)
	}
}

// Chain is a helper function for composing middlewares. Requests will
// traverse them in the order they're declared. That is, the first middleware
// is treated as the outermost middleware.
func Chain(outer Middleware, others ...Middleware) Middleware {
	return func(next Endpoint) Endpoint {
		for i := len(others) - 1; i >= 0; i-- { // reverse
			next = others[i](next)
		}
		return outer(next)
	}
}

// Failer may be implemented by Go kit response types that contain business
// logic error details. If Failed returns a non-nil error, the Go kit transport
// layer may interpret this as a business logic error, and may encode it
// differently than a regular, successful response.
//
// It's not necessary for your response types to implement Failer, but it may
// help for more sophisticated use cases. The addsvc example shows how Failer
// should be used by a complete application.
type Failer interface {
	Failed() error
}

// example
func annotate(s string) Middleware {
	return func(next Endpoint) Endpoint {
		return func(ctx context.Context, request *Message) error {
			fmt.Println(s, "pre")
			defer fmt.Println(s, "post")
			return next(ctx, request)
		}
	}
}

