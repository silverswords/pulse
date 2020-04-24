package whisper

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/silverswords/whisper/pubsub"
	"sync"
)

const ACKchannel = "sys/ack"

// send logic
// 1. dial to target MQ
// 2. get msg, then process and send to MQ
// 3. if ack, wait for MQ get ack for previous msg.
// 4. if not, directly drop the msg in client queue. means that mag arrived.

// sub logic
// 1. dial to target MQ and sub a topic
// 2. get msg, and return ack to MQ

// todo: required
// = use database/sql to suit various MQ
// design in client
// every client have its own options, like its own middleware.
// subscription is derived from a client. and use a client could directly send msg.
// so in client, there is a queue to keep unconfirmed msg
// = use grpc client option to configure this eventbus
// = use go-kit endpoint to handle and process msg lifecycle
// = use ctx to control retry, timeout, ratelimit, trcaing etc.
// how to metric?
// design in msg
// ack or seq imitate tcp to ensure message arrived.
// imitate http, codec and application/json in msg.Header(key[value]).
// design in topic
// topic imitate URL to manage

// Client use to manage the send conn to the MQ
// from GRPC: https://github.com/grpc/grpc-go/blob/master/server.go
type Client struct {
	conn pubsub.Conn
	opts clientOptions

	//ach ch receive the mq's ack respond
	ackch chan uint64
	// ack map to ensure which msg sended
	ackmap map[uint64]bool

	// queue to make send msg
	queue []*Message

	sync.RWMutex

	closeCh chan struct{}
	closed  bool
}

// =========================================================================

// if options changed, reconnect and get a new conn to send
// Dial make a way to send msg to MQ
func Dial(driverName string, opts ...ClientOption) (client *Client, err error) {
	c := &Client{
		opts:    defaultOptions(),
		ackch:   make(chan uint64, 1),
		ackmap:  make(map[uint64]bool),
		closeCh: make(chan struct{}),
	}

	// newclient context.Context and cancel  and ratelimit \ retryThrottler roundroubin etc.
	for _, opt := range opts {
		opt.apply(&c.opts)
	}

	// topic Middlewares in one endpoint
	chainEndpoint(c)

	c.conn, err = pubsub.Open(driverName, c.opts.url, c.opts.pubsubOptions)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			c.Close()
		}
	}()

	// ack goroutine
	if c.opts.ack {
		go func() {
			subs,err := c.conn.Sub(ACKchannel)
			if err != nil {
				fmt.Println("ACK Subscribe error")
			}
			for !c.closed {
				rawMsg := subs.Receive()
				var msg Message
				err := Decode(rawMsg, &msg)
				if err != nil {
					fmt.Println("[Error] In Decode Message: ", err)
				}
				fmt.Println("Debug: ", "receive the msg ack: ", msg.ACK)
				c.Lock()
				for i, x := range c.queue {
					if x.ACK == msg.ACK {
						if len(c.queue) ==1 {
							c.queue = c.queue[0:0]
						}else {
							c.queue = append(c.queue[0:i-1], c.queue[i:]...)
						}
						c.ackmap[msg.ACK] = false
						c.Unlock()
						break
					}
					// ensure unlock
					c.Unlock()
				}
			}
			fmt.Println("These msg not ack: ", c.queue)
		}()
	}

	return c, nil
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

func(c *Client) Receive(topic string) (*Message, error) {
	if c.closed {return nil,errors.New("client closed")}

	subs,err := c.conn.Sub(topic)
	if err != nil {return nil, err}
	raw := subs.Receive()
	var msg Message
	Decode(raw,&msg)

	ctx := context.WithValue(context.Background(), "topic", topic)
	if err := c.opts.subE(ctx, &msg); err != nil {return nil, err}
	return &msg,nil
}

// Close graceful all the conn
func (c *Client) Close() {
	c.closeCh <- struct{}{}
	c.closed = true
}

func defaultOptions() clientOptions {
	return clientOptions{}
}

func chainEndpoint(c *Client) {
	c.opts.endpoint = Chain(c.waitACKM(), c.opts.middleware...)(c.sendH)
	c.opts.subE = Chain(c.ACKM(),c.opts.subM...)(Nop)
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
	if err != nil {return err}
	return nil
}

func( c *Client) sendACK(ack uint64) error {
	raw, err := Encode(Message{ACK: ack})
	if err != nil { return err}
	return c.conn.Pub(ACKchannel,raw)
}

func Encode(data interface{}) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(data)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func Decode(data []byte, to interface{}) error {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	return dec.Decode(to)
}
