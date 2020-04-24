package whisper

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/silverswords/whisper/pubsub"
	"sync"
	"time"
)

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
	queue []Message

	sync.RWMutex

	closeCh chan struct{}
}

// =========================================================================

// if options changed, reconnect and get a new conn to send
// Dial make a way to send msg to MQ
func Dial(driverName string, opts ...ClientOption) (client *Client, err error) {
	c := &Client{
		opts: defaultOptions(),
		ackch: make(chan uint64,1),
		ackmap: make(map[uint64]bool),
		closeCh: make(chan struct{}),
	}

	// newclient context.Context and cancel  and ratelimit \ retryThrottler roundroubin etc.
	for _, opt := range opts {
		opt.apply(&c.opts)
	}

	// topic Middlewares in one endpoint
	chainEndpoint(c)

	c.conn, err = pubsub.Open(driverName,c.opts.url,c.opts.pubsubOptions)
	defer func() {
		if err != nil {
			c.Close()
		}
	}()

	// ack goroutine
	if c.opts.ack {
		go func() {
			for {
				select {
				case <-c.closeCh:
					return
				case ack := <-c.ackch:
					//	do ack and drop arrived msg
					c.Lock()
					c.ackmap[ack] = false
					for i,x := range c.queue {
						if x.ACK == ack {
							c.queue = append(c.queue[0:i-1],c.queue[i:]...)
							c.Unlock()
							break
						}
					}
					continue
				default:
					time.Sleep(100 * time.Microsecond)
				}
			}
		}()
	}
	return c, nil
}
func  (c *Client) ACKH(ctx context.Context, msg Message) error {
	// ack
	if !c.opts.ack {
		return nil
	}
	c.ackmap[msg.ACK] = true
	c.queue = append(c.queue,msg)

	return nil
}

func (c *Client) SendH(ctx context.Context, msg Message) error {
	topic,ok := ctx.Value("topic").(string)
	if !ok {return errors.New("don't have topic to send to")}

	raw, err := Encode(msg)
	if err!= nil {return err}

	// all in endpoint
	c.conn.Pub(topic,raw)
	return nil
}
// Send send msg to target topic
func (c *Client) Send(topic string, msg Message) error {
	ctx := context.WithValue(context.Background(),"topic",topic)
	if err := c.opts.endpoint(ctx, msg); err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}

// Close graceful all the conn
func (c *Client) Close() {
	c.closeCh <- struct{}{}
}

func chainEndpoint(c *Client) {
	c.opts.endpoint = Chain(NopM, c.opts.middleware...)(Nop)
}

func defaultOptions() clientOptions {
	return clientOptions{}
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