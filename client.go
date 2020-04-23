package whisper

import (
	"context"
	"fmt"
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
	opts clientOptions
	ackmap map[uint64]bool
	queue []message
}

// =========================================================================

// Dial make a way to send msg to MQ todo
func Dial(ctx context.Context, target string, opts ...ClientOption) (client *Client, err error) {
	c := &Client{
		opts: defaultOptions(),
	}

	// newclient context.Context and cancel  and ratelimit \ retryThrottler roundroubin etc.
	for _, opt := range opts {
		opt.apply(&c.opts)
	}

	chainEndpoint(c)

	defer func() {
		if err != nil {
			c.Close()
		}
	}()

	return c, nil
}

// Send send msg to target topic
func (c *Client) Send(ctx context.Context, msg interface{}) error {
	if err := c.opts.endpoint(ctx, msg); err != nil {
		fmt.Println(err)
		return err
	}

	if !c.opts.ack {

	}
	return nil
}

// Close graceful all the conn
func (c *Client) Close() {

}

func chainEndpoint(c *Client) {
	c.opts.endpoint = Chain(NopM, c.opts.middleware...)(Nop)
}

func defaultOptions() clientOptions {
	return clientOptions{}
}
