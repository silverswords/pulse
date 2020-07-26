package client

import (
	"context"
	"github.com/silverswords/whisper"
	cecontext "github.com/silverswords/whisper/context"
	driver2 "github.com/silverswords/whisper/driver"
	"github.com/silverswords/whisper/message"
)

type Client struct {
	driver driver2.Driver
	opener whisper.Opener

	q          *Queue
	callbackFn interface{}
}

func (c *Client) Send(ctx context.Context, m *message.Message) error {
	return c.driver.Send(ctx, m)
}

func (c *Client) StartReceive(ctx context.Context, callbackFn interface{}) error {
	// start open inbound.
	if c.opener != nil {
		go func() {
			if err := c.opener.Open(ctx); err != nil {
				cecontext.LoggerFrom(ctx).Errorf("Error while opening the inbound connection: %s", err)
			}
		}()
	}

	msg, err := c.driver.Receive(ctx)
	if err != nil {
		return err
	}
	c.q.Append(msg)
	c.callbackFn = callbackFn
	return nil
}

// from kubernetes to handle queue message.
func Worker(ctx context.Context) {

}

func NewClient(driver driver2.Driver, opts ...Option) (c *Client, err error) {
	c = &Client{driver: driver, q: NewQueue()}

	if d, ok := driver.(whisper.Opener); ok {
		c.opener = d
	}

	if err = c.applyOptions(opts...); err != nil {
		return nil, err
	}
	return c, nil
}

type Option func(*Client) error

func (c *Client) applyOptions(opts ...Option) error {
	for _, fn := range opts {
		if err := fn(c); err != nil {
			return err
		}
	}
	return nil
}
