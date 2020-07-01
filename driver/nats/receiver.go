package nats

import (
	"context"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/silverswords/whisper"
	"github.com/silverswords/whisper/message"
	"io"
	"sync"
)

type Receiver struct {
	incoming chan *message.Message
	Conn     *nats.Conn

	Subscriber Subscriber
	Topic      string

	subMtx   sync.Mutex
	closedCh chan struct{}
	// use to do not close conn once more.
	holdConn bool
}

type ReceiverOption func(*Receiver) error

func WithQueueSubscriber(queue string) ReceiverOption {
	return func(r *Receiver) error {
		if queue == "" {
			return errors.New("Invalid queue name")
		}
		r.Subscriber = &QueueSubscriber{Queue: queue}
		return nil
	}
}

func NewReceiver(url, topic string, natsOpts []nats.Option, opts ...ReceiverOption) (*Receiver, error) {
	conn, err := nats.Connect(url, natsOpts...)
	if err != nil {
		return nil, err
	}

	r, err := NewReceiverFromConn(conn, topic, opts...)
	if err != nil {
		conn.Close()
		return nil, err
	}

	r.holdConn = true

	return r, err
}

func NewReceiverFromConn(conn *nats.Conn, topic string, opts ...ReceiverOption) (*Receiver, error) {
	r := &Receiver{
		incoming:   make(chan *message.Message),
		Conn:       conn,
		Topic:      topic,
		Subscriber: &RegularSubscriber{},
		// capacity 1 avoid close method block.
		closedCh: make(chan struct{}, 1),
	}

	err := r.applyOptions(opts...)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// MsgHandler implements nats.MsgHandler and publishes messages onto our internal incoming channel to be delivered
// via r.Receive(ctx)
func (r *Receiver) MsgHandler(m *nats.Msg) {
	msg, err := message.ToMessage(m.Data)
	if err != nil {
		fmt.Println("Not whisper message: ", err)
		return
	}
	r.incoming <- msg
}

func (r *Receiver) Receive(ctx context.Context) (*message.Message, error) {
	select {
	case msg, ok := <-r.incoming:
		if !ok {
			return nil, io.EOF
		}
		return msg, nil
	case <-ctx.Done():
		return nil, io.EOF
	}
}

func (r *Receiver) Open(ctx context.Context) error {
	r.subMtx.Lock()
	defer r.subMtx.Unlock()

	sub, err := r.Subscriber.Subscribe(r.Conn, r.Topic, r.MsgHandler)
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
	case <-r.closedCh:
	}

	// Drain promise that every message has been received.
	return sub.Drain()
}

func (r *Receiver) Close(ctx context.Context) error {
	r.closedCh <- struct{}{}
	r.subMtx.Lock()
	defer r.subMtx.Unlock()

	if r.holdConn {
		r.Conn.Close()
	}

	close(r.closedCh)

	return nil
}

func (c *Receiver) applyOptions(opts ...ReceiverOption) error {
	for _, fn := range opts {
		if err := fn(c); err != nil {
			return err
		}
	}
	return nil
}

var _ whisper.ReceiverCloser = (*Receiver)(nil)
var _ whisper.Opener = (*Receiver)(nil)
