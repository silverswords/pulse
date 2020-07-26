package singleChan

import (
	"context"
	"errors"
	"fmt"
	"github.com/silverswords/whisper/driver"
	"github.com/silverswords/whisper/message"
	"io"
)

const defaultChanDepth = 20

type Driver struct {
	Sender
	Receiver
}

func NewDriver() *Driver {
	ch := make(chan *message.Message, defaultChanDepth)

	return &Driver{
		Sender:   Sender(ch),
		Receiver: Receiver(ch),
	}
}

type Sender chan<- *message.Message

func (s Sender) Send(ctx context.Context, m *message.Message) error {
	if ctx == nil {
		return fmt.Errorf("nil Context")
	} else if m == nil {
		return fmt.Errorf("nil Message")
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case s <- m:
		return nil
	}
}

func (d Driver) Close(ctx context.Context) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("trying to close a closed Sender")
		}
	}()
	close(d.Sender)
	return nil
}

type Receiver <-chan *message.Message

func (r Receiver) Receive(ctx context.Context) (*message.Message, error) {
	if ctx == nil {
		return nil, fmt.Errorf("nil Context")
	}

	select {
	case <-ctx.Done():
		return nil, io.EOF
	case m, ok := <-r:
		if !ok {
			return nil, io.EOF
		}
		return m, nil
	}
}

var _ driver.Sender = (*Sender)(nil)
var _ driver.Receiver = (*Receiver)(nil)
