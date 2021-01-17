package protocol

import (
	"context"
)

// deprecated
type Client interface {
	Sender

	// startReceive start to handle msg := client.Receive(). with fn function.
	StartReceive(ctx context.Context, fn interface{}) error
}

type Requests interface {
	Request(ctx context.Context, request []byte) (resp []byte, err error)
}

type SenderCloser interface {
	Sender
	Closer
}

type Sender interface {
	Send(ctx context.Context, msg []byte) error
}

type Receiver interface {
	Receive(ctx context.Context) (msg []byte, err error)
}

// Closer is the common interface for things that can be closed.
type Closer interface {
	Close() error
}
