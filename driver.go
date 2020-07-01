package whisper

import (
	"context"
	"github.com/silverswords/whisper/message"
)

type Client interface {
	Sender

	// startReceive start to handle msg := client.Receive(). with fn function.
	StartReceive(ctx context.Context, fn interface{}) error
}
type Sender interface {
	Send(ctx context.Context, msg *message.Message) error
}

type Receiver interface {
	Receive(ctx context.Context) (msg *message.Message, err error)
}

type Requests interface {
	Request(ctx context.Context, request *message.Message) (resp *message.Message, err error)
}
type SenderCloser interface {
	Sender
	Closer
}

type ReceiverCloser interface {
	Receiver
	Closer
}

// Opener is the common interface for things that need to be opened.
type Opener interface {
	// Open is a blocking call and ctx is used to stop the Inbound message Receiver/Responder.
	// Closing the context won't close the Receiver/Responder, aka it won't invoke Close(ctx).
	Open(ctx context.Context) error
}

// Closer is the common interface for things that can be closed.
// After invoking Close(ctx), you cannot reuse the object you closed.
type Closer interface {
	Close(ctx context.Context) error
}

type Driver interface {
	Sender
	Receiver
}

type UnSubscriber interface {
	//Drain() error
	Unsubscribe() error
}
