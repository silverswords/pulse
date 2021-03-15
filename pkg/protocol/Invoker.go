package protocol

import (
	"context"
)

// invoker is old implementation for pubsub. It wait message after publishing event. Although use PubSub pattern for decoupled waiting,
// still block to receive the response. What nothing changed but the surface of the request/response pattern.
// In fact:
// 1. network is not trustable.
// 2. No one should expected the response of self request.
// 3. Programmer make publisher and subscribers couple combined to struct. So they think requester and responser is same one.
// So we need PubSub Which the real pattern match to the real world.
// Puber is no way like Suber, although they could be one struct. request/response just like subscribe topic and then publish event to the topic.

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
