package whisper

import (
	"context"
	"fmt"
)

// from GRPC: https://github.com/grpc/grpc-go/blob/master/server.go
type Client struct {
	opts clientOptions
}

type UnaryInterceptor func(ctx context.Context, msg interface{}) error

type clientOptions struct {
	// ... some options
	endpoint   Endpoint
	middleware []Middleware
	ack        bool
}

type ClientOption interface {
	apply(*clientOptions)
}

type funcClientOption struct {
	f func(*clientOptions)
}

func (fdo *funcClientOption) apply(do *clientOptions) {
	fdo.f(do)
}

func newFuncClientOption(f func(*clientOptions)) *funcClientOption {
	return &funcClientOption{f: f}
}

func ACKsend(b bool) ClientOption {
	return newFuncClientOption(func(o *clientOptions) {
		o.ack = b
	})
}

// from go-kit: https://github.com/go-kit/kit/blob/master/endpoint/endpoint.go
// Endpoint is the fundamental building block of servers and clients.
// It represents a single RPC method.
type Endpoint func(ctx context.Context, request interface{}) error

// Nop is an endpoint that does nothing and returns a nil error.
// Useful for tests.
func Nop(context.Context, interface{}) error { return nil }

// Middleware is a chainable behavior modifier for endpoints.
type Middleware func(Endpoint) Endpoint

// NopM is an Middleware that does nothing and returns a nil error.
// Useful for tests.
func NopM(next Endpoint) Endpoint {
	return func(ctx context.Context, req interface{}) error {
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

// grpc chain call:
// // chainUnaryServerInterceptors chains all unary server interceptors into one.
// func chainUnaryServerInterceptors(s *Client) {
// 	// Prepend opts.unaryInt to the chaining interceptors if it exists, since unaryInt will
// 	// be executed before any other chained interceptors.
// 	interceptors := s.opts.chainUnaryInts
// 	if s.opts.unaryInt != nil {
// 		interceptors = append([]UnaryClientInterceptor{s.opts.unaryInt}, s.opts.chainUnaryInts...)
// 	}

// 	var chainedInt UnaryClientInterceptor
// 	if len(interceptors) == 0 {
// 		chainedInt = nil
// 	} else if len(interceptors) == 1 {
// 		chainedInt = interceptors[0]
// 	} else {
// 		chainedInt = func(ctx context.Context, req interface{}, info *UnaryServerInfo, handler UnaryHandler) (interface{}, error) {
// 			return interceptors[0](ctx, req, info, getChainUnaryHandler(interceptors, 0, info, handler))
// 		}
// 	}

// 	s.opts.unaryInt = chainedInt
// }

// // getChainUnaryHandler recursively generate the chained UnaryHandler
// func getChainUnaryHandler(interceptors []UnaryServerInterceptor, curr int, info *UnaryServerInfo, finalHandler UnaryHandler) UnaryHandler {
// 	if curr == len(interceptors)-1 {
// 		return finalHandler
// 	}

// 	return func(ctx context.Context, req interface{}) (interface{}, error) {
// 		return interceptors[curr+1](ctx, req, info, getChainUnaryHandler(interceptors, curr+1, info, finalHandler))
// 	}
// }

// Dial make a way to send msg to MQ
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

func (c *Client) Send(ctx context.Context, msg interface{}) error {
	if err := c.opts.endpoint(ctx, msg); err != nil {
		fmt.Println(err)
		return err
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
