package main

import "context"

func main() {
	var f foo
	Chain(NopM)(f.nothing)
}

type foo struct{}

func (f foo) nothing(ctx context.Context, msg interface{}) error { return nil }

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
