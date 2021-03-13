package pulseDriver

import (
	"encoding/json"
	"github.com/nats-io/nats.go"
)

// EncodeJSONRequest is an EncodeRequestFunc that serializes the request as a
// JSON object to the Data of the Msg. Many JSON-over-NATS services can use it as
// a sensible default.
func EncodeJSONRequest(_ context.Context, msg *nats.Msg, request interface{}) error {
	b, err := json.Marshal(request)
	if err != nil {
		return err
	}

	msg.Data = b

	return nil
}

// DecodeRequestFunc extracts a user-domain request object from a publisher
// request object. It's designed to be used in NATS subscribers, for subscriber-side
// endpoints. One straightforward DecodeRequestFunc could be something that
// JSON decodes from the request body to the concrete response type.
type DecodeRequestFunc func(context.Context, *nats.Msg) (request interface{}, err error)

// EncodeRequestFunc encodes the passed request object into the NATS request
// object. It's designed to be used in NATS publishers, for publisher-side
// endpoints. One straightforward EncodeRequestFunc could something that JSON
// encodes the object directly to the request payload.
type EncodeRequestFunc func(context.Context, *nats.Msg, interface{}) error

// EncodeResponseFunc encodes the passed response object to the subscriber reply.
// It's designed to be used in NATS subscribers, for subscriber-side
// endpoints. One straightforward EncodeResponseFunc could be something that
// JSON encodes the object directly to the response body.
type EncodeResponseFunc func(context.Context, string, *nats.Conn, interface{}) error

// DecodeResponseFunc extracts a user-domain response object from an NATS
// response object. It's designed to be used in NATS publisher, for publisher-side
// endpoints. One straightforward DecodeResponseFunc could be something that
// JSON decodes from the response payload to the concrete response type.
type DecodeResponseFunc func(context.Context, *nats.Msg) (response interface{}, err error)
