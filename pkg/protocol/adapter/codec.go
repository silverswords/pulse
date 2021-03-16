package adapter

import (
	"context"
	"errors"
	"github.com/silverswords/pulse/pkg/protocol"
	"github.com/silverswords/pulse/pkg/visitor"
	"strings"
)

func init() {
	RegisterCodec(&bytesCodec{})
}

var registeredCodecs = make(map[string]Codec)

func CreateCodec(name string) (Codec, error) {
	if registeredCodecs[name] != nil {
		return registeredCodecs[name], nil
	}
	return nil, errors.New("no target codec registered")
}

type EncodeVisitor struct {
	visitor visitor.Visitor
	Codec
}

// r should be PublishRequest
func (enc *EncodeVisitor) Do(fn visitor.DoFunc) error {
	return enc.visitor.Do(func(ctx context.Context, r interface{}) error {
		msg := r.(*protocol.Message)
		rawData, err := enc.Codec.Marshal(msg)
		if err != nil {
			return err
		}
		msg.SetRawData(rawData)

		return fn(ctx, r)
	})
}

type DecodeVisitor struct {
	visitor visitor.Visitor
	Codec
}

func (dec *DecodeVisitor) Do(fn visitor.DoFunc) error {
	return dec.visitor.Do(func(ctx context.Context, r interface{}) (err error) {
		b := r.([]byte)
		msg := &protocol.Message{}
		err = dec.Codec.Unmarshal(b, msg)
		if err != nil {
			return err
		}
		return fn(ctx, msg)
	})
}

// RegisterCodec registers the provided Codec for use with all gRPC clients and
// servers.
//
// The Codec will be stored and looked up by result of its Name() method, which
// should match the content-subtype of the encoding handled by the Codec.  This
// is case-insensitive, and is stored and looked up as lowercase.  If the
// result of calling Name() is an empty string, RegisterCodec will panic. See
// Content-Type on
// https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md#requests for
// more details.
//
// NOTE: this function must only be called during initialization time (i.e. in
// an init() function), and is not thread-safe.  If multiple Compressors are
// registered with the same name, the one registered last will take effect.
func RegisterCodec(codec Codec) {
	if codec == nil {
		panic("cannot register a nil Codec")
	}
	if codec.Name() == "" {
		panic("cannot register Codec with empty string result for Name()")
	}
	contentSubtype := strings.ToLower(codec.Name())
	registeredCodecs[contentSubtype] = codec
}

// Codec defines the interface gRPC uses to encode and decode messages.  Note
// that implementations of this interface must be thread safe; a Codec's
// methods can be called from concurrent goroutines.
type Codec interface {
	// Marshal returns the wire format of v.
	Marshal(v interface{}) ([]byte, error)
	// Unmarshal parses the wire format into v.
	Unmarshal(data []byte, v interface{}) error
	// Name returns the name of the Codec implementation. The returned string
	// will be used as part of content type in transmission.  The result must be
	// static; the result cannot change between calls.
	Name() string
}

type bytesCodec struct{}

func (c *bytesCodec) Marshal(v interface{}) ([]byte, error) {
	if b, ok := v.([]byte); ok {
		return b, nil
	}
	return nil, errors.New("not []byte")
}

// nolint
func (c *bytesCodec) Unmarshal(data []byte, v interface{}) (err error) {
	if _, ok := v.([]byte); ok {
		err = errors.New("receiver is not []byte")
	}
	v = data
	return
}

func (c *bytesCodec) Name() string {
	return "bytes"
}
