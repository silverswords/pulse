package gob

import (
	"bytes"
	"encoding/gob"
	"github.com/silverswords/pulse/pkg/adapter"
)

// Name is the name registered for the proto compressor.
const Name = "gob"

func init() {
	adapter.RegisterCodec(codec{})
}

// gob is a Codec implementation with gob. It is the default codec for protocol.
type codec struct{}

func (c codec) Marshal(data interface{}) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(data)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c codec) Unmarshal(data []byte, to interface{}) error {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	return dec.Decode(to)
}

func (codec) Name() string {
	return Name
}
