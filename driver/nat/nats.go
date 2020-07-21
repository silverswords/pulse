package nat

import "github.com/nats-io/nats.go"

const DefaultURL = "nats://39.105.141.168:4222"

func init() {
	// use to register the nats to pubsub driver factory
}

type NatsDriver struct {
	Conn *nats.Conn
}

func NewNats(metadata)
