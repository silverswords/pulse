package whisper

import (
	"time"
)

type Driver interface {
	Pub(topic string, msg *Message) error
	Sub(topic string) (*Suber, error)
}

type Suber interface {
	// Receive is a blocked method until get a message
	// todo : add a timeout avoid blocked forever
	Receive(timeout time.Duration) (*Message,error)
}