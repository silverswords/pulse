package nats_deprecated

import (
	"context"
	"github.com/nats-io/nats.go"
	"github.com/silverswords/whisper"
)

type Sender struct {
	Conn *nats.Conn

	topic    string
	holdConn bool
}

func NewSender(url string, natsOpts []nats.Option) (*Sender, error) {
	conn, err := nats.Connect(url, natsOpts...)
	if err != nil {
		return nil, err
	}

	return &Sender{
		Conn:     conn,
		holdConn: true,
	}, nil
}

func NewSenderFromConn(conn *nats.Conn, topic string) (*Sender, error) {
	return &Sender{Conn: conn, topic: topic}, nil
}

func (s *Sender) Send(ctx context.Context, in *whisper.Message) (err error) {
	var topic string
	if topic = in.Topic(); topic == "" {
		topic = s.topic
	}
	return s.Conn.Publish(topic, whisper.ToByte(in))
}

// Close implements Closer.Close
// This method only closes the connection if the Sender opened it
func (s *Sender) Close(_ context.Context) error {
	if s.holdConn {
		s.Conn.Close()
	}

	return nil
}

type SenderOption func(*Sender) error

func (s *Sender) applyOptions(opts ...SenderOption) error {
	for _, fn := range opts {
		if err := fn(s); err != nil {
			return err
		}
	}
	return nil
}
