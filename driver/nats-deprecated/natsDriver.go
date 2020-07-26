package nats_deprecated

import (
	"context"
	"errors"
	"fmt"
	nats "github.com/nats-io/nats.go"
	"github.com/silverswords/whisper"
	"github.com/silverswords/whisper/message"
	"log"
	"time"
)

const (
	natsURL    = "natsURL"
	DefaultURL = "nats://39.105.141.168:4222"
)

func init() {

}

type Driver struct {
	metadata

	Conn *nats.Conn
	holdConn bool
	*Receiver
}

func NewDriver() whisper.Driver {
	return &Driver{}
}

func (d *Driver) Init(metadata whisper.Metadata) error {
	m, err := parseNATSMetadata(metadata)
	if err != nil {
		return err
	}

	// url to conn. subtopic to topic, opts to driver.
	d.metadata = m
	conn, err := nats.Connect(m.natsURL, m.natsOpts...)
	if err != nil {
		conn.Close()
		return fmt.Errorf("nats: error connecting to nats at %s: %s", m.natsURL, err)
	}
	d.Conn = conn
	d.holdConn = true

	if d.Receiver, err = NewReceiverFromConn(conn, m.topic); err != nil {
		return err
	}

	return nil
}

// parseNATSMetadata parse driver's metadata map[string]string to this nats' metadata
type metadata struct {
	natsURL  string
	natsOpts []nats.Option
	topic    string

	// deprecated: options
	sOpts []SenderOption
	rOpts []ReceiverOption
	dOpts []DriverOption
}

func parseNATSMetadata(meta whisper.Metadata) (metadata, error) {
	m := metadata{}
	if val, ok := meta.Properties[natsURL]; ok && val != "" {
		m.natsURL = val
	} else {
		return m, errors.New("nats error: missing nats URL")
	}

	if m.natsOpts == nil {
		m.natsOpts = setupConnOptions(m.natsOpts)
	}

	return m, nil
}

func setupConnOptions(opts []nats.Option) []nats.Option {
	totalWait := 10 * time.Minute
	reconnectDelay := time.Second

	opts = append(opts, nats.ReconnectWait(reconnectDelay))
	opts = append(opts, nats.MaxReconnects(int(totalWait/reconnectDelay)))
	opts = append(opts, nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
		log.Printf("Disconnected due to:%s, will attempt reconnects for %.0fm", err, totalWait.Minutes())
	}))
	opts = append(opts, nats.ReconnectHandler(func(nc *nats.Conn) {
		log.Printf("Reconnected [%s]", nc.ConnectedUrl())
	}))
	opts = append(opts, nats.ClosedHandler(func(nc *nats.Conn) {
		log.Fatalf("Exiting: %v", nc.LastError())
	}))
	return opts
}

type DriverOption func(d *metadata) error

func (m *metadata) applyOptions(opts ...DriverOption) error {
	for _, fn := range opts {
		if err := fn(m); err != nil {
			return err
		}
	}
	return nil
}
func (d *Driver) Send(ctx context.Context, in *message.Message) (err error) {
	var topic string
	if topic = in.Topic(); topic == "" {
		topic = d.topic
	}
	return d.Conn.Publish(topic, message.ToByte(in))
}

// Close implements Closer.Close
func (d *Driver) Close(ctx context.Context) error {
	d.Conn.Close()

	return nil
}


var _ whisper.Driver = &Driver{}
var _ whisper.Closer = Driver(nil)