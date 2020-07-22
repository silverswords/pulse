package nats

import (
	"context"
	nats "github.com/nats-io/nats.go"
	"log"
	"time"
)

const DefaultURL = "nats://39.105.141.168:4222"

func init() {

}
type metadata struct {
	sOpts []SenderOption
	rOpts []ReceiverOption
	dOpts []DriverOption
}

type Driver struct {
	metadata
	Conn *nats.Conn
	*Sender
	sOpts []SenderOption
	*Receiver
	rOpts    []ReceiverOption
	holdConn bool
}

func NewDriver(url, subTopic string, natsOpts []nats.Option, opts ...DriverOption) (*Driver, error) {
	conn, err := nats.Connect(url, natsOpts...)
	if err != nil {
		return nil, err
	}

	d, err := NewDriverFromConn(conn, subTopic, opts...)
	if err != nil {
		conn.Close()
		return nil, err
	}
	d.holdConn = true

	return d, nil
}
func NewDriverFromConn(conn *nats.Conn, subTopic string, opts ...DriverOption) (*Driver, error) {
	var err error
	d := &Driver{
		Conn: conn,
	}

	if err := d.applyOptions(opts...); err != nil {
		return nil, err
	}

	if d.Sender, err = NewSenderFromConn(conn); err != nil {
		return nil, err
	}

	if d.Receiver, err = NewReceiverFromConn(conn, subTopic); err != nil {
		return nil, err
	}
	return d, nil
}
func SetupConnOptions(opts []nats.Option) []nats.Option {
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

// Close implements Closer.Close
func (d *Driver) Close(ctx context.Context) error {
	if d.holdConn {
		defer d.Conn.Close()
	}

	if err := d.Receiver.Close(ctx); err != nil {
		return err
	}

	if err := d.Sender.Close(ctx); err != nil {
		return err
	}

	return nil
}

type DriverOption func(d *Driver) error

func (d *Driver) applyOptions(opts ...DriverOption) error {
	for _, fn := range opts {
		if err := fn(d); err != nil {
			return err
		}
	}
	return nil
}
