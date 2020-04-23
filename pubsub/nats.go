package pubsub

import (
	"context"
	nats "github.com/nats-io/nats.go"
	"log"
	"time"
)

const DefaultURL =  "nats://39.105.141.168:4222"

var nc,_ = nats.Connect(DefaultURL,setupConnOptions([]nats.Option{})...)

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

// Send send msg to nc
func Send(ctx context.Context, msg interface{}) error {
	nc.Publish(ctx.Value("subject").(string),msg.([]byte))
	return nil
}
