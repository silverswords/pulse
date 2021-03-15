package natsstreaming

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"github.com/silverswords/pulse/pkg/logger"
	"github.com/silverswords/pulse/pkg/pubsub/driver"
	"time"
)

type connector struct {
	metadata metadata

	logger logger.Logger

	// design from dapr pubsub package
	ctx    context.Context
	cancel context.CancelFunc
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	clientID := genRandomString(20)
	opts := []nats.Option{nats.Name(clientID)}
	natsConn, err := nats.Connect(c.metadata.natsURL, opts...)
	if err != nil {
		return nil, fmt.Errorf("nats-streaming: error connecting to nats server at %s: %s", c.metadata.natsURL, err)
	}
	natStreamingConn, err := stan.Connect(c.metadata.natsStreamingClusterID, clientID, stan.NatsConn(natsConn))
	if err != nil {
		return nil, fmt.Errorf("nats-streaming: error connecting to nats streaming server %s: %s", c.metadata.natsStreamingClusterID, err)
	}
	c.logger.Debugf("connected to natsstreaming at %s", c.metadata.natsURL)

	ctx, cancel := context.WithCancel(ctx)
	c.ctx = ctx
	c.cancel = cancel

	return &natsStreamingConn{stanConn: natStreamingConn, logger: c.logger, timeout: 2 * time.Minute}, nil
}

func (c *connector) Driver() driver.Driver {
	return &PubSubDriver{log: c.logger}
}

var _ driver.Connector = (*connector)(nil)
