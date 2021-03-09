package natsstreaming

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"github.com/nats-io/stan.go/pb"
	"github.com/silverswords/pulse/pkg/logger"
	"github.com/silverswords/pulse/pkg/pubsub/driver"
	"time"
)

type connector struct {
	metadata metadata

	log logger.Logger

	// design from dapr pubsub package
	ctx    context.Context
	cancel context.CancelFunc
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	clientID := genRandomString(20)
	c.metadata.natsOpts = append(c.metadata.natsOpts, nats.Name(clientID))
	natsConn, err := nats.Connect(c.metadata.natsURL, c.metadata.natsOpts...)
	if err != nil {
		return fmt.Errorf("nats-streaming: error connecting to nats server at %s: %s", c.metadata.natsURL, err)
	}
	natStreamingConn, err := stan.Connect(c.metadata.natsStreamingClusterID, clientID, stan.NatsConn(natsConn))
	if err != nil {
		return fmt.Errorf("nats-streaming: error connecting to nats streaming server %s: %s", c.metadata.natsStreamingClusterID, err)
	}
	c.logger.Debugf("connected to natsstreaming at %s", c.metadata.natsURL)

	ctx, cancel := context.WithCancel(context.Background())
	c.ctx = ctx
	c.cancel = cancel

	natStreamingConn
}

func (c *connector) Driver() driver.Driver {
	return &NatsStreamingPubSubDriver{}
}

var _ driver.Connector = (*connector)(nil)
