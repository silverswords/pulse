package redis

//https://redis.io/topics/pubsub
import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"github.com/silverswords/pulse/pkg/components/mq"
)

// todo: https://github.com/dapr/components-contrib/blob/master/pubsub/redis/redis.go
const (
	URL = "redisURL"

	DefaultURL = "redis://:123456@192.168.0.253:32220/0"
)

func init() {
	// use to register the nats to pubsub mq factory
	mq.Registry.Register("redis", func() mq.Driver {
		return NewRedis()
	})
	//log.Println("Register the nats mq")
}

type metadata struct {
	options *redis.Options
}

func parseNATSMetadata(meta mq.Metadata) (m metadata, err error) {
	m = metadata{}
	if val, ok := meta.Properties[URL]; ok && val != "" {
		if s, ok := val.(string); ok {
			m.options, err = redis.ParseURL(s)
			if err != nil {
				return m, err
			}
		} else {
			return m, errors.New("redis init error: redis URL is not a string")
		}
	} else {
		return m, errors.New("redis init error: missing redis URL: Try redis://localhost:6379/0 if you have a local redis server")
	}

	return m, nil
}

type Driver struct {
	metadata
	redisClient *redis.Client
	stopped     bool
}

func NewRedis() *Driver {
	return &Driver{}
}

// Init initializes the mq and init the connection to the server.
func (d *Driver) Init(metadata mq.Metadata) error {
	m, err := parseNATSMetadata(metadata)
	if err != nil {
		return nil
	}
	d.metadata = m

	c := redis.NewClient(m.options)
	d.redisClient = c
	return nil
}

// Publish publishes a message to EventBus with message destination topic.
// note that there is no error to return.
func (d *Driver) Publish(topic string, in []byte) error {
	if d.stopped {
		return errors.New("draining")
	}
	d.redisClient.Publish(context.Background(), topic, in)
	return nil
}

type Closer func() error

func (c Closer) Close() error {
	return c()
}

// Subscribe handle message from specific topic.
// use context to cancel the subscriber
// in metadata:
// - queueGroupName if not "", will have a queueGroup to receive a message and only one of the group would receive the message.
// handler use to receive the message and move to top level subscriber.
func (d *Driver) Subscribe(topic string, handler func(msg []byte)) (mq.Closer, error) {
	if d.stopped {
		return nil, errors.New("draining")
	}

	ctx := context.Background()
	sub := d.redisClient.Subscribe(ctx, topic)
	iface, err := sub.Receive(ctx)
	if err != nil {
		_ = sub.Close()
		return nil, err
	}

	switch iface.(type) {
	case *redis.Subscription:
		// subscribe succeeded
	case *redis.Message:
		// received first message
	case *redis.Pong:
		// pong received
	default:
		// handle error
	}
	channel := sub.Channel()

	go func() {
		for {
			if d.stopped {
				// drain the message
				go func() {
					for {
						if len(channel) == 0 {
							return
						}
						msg := <-channel
						handler([]byte(msg.Payload))
					}
				}()
			}
			for msg := range channel {
				// todo: make unsafe pointer convert to improve performance.
				handler([]byte(msg.Payload))
			}

		}
	}()

	var closer Closer = func() error {
		//return d.eb.Unsubscribe(topic, handler)
		_ = sub.Close()
		return nil
	}
	return closer, nil
}

func (d *Driver) Close() error {
	return d.redisClient.Close()
}

var _ mq.Driver = (*Driver)(nil)
var _ mq.Closer = (*Driver)(nil)
