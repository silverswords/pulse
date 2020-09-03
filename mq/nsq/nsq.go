package nsq

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/nsqio/go-nsq"
	"github.com/silverswords/pulse/pkg/components/mq"
)

const (
	DriverName = "nsq"
	URL        = "nsqURL"
	DefaultURL = "127.0.0.1:4150"
)

func init() {
	mq.Registry.Register(DriverName, func() mq.Driver {
		return NewNsq()
	})
}

type metadata struct {
	nsqURL string
}

func parseMetadata(meta mq.Metadata) (metadata, error) {
	m := metadata{}
	if val, ok := meta.Properties[URL]; ok && val != "" {
		if m.nsqURL, ok = val.(string); ok {
			return m, nil
		}
		return m, errors.New("nsq error: nsq URL is not a string")
	}
	return m, errors.New("nsq error: nsq URL is not exist")
}

func NewNsq() *Driver {
	return &Driver{}
}

type Driver struct {
	metadata
	producer            *nsq.Producer
	channelSerialNumber int
}

func (n *Driver) Init(metadata mq.Metadata) error {
	m, err := parseMetadata(metadata)
	if err != nil {
		fmt.Println(err)
	}
	n.metadata = m

	p, err := nsq.NewProducer(m.nsqURL, nsq.NewConfig())
	if err != nil {
		return err
	}
	n.producer = p
	return nil
}

func (n *Driver) Publish(topic string, in []byte) error {
	err := n.producer.Publish(topic, in)
	if err != nil {
		return err
	}
	return nil
}

func (n *Driver) Subscribe(topic string, handler func(msg []byte)) (mq.Closer, error) {
	var (
		MsgHandler = func(m *nsq.Message) error {
			handler(m.Body)
			return nil
		}
	)
	n.channelSerialNumber++
	ch := "ch" + strconv.Itoa(n.channelSerialNumber)
	con, err := nsq.NewConsumer(topic, ch, nsq.NewConfig())
	if err != nil {
		return nil, err
	}

	con.AddHandler(nsq.HandlerFunc(MsgHandler))
	err = con.ConnectToNSQD(DefaultURL)
	if err != nil {
		return nil, err
	}

	return &consumer{con: con}, nil
}

type consumer struct {
	con *nsq.Consumer
}

func (c *consumer) Close() error {
	c.con.Stop()
	return nil
}

func (n *Driver) Close() error {
	n.producer.Stop()
	return nil
}

var _ mq.Driver = (*Driver)(nil)
var _ mq.Closer = (*Driver)(nil)
