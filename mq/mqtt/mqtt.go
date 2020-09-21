package mqtt

import (
	"errors"
	"fmt"
	"net/url"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/silverswords/pulse/pkg/components/mq"
)

const (
	// URL is the key for mqtt url in metadata
	URL = "mqttURL"
	// Options is the key for mqtt options in metadata
	Options = "mqttOptions"
	// DefaultURL -
	DefaultURL = "tcp://127.0.0.1:1883"
)

func init() {
	// use to register the mqtt to pubsub mq factory
	mq.Registry.Register("mqtt", func() mq.Driver {
		return NewMQTT()
	})
	//log.Println("Register the mqtt mq")
}

// Driver is the mqtt implementation for driver interface
type Driver struct {
	metadata
	client mqtt.Client
}

type metadata struct {
	mqttURL  string
	mqttOpts *mqtt.ClientOptions
}

// NewMQTT -
func NewMQTT() mq.Driver {
	return &Driver{}
}

func parseMQTTMetaData(md mq.Metadata) (metadata, error) {
	m := metadata{}

	// required configuration settings
	if val, ok := md.Properties[URL]; ok && val != nil {
		if m.mqttURL, ok = val.(string); !ok {
			return m, errors.New("mqtt error: mqtt URL is not a string")
		}
	} else {
		return m, errors.New("mqtt error: missing mqtt URL")
	}

	if val, ok := md.Properties[Options]; ok && val != nil {
		if m.mqttOpts, ok = val.(*mqtt.ClientOptions); !ok {
			return m, errors.New("mqtt error: missing mqtt Options and not use default")
		}
	} else {
		mqttOpts, err := createClientOptions(m.mqttURL)
		if err != nil {
			return m, err
		}

		m.mqttOpts = mqttOpts
	}

	return m, nil
}

// Init initializes the mq and init the connection to the server.
func (m *Driver) Init(metadata mq.Metadata) error {
	mqttMeta, err := parseMQTTMetaData(metadata)
	if err != nil {
		return err
	}
	m.metadata = mqttMeta
	client, err := m.connect()
	if err != nil {
		return err
	}

	m.client = client
	return nil

}

func (m *Driver) connect() (mqtt.Client, error) {
	client := mqtt.NewClient(m.mqttOpts)
	token := client.Connect()
	for !token.WaitTimeout(m.mqttOpts.PingTimeout) {
	}
	if err := token.Error(); err != nil {
		return nil, err
	}
	return client, nil
}

func createClientOptions(mqttURL string) (*mqtt.ClientOptions, error) {
	uri, err := url.Parse(mqttURL)
	if err != nil {
		return nil, err
	}
	opts := mqtt.NewClientOptions()
	opts.AddBroker(uri.Scheme + "://" + uri.Host)
	opts.SetUsername(uri.User.Username())
	password, _ := uri.User.Password()
	opts.SetPassword(password)
	return opts, nil
}

// Publish the topic to mqtt pub sub.
func (m *Driver) Publish(topic string, in []byte) error {
	token := m.client.Publish(topic, m.mqttOpts.WillQos, m.mqttOpts.WillRetained, in)
	if !token.WaitTimeout(m.mqttOpts.PingTimeout) || token.Error() != nil {
		return fmt.Errorf("mqtt error from publish: %v", token.Error())
	}
	return nil
}

// Subscribe to the mqtt pub sub topic.
func (m *Driver) Subscribe(topic string, handler func(msg []byte)) (mq.Closer, error) {
	c, err := m.connect()
	if err != nil {
		return nil, err
	}

	token := c.Subscribe(
		topic, m.mqttOpts.WillQos,
		func(client mqtt.Client, msg mqtt.Message) {
			handler(msg.Payload())
		})

	if token.Wait() && token.Error() != nil {
		// log.Errorf("mqtt error from subscribe: %v", token.Error())
		return nil, token.Error()
	}

	return &consumer{client: c}, nil
}

type consumer struct {
	client mqtt.Client
}

func (c *consumer) Close() error {
	c.client.Disconnect(0)
	return nil
}

// Close closes the client
func (m *Driver) Close() error {
	m.client.Disconnect(0)
	return nil
}
