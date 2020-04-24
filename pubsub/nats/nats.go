package nats

import (
	"fmt"
	nats "github.com/nats-io/nats.go"
	"github.com/silverswords/whisper/pubsub"
	"log"
	"time"
)

const DefaultURL =  "nats://39.105.141.168:4222"

type NatsDriver struct {}

var nc,_ = nats.Connect(DefaultURL, SetupConnOptions([]nats.Option{})...)

func init(){
	pubsub.Register("nats", NatsDriver{})
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

func (d NatsDriver) Dial(target string, options interface{}) (pubsub.Conn, error) {
	var (
		err error
		conn *nats.Conn
	)

	// if not set, use default target and options
	if target == "" {
		target = DefaultURL
	}
	options, ok := options.([]nats.Option)
	if !ok {
		conn, err = nats.Connect(target, SetupConnOptions([]nats.Option{})...)
	}else {
		conn , err = nats.Connect(target, options.([]nats.Option)...)
	}
	if err != nil {
		return nil, err
	}
	return Conn{conn},err
}

type Conn struct {
	 *nats.Conn
}

// Send send msg to nc
func (c Conn) Pub(topic string, msg []byte) error {
	err:= c.Publish(topic,msg)
	if err != nil {
		fmt.Println(err)
		return err
	}
	nc.Flush()
	return nil
}

type Subscription struct {
	conn *nats.Subscription
	ch chan *nats.Msg
}

// Receive is a blocked method
func (s Subscription) Receive() []byte {
	msg := <- s.ch
	return msg.Data
}

func (c Conn) Sub(subject string) (pubsub.Subscription,error) {
	//subs, err := c.Subscribe(subject,func(m *nats.Msg) {
	//	fmt.Printf("Received a message: %s\n", string(m.Data))
	//})
	// Channel Subscriber
	ch := make(chan *nats.Msg, 64)
	sub, err := c.ChanSubscribe(subject, ch)
	if err != nil { fmt.Println(err);return nil,err}

	return Subscription{sub,ch},nil
}