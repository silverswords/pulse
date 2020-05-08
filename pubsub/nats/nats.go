package nats

import (
	"fmt"
	nats "github.com/nats-io/nats.go"
	"github.com/silverswords/whisper"
	"log"
	"time"
)

const DefaultURL =  "nats://39.105.141.168:4222"

type NatsDriver struct {}

var nc,_ = nats.Connect(DefaultURL, SetupConnOptions([]nats.Option{})...)

func init(){

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

func (d NatsDriver) Dial(target string, options interface{}) (whisper.Conn, error) {
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
func (c Conn) Pub(topic string, msg whisper.Message) error {
	raw, err :=whisper.Encode(msg)
	if err != nil { return err}
	err = c.Publish(topic,raw)
	if err != nil {
		fmt.Println(err)
		return err
	}
	nc.Flush()
	return nil
}

type Suber struct {
	conn *nats.Subscription
}

// Receive is a blocked method
func (s Suber) Receive(timeout time.Duration) (*whisper.Message,error) {
	m,err := s.conn.NextMsg(timeout)
	if err != nil {
		return nil, err
	}

	msg := whisper.Message{}
	if err := whisper.Decode(m.Data,&msg); err != nil {return nil, err}
	return &msg, nil
}

func (c Conn) Sub(subject string) (whisper.Suber,error) {
	subs, err := c.SubscribeSync(subject)
	if err != nil { fmt.Println(err);return nil,err}

	return Suber{subs},nil
}