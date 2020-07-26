package nats_deprecated

import "github.com/nats-io/nats.go"

// Subscriber use to different subscribe like queue subscribe
// queue Subscribe is Nats' featrue that every subscriber in queue get message in order.
// it means one message send to a queue. there is one subscriber in queue could get it.
type Subscriber interface {
	Subscribe(conn *nats.Conn, subject string, cb nats.MsgHandler) (*nats.Subscription, error)
}

// RegularSubscriber creates regular subscriptions
type RegularSubscriber struct {
}

// Subscribe implements Subscriber.Subscribe
func (s *RegularSubscriber) Subscribe(conn *nats.Conn, subject string, cb nats.MsgHandler) (*nats.Subscription, error) {
	return conn.Subscribe(subject, cb)
}

var _ Subscriber = (*RegularSubscriber)(nil)

// QueueSubscriber creates queue subscriptions
type QueueSubscriber struct {
	Queue string
}

// Subscribe implements Subscriber.Subscribe
func (s *QueueSubscriber) Subscribe(conn *nats.Conn, subject string, cb nats.MsgHandler) (*nats.Subscription, error) {
	return conn.QueueSubscribe(subject, s.Queue, cb)
}

var _ Subscriber = (*QueueSubscriber)(nil)
