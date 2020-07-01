package loopback

import (
	"errors"
	"github.com/silverswords/whisper"
	"github.com/silverswords/whisper/client"
	"log"
	"sync"
)

var LoopbackDriver = &simpleDriver{}
var subscribers = make(map[string][]chan *client.Message)
var rm sync.RWMutex

type simpleDriver struct{}

func (d *simpleDriver) Pub(topic string, msg *client.Message) error {
	rm.RLock()
	if chans, found := subscribers[topic]; found {
		channels := append([]chan *client.Message{}, chans...)
		go func(msg *client.Message, dataChannelSlices []chan *client.Message) {
			for _, ch := range dataChannelSlices {
				if ch == nil {
					continue
				}
				ch <- msg
				log.Println("send ", msg, " to ", ch)
			}

		}(msg, channels)
	}
	rm.RUnlock()
	return nil
}

type unSubscriber struct {
	topic   string
	serial  int
	channel chan *client.Message
}

func (u *unSubscriber) Unsubscribe() error {
	if u == nil || u.channel == nil {
		return errors.New("nil subscriber")
	}
	rm.Lock()
	subscribers[u.topic][u.serial] = nil
	rm.Unlock()
	return nil
}

func (d *simpleDriver) Sub(topic string, handler func(*client.Message)) (whisper.UnSubscriber, error) {
	suber := &unSubscriber{
		topic:   topic,
		channel: make(chan *client.Message, 100),
		serial:  0,
	}

	rm.Lock()
	if prev, found := subscribers[topic]; found {
		suber.serial = len(prev)
		subscribers[topic] = append(prev, suber.channel)
		//log.Println("already have a subscriber")
	} else {
		suber.serial = 0
		subscribers[topic] = append([]chan *client.Message{}, suber.channel)
		//log.Println("new a subscriber")
	}
	rm.Unlock()

	go func() {
		log.Println("start sub : ", topic)
		for suber.channel != nil {
			msg := <-suber.channel
			handler(msg)
			//log.Println("dispatch message: ",msg)
		}
	}()

	return suber, nil
}
