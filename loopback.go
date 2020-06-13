package whisper

import (
"errors"
	"log"
	"sync"
)

var LoopbackDriver = &simpleDriver{}
var subscribers = make(map[string][]chan *Message)
var rm sync.RWMutex

type simpleDriver struct{}

func (d *simpleDriver) Pub(topic string, msg *Message) error {
	rm.RLock()
	if chans, found := subscribers[topic]; found {
		channels := append([]chan *Message{}, chans...)
		go func(msg *Message, dataChannelSlices []chan *Message) {
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
	topic string
	serial int
	channel chan *Message
}

func (u *unSubscriber) Unsubscribe() error {
	if u == nil || u.channel == nil {return errors.New("nil subscriber")}
	rm.Lock()
	subscribers[u.topic][u.serial] = nil
	rm.Unlock()
	return nil
}

func (d *simpleDriver) Sub(topic string, handler func(*Message) ) (UnSubscriber,error) {
	suber := &unSubscriber{
		topic:topic,
		channel: make(chan *Message,100),
		serial: 0,
	}

	rm.Lock()
	if prev, found := subscribers[topic]; found {
		suber.serial = len(prev)
		subscribers[topic] = append(prev, suber.channel)
		log.Println("hehe")
	} else {
		suber.serial = 0
		subscribers[topic] = append([]chan *Message{}, suber.channel)
		log.Println("new a subscriber")
	}
	rm.Unlock()

	go func (){
		log.Println("start sub goroutine")
		for suber.channel != nil {
			msg := <- suber.channel
			handler(msg)
			log.Println("dispatch message: ",msg)
		}
	}()

	return suber, nil
}
