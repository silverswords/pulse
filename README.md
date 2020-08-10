# pulse
an eventbus made on portable MQ.

an eventsource pattern simple to communicate like async discrete real world.

## Usage
### New Topic & Subscription
```go
meta := mq.NewMetadata()
meta.Properties[nats.URL] = nats.DefaultURL
meta.Properties["DriverName"] = "nats"

t, err := topic.NewTopic("hello", *meta, topic.WithPubACK(), topic.WithCount())
if err != nil {
    log.Println(err)
    return
}

s, err := subscription.NewSubscription("hello", *meta, subscription.WithSubACK())
if err != nil {
    log.Println(err)
    return
}
```

### Publish & Receive
Publish
```go
res := t.Publish(context.Background(), message.NewMessage([]byte("hello")))
go func() {
    if err := res.Get(context.Background()); err != nil {
        log.Println("----------------------", err)
    }
}()
```
Receive
```go
err = s.Receive(context.Background(), func(ctx context.Context, m *message.Message) {
    log.Println("receive the message:", m.Id)
})
```


## **feature**

   - 幂等性: 简单记录在每个 Subscription 的 map 中, 避免单消费者重复处理. nats 可以提供 queueSubscribe

   - 有序性: message 使用 OrderingKey 保证接发有序. 如果出错会暂停某一个 OrderingKey 的发送, 空 key 不暂停

   - 并发处理: topic 和 Subscription 均使用并发处理. 注意中间件是否有临界资源

   - 可靠性: 独立实现了 ack, 保证消息至少一次的投递.
    
# Architecture
  - ![](https://firebasestorage.googleapis.com/v0/b/firescript-577a2.appspot.com/o/imgs%2Fapp%2Fcomputer%2FWOjfpzAWwh.png?alt=media&token=376cb2ea-ab64-4887-9366-c1e23891cdcd)
   
  - ![](https://firebasestorage.googleapis.com/v0/b/firescript-577a2.appspot.com/o/imgs%2Fapp%2Fcomputer%2FiPkp26NkMs.png?alt=media&token=432e16bb-ea5e-4faf-96ae-466924a3f932)

## Concepts
### Driver
Driver is the realization of various protocol like Nats, http etc.

### Message Format
Message is the object transport from the whisper endpoint. It's format as CloudEvents.
                
                {
                  "specversion": "1.x-wip",
                  "type": "coolevent",
                  "id": "xxxx-xxxx-xxxx",
                  "source": "bigco.com",
                  "data": { ... }
                }
 
## **Process**
- [x] 增加本地 eventbus 实现
- [x] EventBus = (MQ)
- [x] copy google cloud-go logic to my topic logic with scheduler [[July 27th, 2020]] 
- [x] 添加 logger 
- [x] 增加 example , new topic and send messagehttps://sourcegraph.com/github.com/GoogleCloudPlatform/golang-samples@master/-/blob/pubsub/subscriptions/pull_concurrency.go#L27
 - 边车功能 Add Endpoint to receive eventSource  GRPC and http
 - 持久订阅功能log 用于恢复 subscribe 的 event = Kafka 
  
## Reference

    - nuid: 现在使用 nats 的 nuid 进行 uuid的生成

    - CloudEvent: a CNCF project

    - Cloud State: sidecar project to move state out of application

    - GoogleCloud Pub sub : use to get a new driver

    - Dapr: sidecar Synthesizer

    - Saga: whisper usage.

    - Kong: siprit on extension.

    - Kafka: log
    
    - axon: event source DDD CQRS

