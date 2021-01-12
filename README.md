# pulse
[![Go Report Card](https://goreportcard.com/badge/github.com/silverswords/pulse)](https://goreportcard.com/report/github.com/silverswords/pulse)
[![visitor badges](https://visitor-badge.laobi.icu/badge?page_id=silverswords.pulse)](https://github.com/abserari)
[![Discord Chat](https://img.shields.io/discord/771388143148073040.svg)](https://discord.gg/rRwryXfj3u)  
an eventbus made on portable MQ.

For Version 2.0. Let's refer https://cloud.google.com/pubsub/docs/choosing-pubsub-or-cloud-tasks

## Usage
Check the example/pulse and find an example for supported MQ. 
Note: You need run your MQ first and get its address.

### Use MQ as broker
example for nats.
```go
meta := mq.NewMetadata()
meta.Properties[nats.URL] = nats.DefaultURL
meta.Properties["DriverName"] = "nats"
```

### Publisher
Publisher is asynchronously and could get result about the success or failure to send the event.
```go
t, err := topic.NewTopic("hello", *meta, topic.WithRequiredACK(), topic.WithOrdered())
if err != nil {
    log.Println(err)
    return
}

res := t.Publish(context.Background(), message.NewMessage([]byte("hello")))
go func() {
    if err := res.Get(context.Background()); err != nil {
        log.Println("----------------------", err)
    }
}()
```

### Subscirbe
Receive is a synchronous function and blocks until have an err set by like ctx.Done() or other error.
```go
s, err := subscription.NewSubscription("hello", *meta, subscription.WithCount(), subscription.WithAutoACK())
if err != nil {
    log.Println(err)
    return
}

err = s.Receive(context.Background(), func(ctx context.Context, m *message.CloudEventsEnvelope) {
    log.Println("receive the message:", m.Id)
})
```


## **feature**

- Idempotence: Simply record in the map of each Subscription to avoid repeated processing by a single consumer. Nats can provide queueSubscribe

- Orderliness: Messages use OrderingKey to ensure orderly delivery. If an error occurs, the sending of a certain OrderingKey will be suspended, and the empty key will not be suspended

- Concurrent processing: Both topic and Subscription use concurrent processing. Pay attention to whether the middleware has critical resources

- Reliability: ack is implemented independently to ensure that the message is delivered at least once.

# Architecture
  - ![](https://firebasestorage.googleapis.com/v0/b/firescript-577a2.appspot.com/o/imgs%2Fapp%2Fcomputer%2FWOjfpzAWwh.png?alt=media&token=376cb2ea-ab64-4887-9366-c1e23891cdcd)
   
  - ![](https://firebasestorage.googleapis.com/v0/b/firescript-577a2.appspot.com/o/imgs%2Fapp%2Fcomputer%2FiPkp26NkMs.png?alt=media&token=432e16bb-ea5e-4faf-96ae-466924a3f932)

## Concepts
### Driver
Driver is the realization of various protocol like Nats, http etc.

### Message Format
Message is the object transport from the pulse endpoint. It's format as CloudEvents.
                
                {
                  "specversion": "1.x-wip",
                  "type": "coolevent",
                  "id": "xxxx-xxxx-xxxx",
                  "source": "bigco.com",
                  "data": { ... }
                }

  
## Reference

### Hall of fame
[![](https://sourcerer.io/fame/abserari/silverswords/pulse/images/0)](https://sourcerer.io/fame/abserari/silverswords/pulse/links/0)[![](https://sourcerer.io/fame/abserari/silverswords/pulse/images/1)](https://sourcerer.io/fame/abserari/silverswords/pulse/links/1)[![](https://sourcerer.io/fame/abserari/silverswords/pulse/images/2)](https://sourcerer.io/fame/abserari/silverswords/pulse/links/2)[![](https://sourcerer.io/fame/abserari/silverswords/pulse/images/3)](https://sourcerer.io/fame/abserari/silverswords/pulse/links/3)[![](https://sourcerer.io/fame/abserari/silverswords/pulse/images/4)](https://sourcerer.io/fame/abserari/silverswords/pulse/links/4)[![](https://sourcerer.io/fame/abserari/silverswords/pulse/images/5)](https://sourcerer.io/fame/abserari/silverswords/pulse/links/5)[![](https://sourcerer.io/fame/abserari/silverswords/pulse/images/6)](https://sourcerer.io/fame/abserari/silverswords/pulse/links/6)[![](https://sourcerer.io/fame/abserari/silverswords/pulse/images/7)](https://sourcerer.io/fame/abserari/silverswords/pulse/links/7)

### Repos

    - nuid: now use nats's package nuid to generated uuid

    - CloudEvent: a CNCF project

    - Cloud State: sidecar project to move state out of application

    - GoogleCloud Pub sub : use to get a new driver

    - Dapr: sidecar Synthesizer

    - Saga: pulse usage.

    - Kong: siprit on extension.

    - Kafka: log
    
    - axon: event source DDD CQRS
    
    - webhook: to establish a webhook to receive the response asynchronously
    
 

