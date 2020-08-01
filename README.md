# whisper
send and receive messages with any protocol like nats etc.

could define the handler to process messages simply.

Could be eventbus with more decorating.

## Usage
```go
	meta := driver.NewMetadata()

	t, err := whisper.NewTopic("eventbus:hello",*meta,whisper.WithPubACK(), whisper.WithCount())
	if err != nil {
		log.Fatal(err)
	}
  
  s, err := whisper.NewSubscription("eventbus:hello", *meta, whisper.WithSubACK())
	if err != nil {
		log.Println(err)
		return
	}
  
  go func() {
		var count int
		for {
			count++
			res := t.Publish(context.Background(), whisper.NewMessage([]byte("hello")))
			go func() {
				if err := res.Get(context.Background()); err != nil {
					log.Println("----------------------", err)
				}
			}()
			//log.Println("send a message", count)
			Sleep(Second)
			if count > 1e2 {
				return
			}
		}
	}()
  
  //ctx, _ := context.WithTimeout(context.Background(),time.Second * 10)
	err = s.Receive(context.Background(), func(ctx context.Context, m *whisper.Message) {
		receiveCount++
		log.Println("receive the message:", m.Id, receiveCount)
	})
```
### 
## feature
- [[uuid]]: 现在使用 nats 的 nuid 进行 uuid 的生成
  - 通过 nuid 生成的 id 的记录保证幂等性
- 通过在同一个 orderingKey 下本身有序的消息添加行为.保证发送和处理和添加行为顺序一样.
    - 如果有序的消息发送失败, 在 Result 会有提示,调用 t.Resume 并重新有序发送错误的信息即可.
- topic 和 subscription 均支持并发处理, 注意如果有 callback 和 middleware 需要注意用户自定义的处理中是否有临界区资源.
- # Architecture
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
 
## Progress
  - [x] completed basic function and framework.
  - [x] find google cloud-go logic with scheduler [[July 27th, 2020]] 
  - [x] 增加 example , new topic and send message
  - EventBus = CloudState(MQ)
  - 增加本地 eventbus 实现
  - 边车功能 Add Endpoint to receive eventSource  GRPC and http
  - 持久订阅功能log 用于恢复 subscribe 的 event = Kafka 
  
## Reference
        - CloudEvent: a CNCF project
        - Cloud State: sidecar project to move state out of application
        - GoogleCloud Pub sub : use to get a new driver
        - Dapr: sidecar Synthesizer
