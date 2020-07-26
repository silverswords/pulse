# whisper
send and receive messages with any protocol like nats etc.

could define the handler to process messages simply.

Could be eventbus with more decorating.

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
completed basic function and framework.

### Not Perfect
* no logger
* some error not detect and handle
