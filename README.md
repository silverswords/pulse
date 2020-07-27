# whisper
send and receive messages with any protocol like nats etc.

could define the handler to process messages simply.

Could be eventbus with more decorating.

## Overview
![](https://firebasestorage.googleapis.com/v0/b/firescript-577a2.appspot.com/o/imgs%2Fapp%2Fcomputer%2FWOjfpzAWwh.png?alt=media&token=376cb2ea-ab64-4887-9366-c1e23891cdcd)

![](https://firebasestorage.googleapis.com/v0/b/firescript-577a2.appspot.com/o/imgs%2Fapp%2Fcomputer%2FJmsiGCZpUg.png?alt=media&token=fb081e7b-91a2-4c4b-82a9-7232b245a127)

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
* driver no reconnect
