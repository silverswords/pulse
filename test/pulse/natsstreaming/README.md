Refer from Dapr setup components docs.

## Component format

```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: natsstreaming-pubsub
  namespace: default
spec:
  type: pubsub.natsstreaming
  version: v1
  metadata:
  - name: natsURL
    value: "nats://localhost:4222"
  - name: natsStreamingClusterID
    value: "clusterId"
    # below are subscription configuration.
  - name: subscriptionType
    value: <REPLACE-WITH-SUBSCRIPTION-TYPE> # Required. Allowed values: topic, queue.
  - name: ackWaitTime
    value: "" # Optional. 
  - name: maxInFlight
    value: "" # Optional.
  - name: durableSubscriptionName
    value: "" # Optional.
  # following subscription options - only one can be used
  - name: deliverNew
    value: <bool>
  - name: startAtSequence
    value: 1
  - name: startWithLastReceived
    value: false
  - name: deliverAll
    value: false
  - name: startAtTimeDelta
    value: ""
  - name: startAtTime
    value: ""
  - name: startAtTimeFormat
    value: ""
```

## Spec metadata fields

| Field              | Required | Details | Example |
|--------------------|:--------:|---------|---------|
| natsURL            | Y  | NATS server address URL   | "`nats://localhost:4222`"|
| natsStreamingClusterID  | Y  | NATS cluster ID   |`"clusterId"`|
| subscriptionType   | Y | Subscription type. Allowed values `"topic"`, `"queue"` | `"topic"` |
| ackWaitTime        | N | See [here](https://docs.nats.io/developing-with-nats-streaming/acks#acknowledgements) | `"300ms"`| 
| maxInFlight        | N | See [here](https://docs.nats.io/developing-with-nats-streaming/acks#acknowledgements) | `"25"` |
| durableSubscriptionName | N | [Durable subscriptions](https://docs.nats.io/developing-with-nats-streaming/durables) identification name. | `"my-durable"`|
| deliverNew         | N | Subscription Options. Only one can be used. Deliver new messages only  | `"true"`, `"false"` | 
| startAtSequence    | N | Subscription Options. Only one can be used. Sets the desired start sequence position and state  | `"100000"`, `"230420"` | 
| startWithLastReceived | N | Subscription Options. Only one can be used. Sets the start position to last received. | `"true"`, `"false"` |
| deliverAll         | N | Subscription Options. Only one can be used. Deliver all available messages  | `"true"`, `"false"` | 
| startAtTimeDelta   | N | Subscription Options. Only one can be used. Sets the desired start time position and state using the delta  | `"10m"`, `"23s"` | 
| startAtTime        | N | Subscription Options. Only one can be used. Sets the desired start time position and state  | `"Feb 3, 2013 at 7:54pm (PST)"` | 
| startAtTimeDelta   | N | Must be used with `startAtTime`. Sets the format for the time  | `"Jan 2, 2006 at 3:04pm (MST)"` | 

## Create a NATS server
```bash
docker run -d --name nats-streaming -p 4222:4222 -p 8222:8222 nats-streaming
```
