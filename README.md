# node-rdkafka-demo

## Building

```
npm run build
```

## Consumer without Avro

### Create a topic

Create a topic with 3 partitions and no replication (for local environments):

```
kafka-topics --bootstrap-server localhost:9092 --create --topic test --partitions 3 --replication-factor 1
```

### Run the test consumer

Run the consumer with default `localhost:9092`

```
npm run consumer
```

Or pass in the broker list.

```
npm run consumer -- -b "broker1:9092,broker2:9092,broker3:9092"
```

Produce a message with key and value:

```
kafka-console-producer --broker-list localhost:9092 --topic test --property 'parse.key=true' --property 'key.separator=|'
3c42b2fb-722a-409f-a076-ee78038ea773|hello
8a3be645-3479-4a71-9264-5e1fa11e1302|world
8a3be645-3479-4a71-9264-5e1fa11e1302|now
```

To test the stall logic pass message with the value `stall`:

```
8a3be645-3479-4a71-9264-5e1fa11e1302|stall
```

You should see messages get resent from that topic onwards and at every time it should delay with increasing time up to the maximum. The consumer should let messages through after specified number of times.

Note that keyed messages are partitioned. So messages landing in other partitions will go through.
