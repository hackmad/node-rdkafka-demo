Build the source:

```
npm run build
```

Run the consumer with default `localhost:9092`

```
npm run consumer
```

Or pass in the broker list.

```
npm run consumer "broker1:9092,broker2:9092,broker3:9092"
```

Create a topic with 3 partitions and no replication (for local environments):

```
kafka-topics --bootstrap-server localhost:9092 --create --topic test --partitions 3 --replication-factor 1
```

Produce a message with key and value:

```
kafka-console-producer --broker-list localhost:9092 --topic test --property 'parse.key=true' --property 'key.separator=|'
>3c42b2fb-722a-409f-a076-ee78038ea773|hello
>8a3be645-3479-4a71-9264-5e1fa11e1302|world
>8a3be645-3479-4a71-9264-5e1fa11e1302|now
```

