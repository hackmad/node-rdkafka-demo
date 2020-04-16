```
kafka-topics --bootstrap-server localhost:9092 --create --topic test --partitions 3 --replication-factor 1
```

```
kafka-console-producer --broker-list localhost:9092 --topic test --property 'parse.key=true' --property 'key.separator=|'
>3c42b2fb-722a-409f-a076-ee78038ea773|hello
>8a3be645-3479-4a71-9264-5e1fa11e1302|world
>8a3be645-3479-4a71-9264-5e1fa11e1302|now
```

```
npm run build
```

```
npm run sync-consumer
```

```
npm run stream-consumer
```

```
npm run back-pressured-consumer
```
