import * as Kafka from 'node-rdkafka'
import { Transform } from 'stream'

const minRetryDelayMs = 1000
const maxRetryDelayMs = 30000
const seekTimeoutMs = 1000

const exponentialBackoff = (minMs: number, maxMs: number, attemptNum: number) =>
  Math.min(Math.pow(2, attemptNum) * minMs, maxMs)

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

const offsetCommitCallback = (err: any, topicPartitions: any) => {
  if (err) {
    console.error(err)
  } else {
    console.log('offset committed', topicPartitions)
  }
}

let retryAttempt = 0

const stream = Kafka.KafkaConsumer.createReadStream(
  {
    'group.id': 'node_rdkafka_demo_sync_consumer',
    'metadata.broker.list': 'kafka:9092',
    'socket.keepalive.enable': true,
    'enable.auto.commit': false,
    offset_commit_cb: offsetCommitCallback,
  },
  {},
  {
    topics: 'test',
    waitInterval: 0,
    objectMode: true,
  },
)
console.log('consumer started\n')

const messageToTopicPartition = (m: Kafka.Message) => {
  return { topic: m.topic, partition: m.partition, offset: m.offset }
}

const commitMessage = (message: Kafka.Message) => {
  console.log('committing', message)
  stream.consumer.commitMessageSync(message)
  retryAttempt = 0
}

const backoff = async (data: any) => {
  console.error('error processing message', data.error)
  console.log('seeking back to current offset', data.topicPartition)

  stream.consumer.seek(data.topicPartition, seekTimeoutMs, (err: any) => {
    if (err) {
      console.error('problem seeking back to current offset', err)
      // TODO: This is bad... should we fail the process?
    }
  })

  retryAttempt++
  const ms = exponentialBackoff(minRetryDelayMs, maxRetryDelayMs, retryAttempt)
  console.log(`backoff retry attempt ${retryAttempt} -> ${ms} milliseconds`)
  await delay(ms)
}

const messageHandler = new Transform({
  readableObjectMode: true,

  writableObjectMode: true,

  async transform(message, _encoding, callback) {
    try {
      console.log(
        `key=${message.key}, value=${message.value}, ` +
          `partition=${message.partition} ` +
          `offset=${message.offset}`,
      )

      // Next stage commit
      this.push({ message: message })

      //await delay(5000)
    } catch (e) {
      // Next stage handle error
      this.push({
        error: e,
        topicPartition: messageToTopicPartition(message),
      })
    }

    callback()
  },
})

const messageCommitter = new Transform({
  writableObjectMode: true,

  async write(data, _encoding, callback) {
    if (data.error) {
      await backoff(data)
    } else {
      commitMessage(data.message)
    }

    callback()
  },
})

stream.pipe(messageHandler).pipe(messageCommitter).pipe(process.stdout)

stream.on('error', (err: any) => {
  console.error('problem with the streaming consumer', err)
  process.exit(1)
})
