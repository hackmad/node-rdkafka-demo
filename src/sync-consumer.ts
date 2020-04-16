import * as _ from 'lodash'
import {
  KafkaConsumer,
  LibrdKafkaError,
  Message,
  Assignment,
  CODES,
} from 'node-rdkafka'

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

const exponentialBackoff = (minMs: number, maxMs: number, attemptNum: number) =>
  Math.min(Math.pow(2, attemptNum) * minMs, maxMs)

const messageToTopicPartition = (m: Message) => {
  return { topic: m.topic, partition: m.partition, offset: m.offset }
}

class Consumer {
  readonly numMessages: number = 1
  readonly minRetryDelayMs = 1000
  readonly maxRetryDelayMs = 30000
  readonly seekTimeoutMs = 1000

  consumer: KafkaConsumer

  topic: string

  retryAttempt: number = 0

  constructor(
    host: string,
    port: number,
    topic: string,
    consumerGroupId: string,
  ) {
    this.topic = topic

    const consumerConfig = {
      'group.id': consumerGroupId,
      'metadata.broker.list': `${host}:${port}`,
      'enable.auto.commit': false,
      'auto.offset.reset': 'earliest',
      offset_commit_cb: this.onOffsetCommit,
      rebalance_cb: this.onRebalance,
    }

    this.consumer = new KafkaConsumer(consumerConfig, {})
  }

  onOffsetCommit = (err: any, topicPartitions: any) => {
    if (err) {
      console.error(err)
    } else {
      console.log('offset committed', topicPartitions)
    }
  }

  onRebalance = (err: LibrdKafkaError, assignment: Assignment) => {
    try {
      if (err.code === CODES.ERRORS.ERR__ASSIGN_PARTITIONS) {
        console.log('\nassigning', assignment, '\n')
        this.consumer.assign(assignment)
      } else if (err.code == CODES.ERRORS.ERR__REVOKE_PARTITIONS) {
        console.log('\nrevoking partition assignments\n')
        this.consumer.unassign()
      } else {
        console.error(err)
      }
    } catch (e) {
      console.error(e)
    }
  }

  start = () => {
    this.consumer.connect()

    this.consumer
      .on('ready', () => {
        this.consumer.subscribe([this.topic])

        console.log('consumer started\n')

        // Initiate first call to consumer. Subsequent ones will take place
        // as messages are handled.
        this.consumer.consume(this.numMessages, this.handleMessages)
      })
      .on('event.error', (err: any) => {
        console.error(err)
        process.exit(1)
      })
  }

  seek = (m: Message): Promise<void> =>
    new Promise((resolve: any, reject: any) => {
      this.consumer.seek(
        messageToTopicPartition(m),
        this.seekTimeoutMs,
        (err: LibrdKafkaError) => {
          if (err) {
            reject(err)
          } else {
            const delayMs = exponentialBackoff(
              this.minRetryDelayMs,
              this.maxRetryDelayMs,
              this.retryAttempt,
            )

            console.log(`waiting for ${delayMs} ms before retry`)
            delay(delayMs).then(() => {
              this.retryAttempt++
              resolve()
            })
          }
        },
      )
    })

  handleMessages = (err: LibrdKafkaError, messages: Message[]) => {
    if (err) {
      console.error(err)
      return
    }

    if (messages.length == 0) {
      this.consumer.consume(this.numMessages, this.handleMessages)
      return
    }

    console.log(`number of messages=${messages.length}`)

    let handleErr: boolean = false
    let m: Message

    for (let i = 0; i < messages.length; i++) {
      m = messages[i]

      try {
        console.log(
          '    ' +
            `key=${m.key}, value=${m.value}, ` +
            `partition=${m.partition} ` +
            `offset=${m.offset}`,
        )

        this.consumer.commitMessageSync(m)
        this.retryAttempt = 0
      } catch (e) {
        console.error(e)
        handleErr = true
        break
      }
    }

    if (handleErr) {
      this.seek(m)
        .then(() => {
          this.consumer.consume(this.numMessages, this.handleMessages)
        })
        .catch((e2) => {
          console.error('problem seeking', e2)
          process.exit(1)
        })
    } else {
      this.consumer.consume(this.numMessages, this.handleMessages)
    }
  }
}

const consumer = new Consumer(
  'kafka',
  9092,
  'test',
  'node_rdkafka_demo_stream_consumer',
)
consumer.start()
