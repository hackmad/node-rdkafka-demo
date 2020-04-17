import { BackPressuredConsumer } from './consumers/back-pressured-consumer'
import { Message } from 'node-rdkafka'
import { messageToString } from './consumers/utils'

// This counter is used to reset sucess/failure status of the handler
// every few times to test how the consumer reacts stalls.
let counts = new Map<number, number>()
const messageHandler = (message: Message) => {
  console.info('MAIN: ', messageToString(message))

  let c = counts[message.partition] || 0
  c++
  let success = c > 5 || message.value.toString() !== 'goo'

  if (success) {
    c = 0
  }
  counts[message.partition] = c

  console.debug(`MAIN: p = ${message.partition}, count = ${c}`)

  return success
}

const failureHandler = (_err: any) => process.exit(1)

const commitNotificationHandler = (offsets: any) => console.info(offsets)

const [brokerList] = process.argv.slice(2)

const group = 'node_rdkafka_demo_stream_consumer_bp'
const topic = 'test'

// Start a new back pressured consumer
const consumer = new BackPressuredConsumer(
  brokerList || 'localhost:9092',
  topic,
  group,
  messageHandler,
  failureHandler,
  commitNotificationHandler,
)
consumer.start()
