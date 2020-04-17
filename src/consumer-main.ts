import { BackPressuredConsumer } from './consumers/back-pressured-consumer'
import { Message } from 'node-rdkafka'

// This counter is used to reset sucess/failure status of the handler
// every few times to test how the consumer reacts stalls.
let i = 0
const messageHandler = (message: Message) => {
  console.info(
    'MAIN: ',
    message.key?.toString(),
    message.value.toString(),
    message.partition,
    message.offset,
  )

  console.debug('MAIN: i before =', i)
  i = i + 1
  let success = i > 5 || message.value.toString() !== 'goo'

  if (success) {
    i = 0
  }
  console.debug('MAIN: i after =', i)

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
