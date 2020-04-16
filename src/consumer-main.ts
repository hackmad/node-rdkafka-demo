import { BackPressuredConsumer } from './consumers/back-pressured-consumer'
import { Message } from 'node-rdkafka'

// This counter is used to reset sucess/failure status of the handler
// every few times to test how the consumer reacts stalls.
let i = 0
const messageHandler = (message: Message) => {
  console.info(
    'main: ',
    message.key,
    message.value,
    message.partition,
    message.offset,
  )

  i = i + 1
  let success = i > 5 || message.value.toString() !== 'goo'

  if (success) {
    i = 0
  }

  return success
}

const failureHandler = (_err: any) => process.exit(1)

const [brokerList] = process.argv.slice(2)

const group = 'node_rdkafka_demo_stream_consumer_bp'
const topic = 'test'

// Start a new back pressured consumer
const consumer = new BackPressuredConsumer(
  brokerList || 'localhost:29092',
  topic,
  group,
  messageHandler,
  failureHandler,
)
consumer.start()
