import yargs from 'yargs'
import { BackPressuredConsumer } from './consumers/back-pressured-consumer'
import { Message } from 'node-rdkafka'
import { messageToString } from './consumers/utils'

interface CommonArgs {
  'broker-list': string
  topic: string
  'seek-timeout': number
  'max-queue-size': number
  'min-retry-delay': number
  'max-retry-delay': number
  _: any
  $0: string
}

interface DefaultCommmandArgs {
  'stall-value': string
  'stall-count': number
}

const argv: CommonArgs = yargs(process.argv.slice(2))
  .command(
    '$0',
    'run default consumer (no avro)',
    (yargs) => <DefaultCommmandArgs>yargs
        .option('stall-value', {
          description: 'message value that will generate errors',
          alias: 'sv',
          type: 'string',
          default: 'stall',
        })
        .option('stall-count', {
          description: 'number of times to stall',
          alias: 'sc',
          type: 'number',
          default: 5,
        })
        .help()
        .alias('help', 'h').argv,
  )
  .option('broker-list', {
    description: 'broker list',
    alias: 'b',
    type: 'string',
    default: 'localhost:9092',
  })
  .option('topic', {
    description: 'topic',
    alias: 't',
    type: 'string',
    default: 'test',
  })
  .option('seek-timeout', {
    description: 'timeout to seek current offset on stall in milliseconds',
    alias: 'sk',
    type: 'number',
    default: 1000,
  })
  .option('max-queue-size', {
    description: 'max number of messages per partition to buffer internally',
    alias: 'q',
    type: 'number',
    default: 100,
  })
  .option('min-retry-delay', {
    description:
      'min time in milliseconds to delay before retrying consumption ' +
      'when a message fails to process',
    alias: 'mnr',
    type: 'number',
    default: 1000,
  })
  .option('max-retry-delay', {
    description:
      'max time in milliseconds to delay before retrying consumption ' +
      'when a message fails to process',
    alias: 'mxr',
    type: 'number',
    default: 16000,
  }).argv

// This counter is used to reset sucess/failure status of the handler
// every few times to test how the consumer reacts stalls.
let counts = new Map<number, number>()
const messageHandler = (message: Message) => {
  console.info('MAIN: ', messageToString(message))
  const value = message.value.toString()
  const partition = message.partition

  const count = (counts[partition] || 0) + 1
  const processed = count > argv['stall-count'] || value !== argv['stall-value']
  counts[partition] = processed ? 0 : count

  console.debug(
    `MAIN: partition = ${partition}, ` +
      `stall count = ${counts[partition]}, processed = ${processed}`,
  )

  return processed
}

const failureHandler = (_err: any) => {}

const commitNotificationHandler = (offsets: any) => console.info(offsets)

const group = `${argv.topic}_default_consumer`

// Start a new back pressured consumer
const consumer = new BackPressuredConsumer(
  argv['broker-list'],
  argv['topic'],
  group,
  messageHandler,
  failureHandler,
  commitNotificationHandler,
  argv['max-queue-size'],
  argv['seek-timeout-ms'],
  argv['min-retry-delay'],
  argv['max-retry-delay'],
)
consumer.start()
