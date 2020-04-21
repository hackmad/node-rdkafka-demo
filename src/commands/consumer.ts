import yargs from 'yargs'
import { CommonArgs } from './types'
import { Message } from 'node-rdkafka'

import { NonFlowingConsumer } from '../consumers/non-flowing-consumer'
import { messageToString } from '../consumers/utils'

interface ConsumerCommandArgs extends CommonArgs {
  'stall-value': string
  'stall-count': number
  topic: string
}

class ConsumerCommand {
  // This counter is used to reset sucess/failure status of the handler
  // every few times to test how the consumer reacts stalls.
  counts = new Map<number, number>()
  stallCount: number
  stallValue: string
  consumer: NonFlowingConsumer

  constructor(argv: ConsumerCommandArgs) {
    this.stallCount = argv['stall-count']
    this.stallValue = argv['stall-value']

    this.consumer = new NonFlowingConsumer(
      argv['broker-list'],
      argv['topic'],
      `${argv.topic}_default_consumer`,
      this.messageHandler,
      this.failureHandler,
      this.commitNotificationHandler,
      argv['seek-timeout-ms'],
      argv['min-retry-delay'],
      argv['max-retry-delay'],
      argv['consume-timeout'],
      argv['commit-interval'],
    )
  }

  execute = () => this.consumer.start()

  messageHandler = async (message: Message) => {
    console.info('MAIN: ', messageToString(message))
    const value = message.value.toString()
    const partition = message.partition

    const count = (this.counts[partition] || 0) + 1
    const processed = count > this.stallCount || value !== this.stallValue

    this.counts[partition] = processed ? 0 : count

    console.debug(
      `MAIN: partition = ${partition}, ` +
        `stall count = ${this.counts[partition]}, ` +
        `processed = ${processed}`,
    )

    return Promise.resolve(processed)
  }

  failureHandler = (err: any) => console.error(`MAIN: error ${err}`)

  commitNotificationHandler = (offsets: any) =>
    console.info('MAIN: committed offsets', offsets)
}

exports.command = 'consumer'

exports.describe = 'run non-avro consumer'

exports.builder = (yargs: yargs.Argv) =>
  yargs
    .option('topic', {
      description: 'topic',
      alias: 't',
      type: 'string',
      default: 'test',
    })
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

exports.handler = (argv: ConsumerCommandArgs) => {
  const consumerCommand = new ConsumerCommand(argv)
  consumerCommand.execute()
}
