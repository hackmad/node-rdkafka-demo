import yargs from 'yargs'
import { CommonArgs } from './types'
import { Message } from 'node-rdkafka'

import { NonFlowingAvroConsumer } from '../consumers/non-flowing-avro-consumer'
import { messageToString } from '../consumers/utils'

interface AvroConsumerCommandArgs extends CommonArgs {
  topic: string
  'schema-registry-url': string
}

class AvroConsumerCommand {
  consumer: NonFlowingAvroConsumer

  constructor(argv: AvroConsumerCommandArgs) {
    this.consumer = new NonFlowingAvroConsumer(
      argv['broker-list'],
      argv['schema-registry-url'],
      argv['topic'],
      `${argv.topic}_avro_consumer`,
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
    return Promise.resolve(true)
  }

  failureHandler = (err: any) => console.error(`MAIN: error ${err}`)

  commitNotificationHandler = (offsets: any) =>
    console.info('MAIN: committed offsets', offsets)
}

exports.command = 'avro-consumer'

exports.describe = 'run avro consumer'

exports.builder = (yargs: yargs.Argv) =>
  yargs
    .option('topic', {
      description: 'topic',
      alias: 't',
      type: 'string',
      default: 'notifications',
    })
    .option('schema-registry-url', {
      description: 'schema registry url',
      alias: 'sr',
      type: 'string',
      default: 'http://localhost:8081',
    })

exports.handler = (argv: AvroConsumerCommandArgs) => {
  const consumerCommand = new AvroConsumerCommand(argv)
  consumerCommand.execute()
}
