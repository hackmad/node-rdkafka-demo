import yargs from 'yargs'
import { CommonArgs } from './types'
import { StandardProducer } from '../producers/standard-producer'
import { LibrdKafkaError, DeliveryReport } from 'node-rdkafka'
import { v4 as uuidv4 } from 'uuid'
import fs from 'fs'
import { randomItem } from './utils'
import { Logger } from '../logger'

interface ProducerCommandArgs extends CommonArgs {
  'dictionary-path': string
  'stall-probability': number
  'stall-value': string
  topic: string
  interval: number
}

class ProducerCommand {
  producer: StandardProducer
  intervalMs: number
  dictionary: string[]
  stallProbability: number
  stallValue: string

  logger = new Logger()

  constructor(argv: ProducerCommandArgs) {
    this.intervalMs = argv['interval']
    this.stallProbability = argv['stall-probability']
    this.stallValue = argv['stall-value']

    this.dictionary = fs
      .readFileSync(argv['dictionary-path'], 'utf8')
      .split('\n')

    this.producer = new StandardProducer(
      this.logger,
      argv['broker-list'],
      argv['topic'],
      this.onReady,
      this.onFailure,
      this.onDeliveryReport,
    )
  }

  execute = () => this.producer.start()

  onReady = () => setInterval(this.produceMessage, this.intervalMs)

  onFailure = (err: LibrdKafkaError) => {
    this.logger.error('MAIN: producer error', err)
  }

  onDeliveryReport = (err: LibrdKafkaError, report: DeliveryReport) => {
    if (err) {
      this.logger.error('MAIN: delivery report error', err)
    } else {
      this.logger.info('MAIN: delivery report', report)
    }
  }

  produceMessage = async () => {
    try {
      const key = uuidv4()
      const value =
        Math.random() < this.stallProbability
          ? this.stallValue
          : randomItem(this.dictionary)

      this.logger.info('MAIN: producing', { key, value })
      await this.producer.produce(key, value)
    } catch (e) {
      this.logger.error('MAIN: error', e)
    }
  }
}

exports.command = 'producer'

exports.describe = 'run non-avro producer'

exports.builder = (yargs: yargs.Argv) =>
  yargs
    .option('schema-registry-url', {
      description: 'schema registry url',
      alias: 'sr',
      type: 'string',
      default: 'http://localhost:8081',
    })
    .option('topic', {
      description: 'topic',
      alias: 't',
      type: 'string',
      default: 'test',
    })
    .option('dictionary-path', {
      description: 'dictionary containing data to use as values',
      alias: 'dict',
      type: 'string',
      default: '/usr/share/dict/words',
    })
    .option('interval', {
      description: 'interval in milliseconds for producing messages',
      alias: 'int',
      type: 'number',
      default: 1000,
    })
    .option('stall-probability', {
      description: 'probability a message value should be stall-value',
      alias: 'sp',
      type: 'number',
      default: 0.2,
    })
    .option('stall-value', {
      description: 'message value that will generate errors in the consumer',
      alias: 'sv',
      type: 'string',
      default: 'stall',
    })

exports.handler = (argv: ProducerCommandArgs) => {
  const command = new ProducerCommand(argv)
  command.execute()
}
