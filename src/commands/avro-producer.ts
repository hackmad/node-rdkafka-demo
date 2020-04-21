import yargs from 'yargs'
import { CommonArgs } from './types'
import { AvroProducer } from '../producers/avro-producer'
import { LibrdKafkaError, DeliveryReport } from 'node-rdkafka'
import { v4 as uuidv4 } from 'uuid'
import fs from 'fs'
import { randomItem, randomItems } from './utils'

interface AvroProducerCommandArgs extends CommonArgs {
  'dictionary-path': string
  'schema-registry-url': string
  topic: string
  interval: number
}

class AvroProducerCommand {
  producer: AvroProducer
  dictionary: string[]
  intervalMs: number

  constructor(argv: AvroProducerCommandArgs) {
    this.intervalMs = argv['interval']

    this.dictionary = fs
      .readFileSync(argv['dictionary-path'], 'utf8')
      .split('\n')

    this.producer = new AvroProducer(
      argv['broker-list'],
      argv['schema-registry-url'],
      argv['topic'],
      this.onReady,
      this.onFailure,
      this.onDeliveryReport,
      `${argv['topic']}-value`,
      `${argv['topic']}-key`,
    )
  }

  execute = () => this.producer.start()

  onReady = () => setInterval(this.produceMessage, this.intervalMs)

  onFailure = (err: LibrdKafkaError) => {
    console.error('MAIN: producer error', err)
  }

  onDeliveryReport = (err: LibrdKafkaError, report: DeliveryReport) => {
    if (err) {
      console.error('MAIN: delivery report error', err)
    } else {
      console.info('MAIN: delivery report', report)
    }
  }

  randomEmail = () => {
    const email1 = randomItem(this.dictionary)
    const email2 = randomItem(this.dictionary)
    const email3 = randomItem(['com', 'io', 'net', 'edu', 'gov', 'ca'])
    return `${email1}@${email2}.${email3}`
  }

  randomWords = () => randomItems(this.dictionary, 10).join(' ')

  produceMessage = async () => {
    try {
      const key = uuidv4()
      const value = {
        email_address: this.randomEmail(),
        message: this.randomWords(),
      }

      console.info('MAIN: producing ', key, value)
      await this.producer.produce(value, key)
    } catch (e) {
      console.error('MAIN: error', e)
    }
  }
}

exports.command = 'avro-producer'

exports.describe = 'run avro producer'

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
      default: 'notifications',
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

exports.handler = (argv: AvroProducerCommandArgs) => {
  const command = new AvroProducerCommand(argv)
  command.execute()
}
