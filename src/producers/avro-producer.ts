import { Producer } from 'node-rdkafka'
import { CachedSchemaRegistry } from '../schema-registry'
import { encode } from '../avro'
import {
  FailureHandler,
  DeliveryReportHandler,
  ProducerReadyHandler,
  DefaultPollIntervalMs,
} from './types'

export class AvroProducer {
  producer: Producer
  topic: string
  registry: CachedSchemaRegistry
  valueSchemaSubject: string
  keySchemaSubject?: string

  constructor(
    brokerList: string,
    schemaRegistryUrl: string,
    topic: string,
    readyHandler: ProducerReadyHandler,
    failureHandler: FailureHandler,
    deliveryReportHandler: DeliveryReportHandler,
    valueSchemaSubject: string,
    keySchemaSubject?: string,
    pollInterval: number = DefaultPollIntervalMs,
  ) {
    this.topic = topic

    this.registry = new CachedSchemaRegistry(schemaRegistryUrl)

    this.valueSchemaSubject = valueSchemaSubject
    this.keySchemaSubject = keySchemaSubject

    this.producer = new Producer({
      'metadata.broker.list': brokerList,
      dr_cb: true,
    })

    this.producer
      .on('ready', readyHandler)
      .on('event.error', failureHandler)
      .on('delivery-report', deliveryReportHandler)

    this.producer.setPollInterval(pollInterval)
  }

  start = () => this.producer.connect()

  encode = async (
    subject?: string,
    data?: object | string,
  ): Promise<Buffer> => {
    if (!subject) {
      if (typeof data === 'string') {
        return Buffer.from(data)
      } else {
        throw Error('subject not provided, cannot encode')
      }
    } else if (!data) {
      throw Error('data not provided, cannot encode')
    }

    const [schemaId, schema] = await this.registry.getLatestBySubject(subject)
    return encode(schemaId, schema, data)
  }

  produce = async (value: object | string, key?: object | string) => {
    const [encodedValue, encodedKey] = await Promise.all([
      this.encode(this.valueSchemaSubject, value),
      this.encode(this.keySchemaSubject, key),
    ])

    console.debug(
      'AvroProducer.produce: encodedKey =',
      encodedKey,
      'encodedValue = ',
      encodedValue,
    )

    this.producer.produce(
      this.topic,
      null,
      encodedValue,
      encodedKey,
      Date.now(),
    )
  }
}
