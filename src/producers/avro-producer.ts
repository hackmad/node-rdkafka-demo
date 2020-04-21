import { Producer } from 'node-rdkafka'
import { CachedSchemaRegistry } from '../schema-registry'

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
    const magic = Buffer.from([0])
    const payload = schema.toBuffer(data)
    return Buffer.concat([magic, this.toBytesInt32(schemaId), payload])
  }

  toBytesInt32 = (n: number) => {
    const arr = new ArrayBuffer(4) // an Int32 takes 4 bytes
    const view = new DataView(arr)
    view.setUint32(0, n, false) // byteOffset = 0; litteEndian = false
    return new Buffer(arr)
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
