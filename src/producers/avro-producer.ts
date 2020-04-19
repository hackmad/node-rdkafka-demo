import { Producer, MessageKey, MessageValue } from 'node-rdkafka'
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
    pollInterval: number = DefaultPollIntervalMs,
    valueSchemaSubject: string,
    keySchemaSubject?: string,
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

  produce = async (value: MessageValue, key?: MessageKey) => {
    const valueSchema = await this.registry.getLatestBySubject(
      this.keySchemaSubject,
    )
    const keySchema = await this.registry.getLatestBySubject(
      this.keySchemaSubject,
    )

    const encodedValue = valueSchema.toBuffer(value)
    const encodedKey = keySchema ? keySchema.toBuffer(key) : null

    this.producer.produce(
      this.topic,
      null,
      encodedValue,
      encodedKey,
      Date.now(),
    )
  }
}
