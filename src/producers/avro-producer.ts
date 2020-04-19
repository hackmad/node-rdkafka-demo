import { Producer, MessageKey, MessageValue } from 'node-rdkafka'
import { CachedSchemaRegistry } from '../schema-registry'

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
  }

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
