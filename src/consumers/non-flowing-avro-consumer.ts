import { NonFlowingConsumer } from './non-flowing-consumer'
import {
  MessageHandler,
  FailureHandler,
  CommitNotificationHandler,
  DefaultSeekTimeoutMs,
  DefaultMinRetryDelayMs,
  DefaultMaxRetryDelayMs,
  DefaultConsumeTimeoutMs,
  DefaultCommitIntervalMs,
} from './types'
import { Message, MessageKey, MessageValue } from 'node-rdkafka'
import {
  CachedSchemaRegistry,
  DefaultRefetchThresholdMs,
} from '../schema-registry'
import { decode } from '../avro'

type MessageData = Buffer | string | null | undefined

export class NonFlowingAvroConsumer {
  consumer: NonFlowingConsumer
  messageHandler: MessageHandler
  registry: CachedSchemaRegistry

  constructor(
    brokerList: string,
    schemaRegistryUrl: string,
    topic: string,
    consumerGroupId: string,
    messageHandler: MessageHandler,
    failureHandler: FailureHandler,
    commitNotificationHandler: CommitNotificationHandler,
    seekTimeoutMs: number = DefaultSeekTimeoutMs,
    minRetryDelayMs: number = DefaultMinRetryDelayMs,
    maxRetryDelayMs: number = DefaultMaxRetryDelayMs,
    consumeTimeoutMs: number = DefaultConsumeTimeoutMs,
    commitIntervalMs: number = DefaultCommitIntervalMs,
    refetchThresholdMs: number = DefaultRefetchThresholdMs,
  ) {
    // We will wire up our own message handler to decode messages.
    this.messageHandler = messageHandler

    this.registry = new CachedSchemaRegistry(
      schemaRegistryUrl,
      refetchThresholdMs,
    )

    this.consumer = new NonFlowingConsumer(
      brokerList,
      topic,
      consumerGroupId,
      this.handleMessage,
      failureHandler,
      commitNotificationHandler,
      seekTimeoutMs,
      minRetryDelayMs,
      maxRetryDelayMs,
      consumeTimeoutMs,
      commitIntervalMs,
    )
  }

  start = () => this.consumer.start()

  handleMessage = async (message: Message) => {
    try {
      const { key, value } = message

      const [decodedKey, decodedValue] = await Promise.all([
        this.decode(key, true),
        this.decode(value, false),
      ])

      console.debug(
        'NonFlowingAvroConsumer.handleMessage: decoded message',
        decodedKey?.toString(),
        decodedValue,
      )

      const decodedMessage = {
        ...message,
        key: <MessageKey>decodedKey,
        value: <MessageValue>decodedValue,
      }
      return await this.messageHandler(decodedMessage)
    } catch (e) {
      console.error('NonFlowingAvroConsumer.handleMessage: error', e)
      return false
    }
  }

  decode = async (
    data: MessageData,
    isKey: boolean = false,
  ): Promise<MessageData> => {
    const t = isKey ? 'key' : 'value'

    if (!data && !isKey) {
      return data // No data allowed for keys
    } else if (!data) {
      throw Error('missing message value')
    }
    if (typeof data === 'string') {
      throw Error(`message ${t} is not avro encoded`)
    }
    if (data[0] !== 0) {
      throw Error(`message ${t} does start with magic byte 0`)
    }

    const [schemaId, payload] = decode(data)
    const schema = await this.registry.getLatestBySchemaId(schemaId)
    return schema.fromBuffer(payload)
  }
}
