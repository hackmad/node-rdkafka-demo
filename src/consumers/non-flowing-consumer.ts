import * as _ from 'lodash'

import {
  Assignment,
  KafkaConsumer,
  LibrdKafkaError,
  Message,
  CODES,
  ConsumerTopicConfig,
  ConsumerGlobalConfig,
} from 'node-rdkafka'

import { CommitManager } from './commit-manager'

import {
  messageToString,
  delay,
  exponentialBackoff,
  assignmentToArray,
} from './utils'

import {
  MessageHandler,
  FailureHandler,
  CommitNotificationHandler,
  DefaultSeekTimeoutMs,
  DefaultMinRetryDelay,
  DefaultMaxRetryDelay,
  DefaultConsumeTimeout,
  DefaultCommitInterval,
} from './types'

export class NonFlowingConsumer {
  consumer: KafkaConsumer
  topic: string
  paused: boolean = false
  seekTimeoutMs: number
  minRetryDelayMs: number
  maxRetryDelayMs: number
  messageHandler: MessageHandler
  failureHandler: FailureHandler

  // Tracks retry attempts for stall delay logic per partition
  retryAttempts = new Map<number, number>()

  commitManager: CommitManager

  constructor(
    brokerList: string,
    topic: string,
    consumerGroupId: string,
    messageHandler: MessageHandler,
    failureHandler: FailureHandler,
    commitNotificationHandler: CommitNotificationHandler,
    seekTimeoutMs: number = DefaultSeekTimeoutMs,
    minRetryDelayMs: number = DefaultMinRetryDelay,
    maxRetryDelayMs: number = DefaultMaxRetryDelay,
    consumeTimeoutMs: number = DefaultConsumeTimeout,
    commitIntervalMs: number = DefaultCommitInterval,
  ) {
    // Store the config
    this.topic = topic
    this.seekTimeoutMs = seekTimeoutMs
    this.messageHandler = messageHandler
    this.failureHandler = failureHandler
    this.minRetryDelayMs = minRetryDelayMs
    this.maxRetryDelayMs = maxRetryDelayMs

    // Create the consumer
    const globalConfig: ConsumerGlobalConfig = {
      'group.id': consumerGroupId,
      'metadata.broker.list': brokerList,
      'enable.auto.commit': false,
      offset_commit_cb: this.onOffsetCommit,
      rebalance_cb: this.onRebalance,
    }
    const topicConfig: ConsumerTopicConfig = {
      'auto.offset.reset': 'earliest',
    }

    this.consumer = new KafkaConsumer(globalConfig, topicConfig)
    this.consumer.setDefaultConsumeTimeout(consumeTimeoutMs)
    this.consumer.on('ready', this.onReady)

    this.commitManager = new CommitManager(
      this.consumer,
      commitNotificationHandler,
      failureHandler,
      commitIntervalMs,
    )
  }

  start = () => {
    // Connect to Kafka
    this.consumer.connect()
  }

  onReady = () => {
    // Start the commit manager
    this.commitManager.start()

    // Wire up the topic and start consuming messages.
    this.consumer.subscribe([this.topic])
    this.consumer.consume(1, this.handleMessage)

    // Non-flowing mode. We'll do one message at a time so we can control
    // offsets that are committed.
    console.info('NonFlowingConsumer.onReady: consumer started')
  }

  handleMessage = async (err: LibrdKafkaError, messages: Message[]) => {
    // Handle errors
    if (err) {
      this.failureHandler(err)
      setTimeout(() => this.consumer.consume(1, this.handleMessage), 100)
      return
    }

    // No message, try again after a short delay.
    if (messages.length === 0) {
      setTimeout(() => this.consumer.consume(1, this.handleMessage), 100)
      return
    }

    // There should only be 1 message.
    const [message] = messages
    console.debug(
      'NonFlowingConsumer.handleMessage: ',
      messageToString(message),
    )

    // Do this here instead of when seeking back in stall().
    // Its harder to do async stuff in that code
    const attemptNum = this.retryAttempts[message.partition]
    if (attemptNum > 0) {
      console.debug(
        `NonFlowingConsumer.handleMessage: ` +
          `stalling partition ${message.partition}, ` +
          `offset ${message.offset}`,
      )
      await this.stallDelay(message.partition, attemptNum)
    }

    // If message is not handled then stall with exponential backoff;
    // otherwise commit the message.
    try {
      this.commitManager.notifyStartProcessing(message)

      const handled = this.messageHandler(message)

      if (!handled) {
        console.debug(
          'NonFlowingConsumer.handleMessage: not handled',
          messageToString(message),
        )
        this.stall(message)
      } else {
        console.debug(
          'NonFlowingConsumer.handleMessage: handled',
          messageToString(message),
        )

        this.retryAttempts[message.partition] = 0
        this.commitManager.notifyFinishedProcessing(message)
      }
    } catch (e) {
      console.error(
        'NonFlowingConsumer.handleMessage: error handling message',
        e,
      )
      this.failureHandler(e)
    } finally {
      // Next message
      console.debug('NonFlowingConsumer.handleMessage: next message')
      this.consumer.consume(1, this.handleMessage)
    }
  }

  stallDelay = async (partition: number, attemptNum: number) => {
    const ms = exponentialBackoff(
      this.minRetryDelayMs,
      this.maxRetryDelayMs,
      attemptNum - 1,
    )

    console.debug(
      `NonFlowingConsumer.stallDelay: ` +
        `partition ${partition}, attempt ${attemptNum} - ${ms} ms`,
    )

    await delay(ms)

    console.debug('NonFlowingConsumer.stallDelay: ${partition}, done')
  }

  stall = (message: Message) => {
    // Clear everything and seek back to failed message
    console.debug('NonFlowingConsumer.stall: partition = ', message.partition)

    // Seek back to current message's offset.
    this.consumer.seek(
      {
        topic: message.topic,
        partition: message.partition,
        offset: message.offset,
      },
      this.seekTimeoutMs,
      async (err: LibrdKafkaError) => {
        if (err) {
          // This could mean a potential problem with consumer. Notify the
          // calling code.
          console.error(
            'NonFlowingConsumer.stall: failed to seek to message',
            err,
          )
          this.failureHandler(err)
        }

        // Increment the retry counter. We won't do the delay here because
        // we are in an async callback which will not delay anything.
        this.retryAttempts[message.partition] =
          this.retryAttempts[message.partition] + 1
      },
    )
  }

  onOffsetCommit = (err: any, topicPartitions: any) => {
    if (err) {
      // Log the error and notify failure
      console.error('NonFlowingConsumer.onOffsetCommit:', err)
      this.failureHandler(err)
    } else {
      // Log the committed offset
      console.debug('NonFlowingConsumer.onOffsetCommit: ', topicPartitions)
    }
  }

  onRebalance = (err: LibrdKafkaError, assignment: Assignment) => {
    // Handle rebalance events
    if (err.code === CODES.ERRORS.ERR__ASSIGN_PARTITIONS) {
      this.assignPartitions(assignment)
    } else if (err.code === CODES.ERRORS.ERR__REVOKE_PARTITIONS) {
      this.revokePartitions(assignment)
      this.commitManager.rebalance()
    } else {
      console.error('NonFlowingConsumer.onRebalance: error', err)
    }
  }

  assignPartitions = (assignment: Assignment) => {
    console.debug('NonFlowingConsumer.assignPartitions: ', assignment)

    // Clear out the retry attempts.
    _.forEach(assignmentToArray(assignment), ({ partition }) => {
      this.retryAttempts[partition] = 0
    })

    this.consumer.assign(assignment)
  }

  revokePartitions = (assignment: Assignment) => {
    console.debug('NonFlowingConsumer.revokePartitions:')

    // If consumer was paused, resume it with the new partition assignment.
    if (this.paused) {
      console.debug('NonFlowingConsumer.revokePartitions: paused -> resuming')
      this.consumer.resume(assignmentToArray(assignment))
      this.paused = false
    } else {
      console.debug('NonFlowingConsumer.revokePartitions: paused -> do nothing')
    }

    // Clear retry attempts
    this.retryAttempts.clear()

    this.consumer.unassign()
  }
}
