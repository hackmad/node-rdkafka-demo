// Based on https://medium.com/walkme-engineering/managing-consumer-commits-and-back-pressure-with-node-js-and-kafka-in-production-cfd20c8120e3

import * as _ from 'lodash'

import {
  Assignment,
  KafkaConsumer,
  LibrdKafkaError,
  Message,
  CODES,
} from 'node-rdkafka'

import { queue, AsyncQueue } from 'async'

import { CommitManager, CommitNotificationHandler } from './commit-manager'

import {
  messageToString,
  delay,
  exponentialBackoff,
  assignmentToArray,
} from './utils'

export type MessageHandler = (message: Message) => boolean
export type FailureHandler = (error: any) => void

export class BackPressuredConsumer {
  consumer: KafkaConsumer
  topic: string
  paused: boolean = false
  maxQueueSize: number = 10
  seekTimeoutMs: number = 1000
  messageHandler: MessageHandler
  failureHandler: FailureHandler
  minRetryDelayMs: number = 1000
  maxRetryDelayMs: number = 30000

  // Tracks retry attempts for stall delay logic per partition
  retryAttempts = new Map<number, number>()

  // Maintain separate queues per partition. If we try to use a single queue
  // for all partitions, the consumer will stall all partitions if messageHandler
  // returns false.
  queues = new Map<number, AsyncQueue<Message>>()

  commitManager: CommitManager

  constructor(
    brokerList: string,
    topic: string,
    consumerGroupId: string,
    messageHandler: MessageHandler,
    failureHandler: FailureHandler,
    commitNotificationHandler: CommitNotificationHandler,
    maxQueueSize: number = 10,
    seekTimeoutMs: number = 1000,
  ) {
    // Store the config
    this.topic = topic
    this.seekTimeoutMs = seekTimeoutMs
    this.messageHandler = messageHandler
    this.failureHandler = failureHandler
    this.maxQueueSize = maxQueueSize

    // Create the consumer
    const consumerConfig = {
      'group.id': consumerGroupId,
      'metadata.broker.list': brokerList,
      'enable.auto.commit': false,
      'auto.offset.reset': 'earliest',
      offset_commit_cb: this.onOffsetCommit,
      rebalance_cb: this.onRebalance,
    }

    this.consumer = new KafkaConsumer(consumerConfig, {})
    this.consumer.on('ready', this.onReady).on('data', this.onData)

    this.commitManager = new CommitManager(
      this.consumer,
      commitNotificationHandler,
      failureHandler,
    )
  }

  createQueue = (messageHandler: MessageHandler): AsyncQueue<Message> => {
    // We cannot have a parallism > 1 otherwise messages in a partition
    // will get processed out of order. This could be made configurable
    // for other usecases where that is not necessary.
    const q = queue(this.handleMessage(messageHandler), 1)
    q.drain(this.onQueueDrained)
    return q
  }

  start = () => {
    // Connect to Kafka
    this.consumer.connect()
  }

  handleMessage = (messageHandler: MessageHandler) => async (
    message: Message,
    done: () => void,
  ) => {
    // Do this here instead of when seeking back in stall().
    // Its harder to do async stuff in that code
    const attemptNum = this.retryAttempts[message.partition]
    if (attemptNum > 0) {
      console.debug(
        'BackPressuredConsumer.handleMessage: stalling partition',
        message.partition,
        'offset',
        message.offset,
      )
      await this.stallDelay(message.partition, attemptNum)
    }

    // If message is not handled then stall with exponential backoff;
    // otherwise commit the message.
    try {
      this.commitManager.notifyStartProcessing(message)

      const handled = messageHandler(message)

      if (!handled) {
        console.debug(
          'BackPressuredConsumer.handleMessage: not handled',
          messageToString(message),
        )
        this.stall(message)
      } else {
        console.debug(
          'BackPressuredConsumer.handleMessage: handled',
          messageToString(message),
        )

        this.retryAttempts[message.partition] = 0
        this.commitManager.notifyFinishedProcessing(message)
      }
    } catch (e) {
      console.error(
        'BackPressuredConsumer.handleMessage: error handling message',
        e,
      )
    } finally {
      done()
    }
  }

  stallDelay = async (partition: number, attemptNum: number) => {
    const ms = exponentialBackoff(
      this.minRetryDelayMs,
      this.maxRetryDelayMs,
      attemptNum - 1,
    )

    console.debug(
      `BackPressuredConsumer.stallDelay: partition ${partition}, attempt ${attemptNum} - ${ms} ms`,
    )

    await delay(ms)

    console.debug('BackPressuredConsumer.stallDelay: ${partition}, done')
  }

  stall = (message: Message) => {
    // Clear everything and seek back to failed message
    console.debug(
      'BackPressuredConsumer.stall: partition = ',
      message.partition,
    )
    this.queues[message.partition].remove(({}) => true)

    // Seeking back to an older offset will trigger onData() and librdkafka
    // will send multiple messages to us. However, the handleMessage will
    // delay processing using exponential backoff and if this same message
    // is not handled again, the process will repeat. Once the message in
    // handled, the regular in order processing of the partition will resume.
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
            'BackPressuredConsumer.stall: failed to seek to message',
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

  onQueueDrained = () => {
    // Message queue is drained after back pressure.
    // Resume consumption if paused.
    if (this.paused) {
      this.consumer.resume(assignmentToArray(this.consumer.assignments()))
      this.paused = false
    }
  }

  onReady = () => {
    // Wire up the topic and start consuming messages.
    this.consumer.subscribe([this.topic])
    this.consumer.consume()

    this.commitManager.start()

    console.info('BackPressuredConsumer.onReady: consumer started')
  }

  onData = (message: Message) => {
    console.debug('BackPressuredConsumer.onData: ', messageToString(message))

    // Push message onto queue
    this.queues[message.partition].push(message)

    // Pause consumption if we hit maxQueueSize. When queue is drained
    // consumption will resume again.
    if (this.queues[message.partition].length() > this.maxQueueSize) {
      this.consumer.pause(assignmentToArray(this.consumer.assignments()))
      this.paused = true
    }
  }

  onOffsetCommit = (err: any, topicPartitions: any) => {
    if (err) {
      // Log the error and notify failure
      console.error('BackPressuredConsumer.onOffsetCommit:', err)
      this.failureHandler(err)
    } else {
      // Log the committed offset
      console.debug('BackPressuredConsumer.onOffsetCommit: ', topicPartitions)
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
      console.error('BackPressuredConsumer.onRebalance: error', err)
    }
  }

  assignPartitions = (assignment: Assignment) => {
    console.debug('BackPressuredConsumer.assignPartitions: ', assignment)

    // Clear out the queues and retry attempts.
    _.forEach(assignmentToArray(assignment), ({ partition }) => {
      this.queues[partition] = this.createQueue(this.messageHandler)
      this.retryAttempts[partition] = 0
    })

    this.consumer.assign(assignment)
  }

  revokePartitions = (assignment: Assignment) => {
    console.debug('BackPressuredConsumer.revokePartitions:')

    // If consumer was paused, resume it with the new partition assignment.
    if (this.paused) {
      console.debug(
        'BackPressuredConsumer.revokePartitions: paused -> resuming',
      )
      this.consumer.resume(assignmentToArray(assignment))
      this.paused = false
    } else {
      console.debug(
        'BackPressuredConsumer.revokePartitions: paused -> do nothing',
      )
    }

    // No need to handle messages left in queue. Partition reassignment
    // will redirect them.
    console.debug('BackPressuredConsumer.revokePartitions: clearing queues')
    this.queues.forEach((q: AsyncQueue<Message>, _partition: number) =>
      q.remove(({}) => true),
    )
    this.queues.clear()
    this.retryAttempts.clear()

    this.consumer.unassign()
  }
}
