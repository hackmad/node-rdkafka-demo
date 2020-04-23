// Based on https://medium.com/walkme-engineering/managing-consumer-commits-and-back-pressure-with-node-js-and-kafka-in-production-cfd20c8120e3
import * as _ from 'lodash'

import { KafkaConsumer, Message } from 'node-rdkafka'

import {
  Offset,
  FailureHandler,
  CommitNotificationHandler,
  DefaultCommitIntervalMs,
} from './types'

import { Logger } from '../logger'

export class CommitManager {
  consumer: KafkaConsumer
  partitionOffsets = new Map<Number, Offset[]>()
  commitIntervalMs: number = DefaultCommitIntervalMs
  commitNotificationHandler: CommitNotificationHandler
  failureHandler: FailureHandler
  logger: Logger

  constructor(
    logger: Logger,
    consumer: KafkaConsumer,
    commitNotificationHandler: CommitNotificationHandler,
    failureHandler: FailureHandler,
    commitIntervalMs: number = DefaultCommitIntervalMs,
  ) {
    this.logger = logger
    this.consumer = consumer
    this.commitIntervalMs = commitIntervalMs
    this.commitNotificationHandler = commitNotificationHandler
    this.failureHandler = failureHandler
  }

  start = () => {
    setInterval(this.commitProcessedOffsets, this.commitIntervalMs)
    this.logger.debug('CommitManager.start: started')
  }

  notifyStartProcessing = (message: Message) => {
    this.logger.debug('CommitManager.notifyStartProcessing:', { message })

    const p = message.partition

    if (!this.partitionOffsets.has(p)) {
      this.partitionOffsets.set(p, [])
    }

    const pd = this.partitionOffsets.get(p)
    const exists = _.filter(pd, (e) => e.offset === message.offset).length > 0

    if (!exists) {
      pd.push({
        topic: message.topic,
        offset: message.offset,
        done: false,
      })
    }
  }

  notifyFinishedProcessing = (message: Message) => {
    this.logger.debug('CommitManager.notifyFinishedProcessing:', message)

    const p = message.partition

    if (this.partitionOffsets.has(p)) {
      const pd = this.partitionOffsets.get(p)

      let messages = _.filter(pd, (e) => e.offset == message.offset)

      if (messages.length > 0) {
        messages[0].done = true
      }
    } else {
      this.logger.error(
        'CommitManager.notifyFinishedProcessing: error ' +
          'called without notifyStartProcessing().',
      )
    }
  }

  commitProcessedOffsets = async () => {
    try {
      let offsetsToCommit = []

      this.partitionOffsets.forEach((offsets, p) => {
        // Find first offset we can commit in partition p.
        const firstDone = offsets.findIndex((e) => e.done)
        const firstNotDone = offsets.findIndex((e) => !e.done)

        const lastProcessed =
          firstNotDone > 0
            ? firstNotDone - 1
            : firstDone > -1
            ? offsets.length - 1
            : -1

        if (lastProcessed >= 0) {
          // We need to add one to the offset otherwise on rebalance or when
          // the consumer restarts, it will reprocess.
          offsetsToCommit.push({
            topic: offsets[lastProcessed].topic,
            partition: p,
            offset: offsets[lastProcessed].offset + 1,
          })

          // Remove committed records
          offsets.splice(0, lastProcessed + 1)
        }
      })

      if (offsetsToCommit.length > 0) {
        this.logger.debug(
          'CommitManager.commitProcessedOffsets: committing offsets',
          { offets: offsetsToCommit },
        )

        this.consumer.commit(offsetsToCommit)
        this.commitNotificationHandler(offsetsToCommit)
      }
    } catch (e) {
      this.logger.error(
        'CommitManager.commitProcessedOffsets: error committing offsets',
        e,
      )
      this.failureHandler(e)
    }
  }

  rebalance = () => {
    this.logger.info('CommitManager.rebalance: clearing partition data')
    this.partitionOffsets.clear()
  }
}
