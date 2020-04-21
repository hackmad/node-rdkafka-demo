// Based on https://medium.com/walkme-engineering/managing-consumer-commits-and-back-pressure-with-node-js-and-kafka-in-production-cfd20c8120e3
import * as _ from 'lodash'

import { KafkaConsumer, Message } from 'node-rdkafka'
import { messageToString } from './utils'

import {
  Offset,
  FailureHandler,
  CommitNotificationHandler,
  DefaultCommitIntervalMs,
} from './types'

export class CommitManager {
  consumer: KafkaConsumer
  partitionOffsets = new Map<Number, Offset[]>()
  commitIntervalMs: number = DefaultCommitIntervalMs
  commitNotificationHandler: CommitNotificationHandler
  failureHandler: FailureHandler

  constructor(
    consumer: KafkaConsumer,
    commitNotificationHandler: CommitNotificationHandler,
    failureHandler: FailureHandler,
    commitIntervalMs: number = DefaultCommitIntervalMs,
  ) {
    this.consumer = consumer
    this.commitIntervalMs = commitIntervalMs
    this.commitNotificationHandler = commitNotificationHandler
    this.failureHandler = failureHandler
  }

  start = () => {
    setInterval(this.commitProcessedOffsets, this.commitIntervalMs)
    console.debug('CommitManager.start: started')
  }

  notifyStartProcessing = (message: Message) => {
    console.debug(
      'CommitManager.notifyStartProcessing:',
      messageToString(message),
    )

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
    console.debug(
      'CommitManager.notifyFinishedProcessing:',
      messageToString(message),
    )

    const p = message.partition

    if (this.partitionOffsets.has(p)) {
      const pd = this.partitionOffsets.get(p)

      let messages = _.filter(pd, (e) => e.offset == message.offset)

      if (messages.length > 0) {
        messages[0].done = true
      }
    } else {
      console.error(
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
        console.debug(
          'CommitManager.commitProcessedOffsets: committing offsets',
          offsetsToCommit,
        )

        this.consumer.commit(offsetsToCommit)
        this.commitNotificationHandler(offsetsToCommit)
      }
    } catch (e) {
      console.error(
        'CommitManager.commitProcessedOffsets: error committing offsets',
        e,
      )
      this.failureHandler(e)
    }
  }

  rebalance = () => {
    console.log('CommitManager.rebalance: clearing partition data')
    this.partitionOffsets.clear()
  }
}
