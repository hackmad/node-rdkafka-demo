import { KafkaConsumer, Message } from 'node-rdkafka'

const DefaultCommitInterval = 5000

interface PartitionEntry {
  topic: string
  offset: number
  done: boolean
}

export type CommitNotificationHandler = (offsets: PartitionEntry[]) => void
export type FailureHandler = (error: any) => void

export class CommitManager {
  partitionData = new Map<Number, PartitionEntry[]>()
  lastCommitted: PartitionEntry[] = []
  consumer: KafkaConsumer
  commitIntervalMs: number = DefaultCommitInterval
  commitNotificationHandler: CommitNotificationHandler
  failureHandler: FailureHandler

  constructor(
    consumer: KafkaConsumer,
    commitNotificationHandler: CommitNotificationHandler,
    failureHandler: FailureHandler,
    commitIntervalMs: number = DefaultCommitInterval,
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
      'notifyStartProcessing:',
      message.key?.toString(),
      message.value.toString(),
      message.topic,
      message.partition,
      message.offset,
    )

    const p = message.partition

    if (!this.partitionData.has(p)) {
      this.partitionData[p] = <PartitionEntry[]>[]
    }

    const exists =
      this.partitionData[p].filter(
        (e: PartitionEntry) => e.offset === message.offset,
      ).length > 0

    if (!exists) {
      this.partitionData[p].push({
        toic: message.topic,
        offset: message.offset,
        done: false,
      })
    }
  }

  notifyFinishedProcessing = (message: Message) => {
    console.debug(
      'CommitManager.notifyFinishedProcessing:',
      message.key?.toString(),
      message.value.toString(),
      message.topic,
      message.partition,
      message.offset,
    )

    const p = message.partition

    this.partitionData[p] = this.partitionData[p] || []

    let messages = this.partitionData[p].filter(
      (e: PartitionEntry) => e.offset == message.offset,
    )

    if (messages.length > 0) {
      messages[0].done = true
    }
  }

  commitProcessedOffsets = async () => {
    try {
      let offsetsToCommit = []

      this.partitionData.forEach((offsets, p) => {
        const pi = offsets.findIndex((e) => e.done)
        const npi = offsets.findIndex((e) => !e.done)

        const lastProcessed =
          npi > 0
            ? offsets[npi - 1]
            : pi > -1
            ? offsets[offsets.length - 1]
            : null

        if (lastProcessed) {
          offsetsToCommit.push({
            topic: lastProcessed.topic,
            partition: p,
            offset: lastProcessed.offset,
          })

          // remove committed records
          offsets.splice(0, offsets.indexOf(lastProcessed) + 1)
        }
      })

      if (offsetsToCommit.length > 0) {
        console.debug(
          'CommitManager.commitProcessedOffsets: committing offsets',
          offsetsToCommit,
        )

        this.consumer.commitSync(offsetsToCommit)

        this.lastCommitted = offsetsToCommit

        this.commitNotificationHandler(offsetsToCommit)
      } else {
        //console.debug(
        //  'CommitManager.commitProcessedOffsets: no offsets to commit',
        //)
      }

      Promise.resolve()
    } catch (e) {
      console.error(
        'CommitManager.commitProcessedOffsets: error committing offsets',
        e,
      )
      Promise.reject(e)

      this.failureHandler(e)
    }
  }

  rebalance = () => {
    console.log('CommitManager.rebalance: clearing partition data')
    this.partitionData.clear()
  }
}
