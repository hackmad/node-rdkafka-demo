import { Producer } from 'node-rdkafka'

import {
  FailureHandler,
  DeliveryReportHandler,
  ProducerReadyHandler,
  DefaultPollIntervalMs,
} from './types'

import { Logger } from '../logger'

export class StandardProducer {
  producer: Producer
  topic: string
  logger: Logger

  constructor(
    logger: Logger,
    brokerList: string,
    topic: string,
    readyHandler: ProducerReadyHandler,
    failureHandler: FailureHandler,
    deliveryReportHandler: DeliveryReportHandler,
    pollInterval: number = DefaultPollIntervalMs,
  ) {
    this.topic = topic

    this.logger = logger

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

  produce = async (key: string, value: string) => {
    await this.producer.produce(
      this.topic,
      null,
      Buffer.from(value),
      key,
      Date.now(),
    )
  }
}
