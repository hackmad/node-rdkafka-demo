import { Producer } from 'node-rdkafka'

export type ProducerReadyHandler = () => void

export class StandardProducer {
  producer: Producer
  topic: string

  constructor(
    brokerList: string,
    topic: string,
    readyHandler: ProducerReadyHandler,
  ) {
    this.topic = topic

    this.producer = new Producer({
      'metadata.broker.list': brokerList,
      dr_cb: true,
    })

    this.producer.on('ready', readyHandler)
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
