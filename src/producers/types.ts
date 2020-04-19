import { LibrdKafkaError, DeliveryReport } from 'node-rdkafka'

export type FailureHandler = (error: LibrdKafkaError) => void

export type DeliveryReportHandler = (
  err: LibrdKafkaError,
  report: DeliveryReport,
) => void

export type ProducerReadyHandler = () => void

export const DefaultPollIntervalMs = 100

