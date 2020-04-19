import { Message } from 'node-rdkafka'

export type MessageHandler = (message: Message) => boolean
export type FailureHandler = (error: any) => void

export interface Offset {
  topic: string
  offset: number
  done: boolean
}

export type CommitNotificationHandler = (offsets: Offset[]) => void

export const DefaultSeekTimeoutMs = 1000
export const DefaultMinRetryDelay = 1000
export const DefaultMaxRetryDelay = 30000
export const DefaultConsumeTimeout = 50
export const DefaultCommitInterval = 5000
