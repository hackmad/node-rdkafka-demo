import { Message } from 'node-rdkafka'

export type MessageHandler = (message: Message) => Promise<boolean>
export type FailureHandler = (error: any) => void

export interface Offset {
  topic: string
  offset: number
  done: boolean
}

export type CommitNotificationHandler = (offsets: Offset[]) => void

export const DefaultSeekTimeoutMs = 1000
export const DefaultMinRetryDelayMs = 1000
export const DefaultMaxRetryDelayMs = 30000
export const DefaultConsumeTimeoutMs = 50
export const DefaultCommitIntervalMs = 5000
