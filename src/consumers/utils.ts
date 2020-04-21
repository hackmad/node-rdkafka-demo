import * as _ from 'lodash'

import { Assignment, Message } from 'node-rdkafka'

export const delay = (ms: number) =>
  new Promise((resolve) => setTimeout(resolve, ms))

export const messageToString = (message: Message): string => {
  const key = message.key?.toString() ?? ''
  const value = message.value.toString()

  return (
    `${key}|${value} topic: ${message.topic}, ` +
    `partition: ${message.partition}, offset: ${message.offset}`
  )
}

// Flatten because of type ambiguity of Assignment which gets returned
// as an array not single item.
export const assignmentToArray = (a: Assignment) => _.flatten([a])

export const exponentialBackoff = (
  minMs: number,
  maxMs: number,
  attemptNum: number,
) => Math.min(Math.pow(2, attemptNum) * minMs, maxMs)
