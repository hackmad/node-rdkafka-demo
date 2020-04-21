import yargs from 'yargs'

import {
  DefaultConsumeTimeoutMs,
  DefaultSeekTimeoutMs,
  DefaultMinRetryDelayMs,
  DefaultMaxRetryDelayMs,
  DefaultCommitIntervalMs,
} from './consumers/types'

import { CommonArgs } from './commands/types'

<CommonArgs>yargs(process.argv.slice(2))
  .commandDir('commands')
  .option('broker-list', {
    description: 'broker list',
    alias: 'b',
    type: 'string',
    default: 'localhost:9092',
  })
  .option('topic', {
    description: 'topic',
    alias: 't',
    type: 'string',
    default: 'test',
  })
  .option('seek-timeout', {
    description: 'timeout to seek current offset on stall in milliseconds',
    alias: 'sk',
    type: 'number',
    default: DefaultSeekTimeoutMs,
  })
  .option('min-retry-delay', {
    description:
      'min time in milliseconds to delay before retrying consumption ' +
      'when a message fails to process',
    alias: 'minr',
    type: 'number',
    default: DefaultMinRetryDelayMs,
  })
  .option('max-retry-delay', {
    description:
      'max time in milliseconds to delay before retrying consumption ' +
      'when a message fails to process',
    alias: 'maxr',
    type: 'number',
    default: DefaultMaxRetryDelayMs,
  })
  .option('consume-timeout', {
    description: 'timeout in milliseconds for consume to wait for message',
    alias: 'ct',
    type: 'number',
    default: DefaultConsumeTimeoutMs,
  })
  .option('commit-interval', {
    description: 'time in milliseconds for committing offsets',
    alias: 'ci',
    type: 'number',
    default: DefaultCommitIntervalMs,
  })
  .demandCommand()
  .help()
  .alias('help', 'h').argv
