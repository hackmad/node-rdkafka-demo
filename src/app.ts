import yargs from 'yargs'

import {
  DefaultConsumeTimeout,
  DefaultSeekTimeoutMs,
  DefaultMinRetryDelay,
  DefaultMaxRetryDelay,
  DefaultCommitInterval,
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
    default: DefaultMinRetryDelay,
  })
  .option('max-retry-delay', {
    description:
      'max time in milliseconds to delay before retrying consumption ' +
      'when a message fails to process',
    alias: 'maxr',
    type: 'number',
    default: DefaultMaxRetryDelay,
  })
  .option('consume-timeout', {
    description: 'timeout in milliseconds for consume to wait for message',
    alias: 'ct',
    type: 'number',
    default: DefaultConsumeTimeout,
  })
  .option('commit-interval', {
    description: 'time in milliseconds for committing offsets',
    alias: 'ci',
    type: 'number',
    default: DefaultCommitInterval,
  })
  .demandCommand()
  .help()
  .alias('help', 'h').argv
