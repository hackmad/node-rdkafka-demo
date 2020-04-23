import {
  Logger as WinstonLogger,
  LeveledLogMethod,
  createLogger,
  transports,
  format,
} from 'winston'

const customFormatter = format((info, _opts) => {
  info.msg = info.message
  delete info.message
  return info
})

export class Logger {
  logger: WinstonLogger

  constructor() {
    this.logger = createLogger({
      defaultMeta: { service: 'node-rdkafka-demo' },
      format: format.combine(customFormatter(), format.json()),
      transports: [new transports.Console({ level: 'info' })],
    })
  }

  // Generic logger that can include error messages with metadata
  //
  // This is so we can do this for non-critical errors:
  //   log.warn("problem happened", err, {topic: 'test', partitions: 30})
  private log = (
    fn: LeveledLogMethod,
    message: string,
    error?: any,
    meta?: object | string | number,
  ) => {
    if (!error) {
      fn(message)
    } else {
      const m = meta instanceof Object ? meta : { meta: meta }

      if (error instanceof Error) {
        fn(message, {
          error: {
            message: error.message,
            name: error.name,
            stack: error.stack,
          },
          ...m,
        })
      } else {
        fn(message, { error: error, ...m })
      }
    }
  }

  info = (message: string, error?: any, meta?: object | string | number) => {
    this.log(this.logger.info, message, error, meta)
  }

  error = (message: string, error?: any, meta?: object | string | number) => {
    this.log(this.logger.error, message, error, meta)
  }

  warn = (message: string, error?: any, meta?: object | string | number) => {
    this.log(this.logger.warn, message, error, meta)
  }

  debug = (message: string, error?: any, meta?: object | string | number) => {
    this.log(this.logger.debug, message, error, meta)
  }
}
