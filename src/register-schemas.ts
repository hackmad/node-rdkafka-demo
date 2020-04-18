import { SchemaRegistry } from './schema-registry'

import * as notificationKeySchema from '../avro/notification-key.json'
import * as notificationValueSchema from '../avro/notification-value.json'

const topic = 'notifications'
const [url] = process.argv.slice(2)
const registry = new SchemaRegistry(url || 'http://localhost:8081')

const main = async () => {
  console.info('Registering notifications-key schema')
  await registry.register(topic, notificationKeySchema, true)

  console.info('Registering notifications-value schema')
  await registry.register(topic, notificationValueSchema, false)
}
main()
