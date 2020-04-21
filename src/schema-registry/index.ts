import axios from 'axios'
import avsc from 'avsc'

interface Schema {
  schemaId: number
  type: avsc.Type
  timestamp: Date
}

export const DefaultRefetchThresholdMs = 1 * 60 * 60 * 1000 // 1 hour

export class CachedSchemaRegistry {
  url: string
  refetchThresholdMs: number

  schemasBySubject = new Map<string, Schema>()
  schemasById = new Map<number, Schema>()

  constructor(
    url: string,
    refetchThresholdMs: number = DefaultRefetchThresholdMs,
  ) {
    this.url = url
    this.refetchThresholdMs = refetchThresholdMs
  }

  isSchemaOld = (s: Schema): boolean =>
    new Date().getTime() - s.timestamp.getTime() > this.refetchThresholdMs

  getLatestBySchemaId = async (schemaId: number): Promise<avsc.Type> => {
    const url = `${this.url}/schemas/ids/${schemaId}`

    const s = this.schemasById.get(schemaId)
    if (s && !this.isSchemaOld(s)) {
      return s.type
    }

    try {
      const response = await axios.get(url)

      const schema = response.data?.schema
      if (!schema) {
        throw Error('SchemaRegistry.getBySchemaId: no schema returned')
      }

      const avroType = avsc.Type.forSchema(schema)

      this.schemasById.set(schemaId, {
        schemaId: schemaId,
        type: avroType,
        timestamp: new Date(),
      })

      return avroType
    } catch (e) {
      console.error('SchemaRegistry.getLatestBySchemaId: error', e)
      throw e
    }
  }

  getLatestBySubject = async (
    subject: string,
  ): Promise<[number, avsc.Type]> => {
    const s = this.schemasBySubject.get(subject)
    if (s && !this.isSchemaOld(s)) {
      return [s.schemaId, s.type]
    }

    try {
      const versions = await this.getVersionsBySubject(subject)
      const latest = versions[versions.length - 1]
      const url = `${this.url}/subjects/${subject}/versions/${latest}`

      const response = await axios.get(url)

      const data = response.data
      console.debug('********', subject, data)
      if (!data) {
        throw Error('SchemaRegistry.getLatestBySubject: no schema returned')
      }

      const schemaId = data.id
      const avroType = avsc.Type.forSchema(JSON.parse(data.schema))

      const s = { schemaId: schemaId, type: avroType, timestamp: new Date() }
      this.schemasBySubject.set(subject, s)
      this.schemasById.set(schemaId, s)

      return [schemaId, avroType]
    } catch (e) {
      console.error('SchemaRegistry.getLatestBySubject: error', e)
      throw e
    }
  }

  getVersionsBySubject = async (subject: string): Promise<Number[]> => {
    const url = `${this.url}/subjects/${subject}/versions`

    try {
      const response = await axios.get(url)
      return response.data
    } catch (e) {
      console.error('SchemaRegistry.getVersionsBySubject: error', e)
      throw e
    }
  }

  register = async (
    subject: string,
    schema: object,
    isKeySchema: boolean,
  ): Promise<void> => {
    const suffix = isKeySchema ? 'key' : 'value'
    const url = `${this.url}/subjects/${subject}-${suffix}/versions`

    try {
      const jsonSchema = JSON.stringify(schema)

      const response = await axios.post(
        url,
        { schema: jsonSchema },
        { headers: { 'Content-Type': 'application/json' } },
      )
      if (response.status !== 200) {
        throw Error(`${response.status} ${response.statusText}`)
      }
    } catch (e) {
      console.error('SchemaRegistry.register: error', e)
      throw e
    }
  }
}
