import axios from 'axios'
import avsc from 'avsc'

interface Schema {
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

      const jsonSchema = JSON.parse(schema)
      const avroType = avsc.Type.forSchema(jsonSchema)

      this.schemasById.set(schemaId, {
        type: avroType,
        timestamp: new Date(),
      })

      return avroType
    } catch (e) {
      console.error('SchemaRegistry.getLatestBySchemaId: error', e)
      throw e
    }
  }

  getLatestBySubject = async (subject: string): Promise<avsc.Type> => {
    const s = this.schemasBySubject.get(subject)
    if (s && !this.isSchemaOld(s)) {
      return s.type
    }

    try {
      const versions = await this.getVersionsBySubject(subject)
      const latest = versions.slice(-1)[0]
      const url = `${this.url}/schemas/subjects/${subject}/versions/${latest}/schema`

      const response = await axios.get(url)

      const schema = response.data
      if (!schema) {
        throw Error('SchemaRegistry.getBySchemaId: no schema returned')
      }

      const jsonSchema = JSON.parse(schema)
      const avroType = avsc.Type.forSchema(jsonSchema)

      this.schemasBySubject.set(subject, {
        type: avroType,
        timestamp: new Date(),
      })

      return avroType
    } catch (e) {
      console.error('SchemaRegistry.getLatestBySubject: error', e)
      throw e
    }
  }

  getVersionsBySubject = async (subject: string): Promise<Number[]> => {
    const url = `${this.url}/schemas/subjects/${subject}/versions`

    try {
      const response = await axios.get(url)

      const versions = response.data
      if (!versions) {
        throw Error(
          'SchemaRegistry.getVersionsBySubject: ' +
            'no schema versions returned',
        )
      }

      return <Number[]>JSON.parse(versions)
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
