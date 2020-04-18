import axios from 'axios'

export class SchemaRegistry {
  url: string

  constructor(url: string) {
    this.url = url
  }

  getLatestBySchemaId = async (schemaId: number) => {
    const url = `${this.url}/schemas/ids/${schemaId}`

    try {
      const response = await axios.get(url)

      const schema = response.data?.schema
      if (!schema) {
        throw Error('SchemaRegistry.getBySchemaId: no schema returned')
      }

      return JSON.parse(schema)
    } catch (e) {
      console.error('SchemaRegistry.getLatestBySchemaId: error', e)
      throw e
    }
  }

  getLatestBySubject = async (subject: string) => {
    try {
      const versions = await this.getVersionsBySubject(subject)
      const latest = versions.slice(-1)[0]
      const url = `${this.url}/schemas/subjects/${subject}/versions/${latest}/schema`

      const response = await axios.get(url)

      const schema = response.data
      if (!schema) {
        throw Error('SchemaRegistry.getBySchemaId: no schema returned')
      }

      return JSON.parse(schema)
    } catch (e) {
      console.error('SchemaRegistry.getLatestBySubject: error', e)
      throw e
    }
  }

  getVersionsBySubject = async (subject: string) => {
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

      return JSON.parse(versions)
    } catch (e) {
      console.error('SchemaRegistry.getVersionsBySubject: error', e)
      throw e
    }
  }

  register = async (subject: string, schema: object, isKeySchema: boolean) => {
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
