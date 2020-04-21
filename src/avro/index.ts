import avsc from 'avsc'

const toBytesInt32 = (n: number) => {
  const arr = new ArrayBuffer(4) // an Int32 takes 4 bytes
  const view = new DataView(arr)
  view.setUint32(0, n, false) // byteOffset = 0; litteEndian = false
  return new Buffer(arr)
}

export const encode = (
  schemaId: number,
  schema: avsc.Type,
  data: object | string,
): Buffer => {
  const magic = Buffer.from([0])
  const payload = schema.toBuffer(data)
  return Buffer.concat([magic, toBytesInt32(schemaId), payload])
}

export const decode = (data: Buffer): [number, Buffer] => {
  const schemaIdBytes = data.slice(1, 5)
  const schemaIdHexStr = schemaIdBytes.toString('hex')
  const schemaId = parseInt(schemaIdHexStr, 16)
  const payload = data.slice(5)
  return [schemaId, payload]
}
