/**
 * A minimal ZIP archive encoder — "stored" (no compression), enough to bundle
 * the generated text files for download. Avoids a third-party dependency.
 */

const CRC_TABLE = (() => {
  const table = new Uint32Array(256)
  for (let n = 0; n < 256; n++) {
    let c = n
    for (let k = 0; k < 8; k++) {
      c = (c & 1) === 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1
    }
    table[n] = c >>> 0
  }
  return table
})()

function crc32(bytes: Uint8Array): number {
  let crc = 0xffffffff
  for (let i = 0; i < bytes.length; i++) {
    crc = (CRC_TABLE[(crc ^ bytes[i]) & 0xff] ^ (crc >>> 8)) >>> 0
  }
  return (crc ^ 0xffffffff) >>> 0
}

export interface ZipEntry {
  path: string
  content: string
}

/** Build a ZIP archive (stored / uncompressed) from a set of text files. */
export function createZip(entries: ZipEntry[]): Uint8Array<ArrayBuffer> {
  const encoder = new TextEncoder()
  const local: number[] = []
  const central: number[] = []

  const u16 = (arr: number[], v: number) => {
    arr.push(v & 0xff, (v >>> 8) & 0xff)
  }
  const u32 = (arr: number[], v: number) => {
    arr.push(
      v & 0xff,
      (v >>> 8) & 0xff,
      (v >>> 16) & 0xff,
      (v >>> 24) & 0xff,
    )
  }
  const append = (arr: number[], data: Uint8Array) => {
    for (let i = 0; i < data.length; i++) arr.push(data[i])
  }

  for (const entry of entries) {
    const name = encoder.encode(entry.path)
    const data = encoder.encode(entry.content)
    const crc = crc32(data)
    const localOffset = local.length

    // Local file header.
    u32(local, 0x04034b50)
    u16(local, 20) // version needed
    u16(local, 0) // flags
    u16(local, 0) // method: stored
    u16(local, 0) // mod time
    u16(local, 0) // mod date
    u32(local, crc)
    u32(local, data.length) // compressed size
    u32(local, data.length) // uncompressed size
    u16(local, name.length)
    u16(local, 0) // extra length
    append(local, name)
    append(local, data)

    // Central directory header.
    u32(central, 0x02014b50)
    u16(central, 20) // version made by
    u16(central, 20) // version needed
    u16(central, 0) // flags
    u16(central, 0) // method
    u16(central, 0) // mod time
    u16(central, 0) // mod date
    u32(central, crc)
    u32(central, data.length)
    u32(central, data.length)
    u16(central, name.length)
    u16(central, 0) // extra length
    u16(central, 0) // comment length
    u16(central, 0) // disk number
    u16(central, 0) // internal attributes
    u32(central, 0) // external attributes
    u32(central, localOffset)
    append(central, name)
  }

  // End of central directory record.
  const eocd: number[] = []
  u32(eocd, 0x06054b50)
  u16(eocd, 0) // this disk
  u16(eocd, 0) // disk with central directory
  u16(eocd, entries.length) // entries on this disk
  u16(eocd, entries.length) // total entries
  u32(eocd, central.length) // central directory size
  u32(eocd, local.length) // central directory offset
  u16(eocd, 0) // comment length

  return Uint8Array.from([...local, ...central, ...eocd])
}
