import { describe, it, expect } from 'vitest'
import { createZip } from './zip'

describe('createZip', () => {
  it('starts with the local-file-header signature', () => {
    const zip = createZip([{ path: 'a.txt', content: 'hello' }])
    expect([zip[0], zip[1], zip[2], zip[3]]).toEqual([0x50, 0x4b, 0x03, 0x04])
  })

  it('contains an end-of-central-directory record', () => {
    const zip = createZip([{ path: 'a.txt', content: 'hello' }])
    const hasEocd = Array.from(zip).some(
      (_, i) =>
        zip[i] === 0x50 &&
        zip[i + 1] === 0x4b &&
        zip[i + 2] === 0x05 &&
        zip[i + 3] === 0x06,
    )
    expect(hasEocd).toBe(true)
  })

  it('embeds each file path and its content', () => {
    const zip = createZip([
      { path: 'src/Main.java', content: 'class Main {}' },
      { path: 'pom.xml', content: '<project/>' },
    ])
    const text = new TextDecoder().decode(zip)
    expect(text).toContain('src/Main.java')
    expect(text).toContain('class Main {}')
    expect(text).toContain('pom.xml')
    expect(text).toContain('<project/>')
  })
})
