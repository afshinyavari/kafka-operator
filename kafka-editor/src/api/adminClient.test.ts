import { describe, expect, it } from 'vitest'
import { connQuery } from './adminClient'

describe('connQuery', () => {
  it('encodes set fields and omits blank ones', () => {
    const qs = connQuery({
      bootstrapServers: 'host:9092',
      schemaRegistryUrl: '',
      connectUrl: '',
    })
    expect(qs).toBe('bootstrap=host%3A9092')
  })

  it('includes registry and connect when present', () => {
    const params = new URLSearchParams(
      connQuery({
        bootstrapServers: 'h:9092',
        schemaRegistryUrl: 'http://r',
        connectUrl: 'http://c',
      }),
    )
    expect(params.get('bootstrap')).toBe('h:9092')
    expect(params.get('registry')).toBe('http://r')
    expect(params.get('connect')).toBe('http://c')
  })

  it('produces an empty string when nothing is set', () => {
    expect(
      connQuery({
        bootstrapServers: '',
        schemaRegistryUrl: '',
        connectUrl: '',
      }),
    ).toBe('')
  })
})
