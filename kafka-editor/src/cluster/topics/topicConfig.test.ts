import { describe, expect, it } from 'vitest'
import { computeConfigDiff, parseConfigText } from './topicConfig'

describe('computeConfigDiff', () => {
  it('returns nothing when no value changed', () => {
    expect(
      computeConfigDiff([
        { id: '1', name: 'retention.ms', value: '1000', original: '1000' },
      ]),
    ).toEqual({})
  })

  it('includes a changed value', () => {
    expect(
      computeConfigDiff([
        { id: '1', name: 'retention.ms', value: '2000', original: '1000' },
      ]),
    ).toEqual({ 'retention.ms': '2000' })
  })

  it('treats a cleared value as a reset (blank)', () => {
    expect(
      computeConfigDiff([
        { id: '1', name: 'retention.ms', value: '', original: '1000' },
      ]),
    ).toEqual({ 'retention.ms': '' })
  })

  it('includes a new key with a value', () => {
    expect(
      computeConfigDiff([
        { id: '1', name: 'cleanup.policy', value: 'compact', original: '' },
      ]),
    ).toEqual({ 'cleanup.policy': 'compact' })
  })

  it('ignores blank-named rows and trims the key', () => {
    expect(
      computeConfigDiff([
        { id: '1', name: '  ', value: 'x', original: '' },
        { id: '2', name: ' cleanup.policy ', value: 'compact', original: '' },
      ]),
    ).toEqual({ 'cleanup.policy': 'compact' })
  })
})

describe('parseConfigText', () => {
  it('parses key=value lines', () => {
    expect(
      parseConfigText('retention.ms=1000\ncleanup.policy=compact'),
    ).toEqual({ 'retention.ms': '1000', 'cleanup.policy': 'compact' })
  })

  it('skips blank lines and comments', () => {
    expect(parseConfigText('\n# a comment\nretention.ms=1000\n')).toEqual({
      'retention.ms': '1000',
    })
  })

  it('trims whitespace and keeps = inside values', () => {
    expect(parseConfigText('  key  =  a=b  ')).toEqual({ key: 'a=b' })
  })

  it('ignores lines without =', () => {
    expect(parseConfigText('garbage')).toEqual({})
  })
})
