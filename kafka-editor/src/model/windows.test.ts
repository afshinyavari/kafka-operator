import { describe, it, expect } from 'vitest'
import { defaultWindow, describeWindow, isWindowConfig } from './windows'

describe('window helpers', () => {
  it('defaultWindow is an unwindowed aggregation', () => {
    expect(defaultWindow().type).toBe('none')
  })

  it('isWindowConfig recognizes a window config', () => {
    expect(isWindowConfig(defaultWindow())).toBe(true)
    expect(isWindowConfig({ type: 'tumbling', sizeMs: 1000 })).toBe(true)
    expect(isWindowConfig('tumbling')).toBe(false)
    expect(isWindowConfig({ type: 'tumbling' })).toBe(false)
  })

  it('describeWindow renders each window type', () => {
    expect(describeWindow({ type: 'none', sizeMs: 0 })).toBe('unwindowed')
    expect(describeWindow({ type: 'tumbling', sizeMs: 60000 })).toBe(
      'tumbling 60000ms',
    )
    expect(
      describeWindow({ type: 'hopping', sizeMs: 60000, advanceMs: 10000 }),
    ).toBe('hopping 60000ms / 10000ms')
    expect(describeWindow({ type: 'session', sizeMs: 30000 })).toBe(
      'session gap 30000ms',
    )
  })
})
