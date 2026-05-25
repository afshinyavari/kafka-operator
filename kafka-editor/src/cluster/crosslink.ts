import { newTopic } from '../model/catalog'
import type { Catalog, TopicDef } from '../model/catalog'

/** Pure helpers bridging the live cluster and the design-time editor. */

/** Build a design-time catalog TopicDef from a live cluster topic. */
export function liveTopicToDef(name: string, partitionCount: number): TopicDef {
  return { ...newTopic(name), partitions: Math.max(1, partitionCount) }
}

/** Whether the catalog already holds a topic with this name. */
export function catalogHasTopic(catalog: Catalog, name: string): boolean {
  return catalog.topics.some((t) => t.name === name)
}
