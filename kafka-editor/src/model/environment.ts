/**
 * The project environment — the Kafka cluster and schema registry the topology
 * runs against. Saved in the project document. (Environment *variables* are
 * kept local-only — see `src/run/envStore.ts` — because they hold secrets.)
 */
export interface Environment {
  bootstrapServers: string
  schemaRegistryUrl: string
}

export function emptyEnvironment(): Environment {
  return {
    bootstrapServers: 'localhost:9092',
    schemaRegistryUrl: 'http://localhost:8085',
  }
}
