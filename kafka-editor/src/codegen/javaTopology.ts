import type { ProjectDocument, ProjectNode } from '../model/project'
import { isPredicate, isValueExpression } from '../expressions/types'
import type { ValueMappingEntry } from '../expressions/types'
import type { AggregateField } from '../model/aggregations'
import { expressionToJava, predicateToJava } from './javaExpressions'
import {
  aggregateBlock,
  joinBlock,
  mapBlock,
  mapValuesBlock,
  reduceExpression,
} from './javaOperators'
import { topoSort } from './topoSort'
import { computeOutputTypes } from './outputTypes'

/** Statement indent (method body) and branch-continuation indent. */
const I = '        '
const B = '            '

function classWrapper(body: string): string {
  return `package org.acme.kafka;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

/**
 * Generated Kafka Streams topology.
 *
 * The editor's structured logic is compiled to Java. Values reshaped by
 * map / aggregate / join flow as Map<String, Object>; review the // TODO
 * markers for the few operators left as stubs.
 */
@ApplicationScoped
public class TopologyProducer {

    @Produces
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

${body}

        return builder.build();
    }
}
`
}

/** Generate the Quarkus topology-producer class for a project. */
export function generateTopology(doc: ProjectDocument): string {
  const sorted = topoSort(doc.nodes, doc.edges)
  if (!sorted) {
    return classWrapper(
      `${I}// Cannot generate — the topology contains a cycle.`,
    )
  }
  if (sorted.length === 0) {
    return classWrapper(`${I}// The canvas is empty.`)
  }

  const nodeById = new Map(doc.nodes.map((n) => [n.id, n]))
  const outputTypes = computeOutputTypes(doc)

  const topicName = (node: ProjectNode): string => {
    const topic = doc.catalog.topics.find((t) => t.id === node.config.topicId)
    return topic ? topic.name : 'TODO-topic'
  }

  const used = new Set<string>()
  const varByNode = new Map<string, string>()
  const nameFor = (node: ProjectNode): string => {
    const label =
      typeof node.config.label === 'string' && node.config.label
        ? node.config.label
        : (node.type ?? 'node')
    const words = label.replace(/[^a-zA-Z0-9]+/g, ' ').trim().split(' ')
    let base = words
      .map((w, i) =>
        i === 0
          ? w.toLowerCase()
          : w.charAt(0).toUpperCase() + w.slice(1).toLowerCase(),
      )
      .join('')
    if (!base || !/^[a-zA-Z]/.test(base)) base = `node${base}`
    let name = base
    let suffix = 2
    while (used.has(name)) name = `${base}${suffix++}`
    used.add(name)
    return name
  }

  const inVar = (nodeId: string, handle?: string): string => {
    const edge = doc.edges.find(
      (e) => e.target === nodeId && (handle ? e.targetHandle === handle : true),
    )
    if (!edge) return 'TODO_input'
    const sourceVar = varByNode.get(edge.source)
    if (!sourceVar) return 'TODO_input'
    // A branch source exposes its outputs through the split() result map.
    if (nodeById.get(edge.source)?.type === 'branch') {
      return `${sourceVar}.get("b_${edge.sourceHandle}")`
    }
    return sourceVar
  }

  const entriesOf = (
    config: Record<string, unknown>,
    key: string,
  ): ValueMappingEntry[] =>
    Array.isArray(config[key]) ? (config[key] as ValueMappingEntry[]) : []

  const lines: string[] = []

  for (const node of sorted) {
    const config = node.config
    const predicate = isPredicate(config.predicate)
      ? predicateToJava(config.predicate)
      : 'true'
    const keyExpr = isValueExpression(config.keyExpression)
      ? expressionToJava(config.keyExpression)
      : 'key'
    let varName: string | null = nameFor(node)
    let code: string

    switch (node.type) {
      case 'source':
      case 'connect-source':
        code = `${I}var ${varName} = builder.stream("${topicName(node)}");`
        break
      case 'table-source':
        code = `${I}var ${varName} = builder.table("${topicName(node)}");`
        break
      case 'sink':
      case 'connect-sink':
        code = `${I}${inVar(node.id)}.to("${topicName(node)}");`
        varName = null
        break
      case 'filter':
        code = `${I}var ${varName} = ${inVar(node.id)}.filter((key, value) -> ${predicate});`
        break
      case 'filter-not':
        code = `${I}var ${varName} = ${inVar(node.id)}.filterNot((key, value) -> ${predicate});`
        break
      case 'map-values':
        code = mapValuesBlock(
          varName,
          inVar(node.id),
          entriesOf(config, 'valueMapping'),
          outputTypes.get(node.id),
        )
        break
      case 'map':
        code = mapBlock(
          varName,
          inVar(node.id),
          keyExpr,
          entriesOf(config, 'valueMapping'),
          outputTypes.get(node.id),
        )
        break
      case 'select-key':
        code = `${I}var ${varName} = ${inVar(node.id)}.selectKey((key, value) -> ${keyExpr});`
        break
      case 'peek':
        code = `${I}var ${varName} = ${inVar(node.id)}.peek((key, value) -> { /* TODO: side effect */ });`
        break
      case 'foreach':
        code = `${I}${inVar(node.id)}.foreach((key, value) -> { /* TODO: side effect */ });`
        varName = null
        break
      case 'flat-map':
        code = `${I}var ${varName} = ${inVar(node.id)}.flatMap((key, value) -> /* TODO */ java.util.List.of());`
        break
      case 'flat-map-values':
        code = `${I}var ${varName} = ${inVar(node.id)}.flatMapValues(value -> /* TODO */ java.util.List.of());`
        break
      case 'to-stream':
        code = `${I}var ${varName} = ${inVar(node.id)}.toStream();`
        break
      case 'to-table':
        code = `${I}var ${varName} = ${inVar(node.id)}.toTable();`
        break
      case 'repartition':
        code = `${I}var ${varName} = ${inVar(node.id)}.repartition();`
        break
      case 'group-by-key':
        code = `${I}var ${varName} = ${inVar(node.id)}.groupByKey();`
        break
      case 'group-by':
        code = `${I}var ${varName} = ${inVar(node.id)}.groupBy((key, value) -> ${keyExpr});`
        break
      case 'count':
        code = `${I}var ${varName} = ${inVar(node.id)}.count();`
        break
      case 'reduce':
        code = `${I}var ${varName} = ${inVar(node.id)}.reduce((value1, value2) -> ${reduceExpression(config.reducer)});`
        break
      case 'aggregate':
        code = aggregateBlock(
          varName,
          inVar(node.id),
          Array.isArray(config.aggregation)
            ? (config.aggregation as AggregateField[])
            : [],
        )
        break
      case 'stream-stream-join': {
        const ms =
          typeof config.windowSizeMs === 'number' ? config.windowSizeMs : 60000
        const window = `org.apache.kafka.streams.kstream.JoinWindows.ofTimeDifferenceWithNoGrace(java.time.Duration.ofMillis(${ms}))`
        code = joinBlock(
          varName,
          inVar(node.id, 'left'),
          inVar(node.id, 'right'),
          window,
          entriesOf(config, 'valueJoiner'),
          outputTypes.get(node.id),
        )
        break
      }
      case 'stream-table-join':
      case 'table-table-join':
        code = joinBlock(
          varName,
          inVar(node.id, 'left'),
          inVar(node.id, 'right'),
          null,
          entriesOf(config, 'valueJoiner'),
          outputTypes.get(node.id),
        )
        break
      case 'branch': {
        const branches = Array.isArray(config.branches) ? config.branches : []
        const branchLines = branches.map((b) => {
          const pred = isPredicate(b?.predicate)
            ? predicateToJava(b.predicate)
            : 'true'
          return `${B}.branch((key, value) -> ${pred}, org.apache.kafka.streams.kstream.Branched.as("b_${b?.id}"))`
        })
        code = [
          `${I}var ${varName} = ${inVar(node.id)}.split()`,
          ...branchLines,
          `${B}.noDefaultBranch();`,
        ].join('\n')
        break
      }
      default:
        code = `${I}var ${varName} = ${inVar(node.id)}; // TODO: "${node.type}" is not yet supported by codegen`
        break
    }

    if (varName) varByNode.set(node.id, varName)
    lines.push(code)
  }

  return classWrapper(lines.join('\n'))
}
