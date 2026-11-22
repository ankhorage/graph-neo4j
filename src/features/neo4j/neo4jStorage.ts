import type { GraphStorageData, StoredGraphNode } from '@ankhorage/graph/persistence';

const nodeLabel = 'AnkhGraphNode';
const scalarPrefix = 'ankhProperty_';

/*** Convert portable data into exact Neo4j properties while keeping scalar fields natively filterable. */
export function createNeo4jStorage(data: GraphStorageData): {
  readonly ankhData: string;
  readonly properties: Readonly<Record<string, string | boolean | number>>;
} {
  return {
    ankhData: JSON.stringify(data),
    properties: Object.fromEntries(
      Object.entries(data.properties).map(([key, value]) => [scalarPrefix + key, value]),
    ),
  };
}

/*** Rehydrate one node returned by a Neo4j query and preserve its native labels. */
export function readNeo4jNode(row: readonly unknown[]): StoredGraphNode {
  const [idValue, labelsValue, dataValue] = row;
  const id = readString(idValue, 'node identity');
  const labels = readLabels(labelsValue);
  return { id, labels, data: readStorage(dataValue) };
}

/*** Decode storage data after transport validation without interpreting application documents. */
export function readNeo4jStorage(value: unknown): GraphStorageData {
  return readStorage(value);
}

/*** Quote arbitrary contract labels and types as Cypher identifiers rather than interpolating tokens. */
export function quoteNeo4jIdentifier(value: string): string {
  return '`' + value.replaceAll('`', '``') + '`';
}

/*** Build the required persistent node label. */
export function getNeo4jNodeLabel(): string {
  return nodeLabel;
}

/*** Decode exactly the JSON data written by this adapter. */
function readStorage(value: unknown): GraphStorageData {
  if (typeof value !== 'string') throw new Error('Neo4j graph property payload is invalid.');
  try {
    const parsed: unknown = JSON.parse(value);
    if (isRecord(parsed) && isRecord(parsed.properties))
      return parsed as unknown as GraphStorageData;
  } catch {
    // The opaque payload remains intentionally absent from errors.
  }
  throw new Error('Neo4j graph property payload is invalid.');
}

/*** Restrict labels to the non-empty strings guaranteed by the storage contract. */
function readLabels(value: unknown): readonly [string, ...string[]] {
  if (!Array.isArray(value)) throw new Error('Neo4j returned invalid node labels.');
  const labels = value.filter(
    (label): label is string => typeof label === 'string' && label !== nodeLabel,
  );
  if (labels.length === 0) throw new Error('Neo4j returned a node without storage labels.');
  return labels as [string, ...string[]];
}

/*** Validate protocol records locally. */
function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

/*** Narrow a provider scalar before returning it as a durable graph identifier. */
function readString(value: unknown, kind: string): string {
  if (typeof value !== 'string') throw new Error('Neo4j returned an invalid ' + kind + '.');
  return value;
}
