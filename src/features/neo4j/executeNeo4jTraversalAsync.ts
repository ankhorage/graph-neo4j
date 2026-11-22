import type {
  GraphPersistenceTraversalOperation,
  GraphPersistenceTraversalResult,
  StoredGraphEdge,
} from '@ankhorage/graph/persistence';

import { getNeo4jNodeLabel, readNeo4jNode, readNeo4jStorage } from './neo4jStorage.js';
import type { Neo4jReply } from './neo4jTransport.js';

interface Neo4jReader {
  readonly queryAsync: (query: {
    readonly statement: string;
    readonly parameters?: Readonly<Record<string, unknown>>;
  }) => Promise<Neo4jReply>;
}

/*** Execute a bounded live traversal with an opaque scope-bound cursor. */
export async function executeNeo4jTraversalAsync(
  reader: Neo4jReader,
  operation: GraphPersistenceTraversalOperation,
): Promise<GraphPersistenceTraversalResult> {
  if (operation.startNodeIds.length === 0)
    return { kind: 'traversal', nodes: [], edges: [], cursorConsistency: 'live' };
  if (operation.maxDepth === 0) {
    return {
      kind: 'traversal',
      nodes: await readNodesAsync(reader, operation.startNodeIds),
      edges: [],
      cursorConsistency: 'live',
    };
  }
  const offset = decodeTraversalCursor(operation);
  const result = await reader.queryAsync({
    statement: createTraversalStatement(operation),
    parameters: {
      ids: operation.startNodeIds,
      types: operation.types ?? [],
      anyType: operation.types === undefined,
      offset,
      limit: operation.pageSize + 1,
    },
  });
  const records = result.values;
  const edges = records.slice(0, operation.pageSize).map(readEdge);
  const nodes = await readNodesAsync(reader, [
    ...new Set([...operation.startNodeIds, ...edges.flatMap((edge) => [edge.source, edge.target])]),
  ]);
  const nextCursor =
    records.length > operation.pageSize
      ? encodeTraversalCursor(operation, offset + operation.pageSize)
      : undefined;
  return {
    kind: 'traversal',
    nodes,
    edges,
    cursorConsistency: 'live',
    ...(nextCursor === undefined ? {} : { nextCursor }),
  };
}

/*** Look up stable node IDs without treating absent values as provider errors. */
export async function readNeo4jNodesAsync(reader: Neo4jReader, ids: readonly string[]) {
  return readNodesAsync(reader, ids);
}

/*** Return requested nodes in caller order after a single provider lookup. */
async function readNodesAsync(reader: Neo4jReader, ids: readonly string[]) {
  if (ids.length === 0) return [];
  const label = getNeo4jNodeLabel();
  const result = await reader.queryAsync({
    statement:
      'MATCH (n:' + label + ') WHERE n.ankhId IN $ids RETURN n.ankhId, labels(n), n.ankhData',
    parameters: { ids },
  });
  const found = new Map(
    result.values.map((row) => {
      const node = readNeo4jNode(row);
      return [node.id, node];
    }),
  );
  return ids.flatMap((id) => {
    const node = found.get(id);
    return node === undefined ? [] : [node];
  });
}

/*** Build a directed Cypher pattern while retaining native relationship-type filtering. */
function createTraversalStatement(operation: GraphPersistenceTraversalOperation): string {
  const arrow =
    operation.direction === 'incoming'
      ? '<-[*1..' + operation.maxDepth + ']-'
      : operation.direction === 'outgoing'
        ? '-[*1..' + operation.maxDepth + ']->'
        : '-[*1..' + operation.maxDepth + ']-';
  return (
    'MATCH (s:' +
    getNeo4jNodeLabel() +
    ') WHERE s.ankhId IN $ids MATCH p=(s)' +
    arrow +
    '() WHERE ALL(r IN relationships(p) WHERE $anyType OR type(r) IN $types) UNWIND relationships(p) AS e WITH DISTINCT e ORDER BY e.ankhIdentityId SKIP $offset LIMIT $limit RETURN e.ankhIdentityId, startNode(e).ankhId, endNode(e).ankhId, type(e), e.ankhData'
  );
}

/*** Decode one native relationship as the canonical stored-edge refinement. */
function readEdge(row: readonly unknown[]): StoredGraphEdge {
  const [id, source, target, type, data] = row;
  if (
    typeof id !== 'string' ||
    typeof source !== 'string' ||
    typeof target !== 'string' ||
    typeof type !== 'string'
  )
    throw new Error('Neo4j returned an invalid graph edge.');
  return { id, source, target, type, data: readNeo4jStorage(data) };
}

/*** Encode page position together with every behavior-affecting traversal field. */
function encodeTraversalCursor(
  operation: GraphPersistenceTraversalOperation,
  offset: number,
): string {
  return Buffer.from(
    JSON.stringify({
      version: 1,
      offset,
      ids: operation.startNodeIds,
      direction: operation.direction,
      maxDepth: operation.maxDepth,
      pageSize: operation.pageSize,
      types: operation.types ?? null,
    }),
  ).toString('base64url');
}

/*** Reject stale or cross-scope cursors before any provider query is issued. */
function decodeTraversalCursor(operation: GraphPersistenceTraversalOperation): number {
  if (operation.cursor === undefined) return 0;
  try {
    const value: unknown = JSON.parse(Buffer.from(operation.cursor, 'base64url').toString('utf8'));
    if (isMatchingCursor(value, operation)) return value.offset;
  } catch {
    // All invalid cursors intentionally have one public failure mode.
  }
  throw new Error('Invalid traversal cursor.');
}

/*** Compare an untrusted cursor with its exact request scope. */
function isMatchingCursor(
  value: unknown,
  operation: GraphPersistenceTraversalOperation,
): value is { readonly offset: number } {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return false;
  const cursor = value as Record<string, unknown>;
  return (
    cursor.version === 1 &&
    typeof cursor.offset === 'number' &&
    Number.isSafeInteger(cursor.offset) &&
    cursor.offset >= 0 &&
    JSON.stringify(cursor.ids) === JSON.stringify(operation.startNodeIds) &&
    cursor.direction === operation.direction &&
    cursor.maxDepth === operation.maxDepth &&
    cursor.pageSize === operation.pageSize &&
    JSON.stringify(cursor.types) === JSON.stringify(operation.types ?? null)
  );
}
