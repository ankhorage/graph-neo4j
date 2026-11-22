import type {
  GraphPersistenceMutation,
  StoredGraphEdge,
  StoredGraphNode,
} from '@ankhorage/graph/persistence';

import { createNeo4jStorage, getNeo4jNodeLabel, quoteNeo4jIdentifier } from './neo4jStorage.js';
import type { Neo4jReply } from './neo4jTransport.js';

interface Neo4jTransaction {
  readonly queryAsync: (query: {
    readonly statement: string;
    readonly parameters?: Readonly<Record<string, unknown>>;
  }) => Promise<Neo4jReply>;
}

/*** Apply one ordered mutation batch and return the exact number of state-changing entries. */
export async function executeNeo4jMutationAsync(
  transaction: Neo4jTransaction,
  mutations: readonly GraphPersistenceMutation[],
): Promise<number> {
  const deletedEdgeIds = new Set(
    mutations.flatMap((mutation) => (mutation.kind === 'deleteEdge' ? [mutation.edgeId] : [])),
  );
  const orderedMutations = [
    ...mutations.filter((mutation) => mutation.kind === 'deleteEdge'),
    ...mutations.filter((mutation) => mutation.kind !== 'deleteEdge'),
  ];
  let mutationCount = 0;
  for (const mutation of orderedMutations) {
    if (await executeMutationAsync(transaction, mutation, deletedEdgeIds)) mutationCount += 1;
  }
  return mutationCount;
}

/*** Route a validated portable mutation to its exact provider implementation. */
async function executeMutationAsync(
  transaction: Neo4jTransaction,
  mutation: GraphPersistenceMutation,
  deletedEdgeIds: ReadonlySet<string>,
): Promise<boolean> {
  if (mutation.kind === 'upsertNode') return upsertNodeAsync(transaction, mutation.node);
  if (mutation.kind === 'upsertEdge') return upsertEdgeAsync(transaction, mutation.edge);
  if (mutation.kind === 'deleteEdge') return deleteEdgeAsync(transaction, mutation.edgeId);
  return deleteNodeAsync(transaction, mutation.nodeId, deletedEdgeIds);
}

/*** Replace node properties and application labels only when its canonical stored representation changed. */
async function upsertNodeAsync(
  transaction: Neo4jTransaction,
  node: StoredGraphNode,
): Promise<boolean> {
  const label = getNeo4jNodeLabel();
  const existing = await transaction.queryAsync({
    statement: 'MATCH (n:' + label + ' {ankhId: $id}) RETURN n.ankhData, labels(n)',
    parameters: { id: node.id },
  });
  const storage = createNeo4jStorage(node.data);
  const [existingNode] = existing.values;
  if (existingNode !== undefined && isEqualNode(existingNode, node, storage.ankhData)) return false;
  const oldLabels = existingNode === undefined ? [] : getLabels(existingNode);
  const statement = getNodeWriteStatement(node.labels, oldLabels);
  await transaction.queryAsync({
    statement,
    parameters: {
      id: node.id,
      data: storage.ankhData,
      properties: { ankhId: node.id, ankhData: storage.ankhData, ...storage.properties },
    },
  });
  return true;
}

/*** Preserve global edge identity by rejecting a change to its endpoints or native relationship type. */
async function upsertEdgeAsync(
  transaction: Neo4jTransaction,
  edge: StoredGraphEdge,
): Promise<boolean> {
  const label = getNeo4jNodeLabel();
  const existing = await transaction.queryAsync({
    statement:
      'MATCH (a:' +
      label +
      ')-[e {ankhId: $id}]->(b:' +
      label +
      ') RETURN a.ankhId, b.ankhId, type(e), e.ankhData',
    parameters: { id: edge.id },
  });
  const [first] = existing.values;
  const storage = createNeo4jStorage(edge.data);
  if (first !== undefined) return updateExistingEdgeAsync(transaction, first, edge, storage);
  const created = await transaction.queryAsync({
    statement:
      'MATCH (a:' +
      label +
      ' {ankhId: $source}) MATCH (b:' +
      label +
      ' {ankhId: $target}) CREATE (a)-[e:' +
      quoteNeo4jIdentifier(edge.type) +
      ' {ankhId: $id}]->(b) SET e = $properties RETURN e.ankhId',
    parameters: {
      source: edge.source,
      target: edge.target,
      id: edge.id,
      properties: { ankhId: edge.id, ankhData: storage.ankhData, ...storage.properties },
    },
  });
  if (created.values.length !== 1)
    throw new Error('Graph edge endpoints are missing or ambiguous.');
  return true;
}

/*** Update data on a stable existing relationship without ever recreating its identity. */
async function updateExistingEdgeAsync(
  transaction: Neo4jTransaction,
  row: readonly unknown[],
  edge: StoredGraphEdge,
  storage: ReturnType<typeof createNeo4jStorage>,
): Promise<boolean> {
  const [source, target, type, data] = row;
  if (source !== edge.source || target !== edge.target || type !== edge.type)
    throw new Error('Graph edge identity cannot change endpoints or type.');
  if (data === storage.ankhData) return false;
  await transaction.queryAsync({
    statement: 'MATCH ()-[e {ankhId: $id}]->() SET e = $properties RETURN e.ankhId',
    parameters: {
      id: edge.id,
      properties: { ankhId: edge.id, ankhData: storage.ankhData, ...storage.properties },
    },
  });
  return true;
}

/*** Delete an edge idempotently. */
async function deleteEdgeAsync(transaction: Neo4jTransaction, edgeId: string): Promise<boolean> {
  const result = await transaction.queryAsync({
    statement: 'MATCH ()-[e {ankhId: $id}]->() DELETE e RETURN $id',
    parameters: { id: edgeId },
  });
  return result.values.length === 1;
}

/*** Refuse a node delete until every attached edge is also deleted by the same batch. */
async function deleteNodeAsync(
  transaction: Neo4jTransaction,
  nodeId: string,
  deletedEdgeIds: ReadonlySet<string>,
): Promise<boolean> {
  const label = getNeo4jNodeLabel();
  const result = await transaction.queryAsync({
    statement:
      'MATCH (n:' +
      label +
      ' {ankhId: $id}) OPTIONAL MATCH (n)-[e]-() WHERE NOT e.ankhId IN $deletedEdgeIds WITH n, count(e) AS remaining WHERE remaining = 0 DELETE n RETURN $id',
    parameters: { id: nodeId, deletedEdgeIds: [...deletedEdgeIds] },
  });
  if (result.values.length === 1) return true;
  const attached = await transaction.queryAsync({
    statement: 'MATCH (n:' + label + ' {ankhId: $id})-[e]-() RETURN e.ankhId LIMIT 1',
    parameters: { id: nodeId },
  });
  if (attached.values.length > 0)
    throw new Error('Graph node delete requires deleting attached edges in the same batch.');
  return false;
}

/*** Compare the canonical data and labels rather than provider implementation fields. */
function isEqualNode(row: readonly unknown[], node: StoredGraphNode, data: string): boolean {
  return row[0] === data && JSON.stringify(getLabels(row)) === JSON.stringify(node.labels);
}

/*** Extract labels while excluding the adapter-owned storage label. */
function getLabels(row: readonly unknown[]): readonly string[] {
  const [, labels] = row;
  return Array.isArray(labels)
    ? labels.filter(
        (label): label is string => typeof label === 'string' && label !== getNeo4jNodeLabel(),
      )
    : [];
}

/*** Produce a literal-safe label replacement statement from contract labels. */
function getNodeWriteStatement(labels: readonly string[], oldLabels: readonly string[]): string {
  const label = getNeo4jNodeLabel();
  const removals = oldLabels
    .filter((oldLabel) => !labels.includes(oldLabel))
    .map((oldLabel) => ' REMOVE n:' + quoteNeo4jIdentifier(oldLabel))
    .join('');
  return (
    'MERGE (n:' +
    label +
    ' {ankhId: $id})' +
    removals +
    ' SET n:' +
    labels.map(quoteNeo4jIdentifier).join(':') +
    ' SET n = $properties RETURN n.ankhId'
  );
}
