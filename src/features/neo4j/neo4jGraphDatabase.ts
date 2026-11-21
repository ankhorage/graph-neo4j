import type {
  GraphDatabaseCapability,
  GraphDatabaseOperation,
  GraphDatabaseOperationResult,
  GraphDatabasePort,
  GraphDatabaseSession,
  GraphEdge,
  GraphNode,
  GraphMutation,
  GraphPropertyObject,
  GraphTraversalOperation,
} from '@ankhorage/graphdb';

/*** Server-only settings for one Neo4j Query API v2 backend. */
export interface Neo4jGraphDatabaseOptions {
  readonly endpoint: string;
  readonly username: string;
  readonly password: string;
  readonly database?: string;
  readonly timeoutMs?: number;
  readonly fetcher?: typeof fetch;
}

type QueryBody = { readonly statement?: string; readonly parameters?: Readonly<Record<string, unknown>> };
type QueryReply = {
  readonly data?: { readonly fields?: readonly string[]; readonly values?: readonly (readonly unknown[])[] };
  readonly transaction?: { readonly id?: string };
  readonly errors?: readonly { readonly code?: string }[];
};
type QueryResponse = { readonly reply: QueryReply; readonly affinity?: string };
type MutationContext = { readonly txId: string; readonly affinity?: string };

const nodeLabel = 'AnkhGraphNode';
const edgeLabel = 'ANKH_EDGE';
const capabilities: readonly GraphDatabaseCapability[] = ['read', 'mutate', 'traverse'];
const maxDepth = 4;
const maxPageSize = 200;
const maxOffset = 100000;

/*** A Neo4j provider for the published portable GraphDatabasePort. */
export function createNeo4jGraphDatabasePort(options: Neo4jGraphDatabaseOptions): GraphDatabasePort {
  const client = new Neo4jClient(options);
  return { openAsync: async () => createSession(client) };
}

/*** Explicit, idempotent schema preparation; never occurs as an implicit read side effect. */
export async function prepareNeo4jGraphDatabaseAsync(options: Neo4jGraphDatabaseOptions): Promise<void> {
  const client = new Neo4jClient(options);
  await client.query('POST', '', {
    statement: 'CREATE CONSTRAINT ankh_graph_node_id IF NOT EXISTS FOR (n:' + nodeLabel + ') REQUIRE n.ankhId IS UNIQUE',
  });
  await client.query('POST', '', {
    statement: 'CREATE CONSTRAINT ankh_graph_edge_id IF NOT EXISTS FOR ()-[e:' + edgeLabel + ']-() REQUIRE e.ankhId IS UNIQUE',
  });
}

/*** Provides session lifecycle without hiding durable side effects. */
function createSession(client: Neo4jClient): GraphDatabaseSession {
  let closed = false;
  return {
    capabilities,
    executeAsync: async (operation: GraphDatabaseOperation): Promise<GraphDatabaseOperationResult> => {
      if (closed) throw new Error('Graph session is closed.');
      if (operation.kind === 'mutate') return mutateAsync(client, operation.mutations);
      if (operation.kind === 'nodes') return { kind: 'nodes', nodes: await readNodesAsync(client, operation.nodeIds) };
      return traverseAsync(client, operation);
    },
    closeAsync: async () => { closed = true; },
  };
}

/*** Atomically apply a batch, aborting the transaction for any provider or contract error. */
async function mutateAsync(client: Neo4jClient, mutations: readonly GraphMutation[]): Promise<GraphDatabaseOperationResult> {
  if (mutations.length === 0) return { kind: 'mutated', mutationCount: 0 };
  const begin = await client.query('POST', '/tx', {});
  const txId = begin.reply.transaction?.id;
  if (typeof txId !== 'string' || !/^[A-Za-z0-9_-]+$/u.test(txId)) {
    throw new Error('Neo4j did not return a valid transaction ID.');
  }
  const context = { txId, ...(begin.affinity === undefined ? {} : { affinity: begin.affinity }) };
  try {
    for (const mutation of mutations) await applyMutationAsync(client, context, mutation);
    await client.query('POST', '/tx/' + txId + '/commit', {}, context.affinity);
    return { kind: 'mutated', mutationCount: mutations.length };
  } catch (error) {
    try { await client.query('DELETE', '/tx/' + txId, undefined, context.affinity); }
    catch { /* Failed rollback is never represented as successful mutation. */ }
    throw error;
  }
}

/*** Apply exactly one mutation inside the caller's open transaction. */
async function applyMutationAsync(client: Neo4jClient, context: MutationContext, mutation: GraphMutation): Promise<void> {
  if (mutation.kind === 'upsertNode') {
    assertId(mutation.node.id);
    await txQuery(client, context, {
      statement: 'MERGE (n:' + nodeLabel + ' {ankhId: $id}) SET n.ankhData = $data RETURN n.ankhId',
      parameters: { id: mutation.node.id, data: encodeProperties(mutation.node.properties) },
    });
    return;
  }
  if (mutation.kind === 'deleteNode') {
    assertId(mutation.nodeId);
    await txQuery(client, context, {
      statement: 'MATCH (n:' + nodeLabel + ' {ankhId: $id}) DELETE n',
      parameters: { id: mutation.nodeId },
    });
    return;
  }
  if (mutation.kind === 'deleteEdge') {
    assertId(mutation.edgeId);
    await txQuery(client, context, {
      statement: 'MATCH ()-[e:' + edgeLabel + ' {ankhId: $id}]->() DELETE e',
      parameters: { id: mutation.edgeId },
    });
    return;
  }
  await upsertEdgeAsync(client, context, mutation.edge);
}

/*** Preserve edge identity, direction and type across re-upserts; roles change via new edges. */
async function upsertEdgeAsync(client: Neo4jClient, context: MutationContext, edge: GraphEdge): Promise<void> {
  assertId(edge.id);
  assertId(edge.sourceNodeId);
  assertId(edge.targetNodeId);
  assertId(edge.type);
  const existing = rows(await txQuery(client, context, {
    statement: 'MATCH (a:' + nodeLabel + ')-[e:' + edgeLabel + ' {ankhId: $id}]->(b:' + nodeLabel + ') RETURN a.ankhId, b.ankhId, e.ankhType',
    parameters: { id: edge.id },
  }));
  if (existing.length > 1) throw new Error('Graph edge identity is not unique.');
  const first = existing[0];
  if (first !== undefined && (first[0] !== edge.sourceNodeId || first[1] !== edge.targetNodeId || first[2] !== edge.type)) {
    throw new Error('Graph edge identity cannot change endpoints or type.');
  }
  const updated = rows(await txQuery(client, context, {
    statement: 'MATCH (a:' + nodeLabel + ' {ankhId: $source}) MATCH (b:' + nodeLabel + ' {ankhId: $target}) MERGE (a)-[e:' + edgeLabel + ' {ankhId: $id}]->(b) SET e.ankhType = $type, e.ankhData = $data RETURN e.ankhId',
    parameters: {
      source: edge.sourceNodeId,
      target: edge.targetNodeId,
      id: edge.id,
      type: edge.type,
      data: encodeProperties(edge.properties),
    },
  }));
  if (updated.length !== 1) throw new Error('Graph edge endpoints are missing or ambiguous.');
}

/*** Retrieve known nodes without treating missing IDs as database errors. */
async function readNodesAsync(client: Neo4jClient, ids: readonly string[]): Promise<readonly GraphNode[]> {
  if (ids.length === 0) return [];
  ids.forEach(assertId);
  const result = await client.query('POST', '', {
    statement: 'MATCH (n:' + nodeLabel + ') WHERE n.ankhId IN $ids RETURN n.ankhId, n.ankhData',
    parameters: { ids },
  });
  const found = new Map(rows(result).map(row => {
    const id = readString(row[0]);
    return [id, { id, properties: decodeProperties(row[1]) } satisfies GraphNode];
  }));
  return ids.flatMap(id => { const node = found.get(id); return node === undefined ? [] : [node]; });
}

/*** Expand a bounded, stable page of reached edges; include nodes incident to those edges. */
async function traverseAsync(client: Neo4jClient, operation: GraphTraversalOperation): Promise<GraphDatabaseOperationResult> {
  const { maxDepth: depth, pageSize, startNodeIds, direction, edgeTypes } = operation;
  if (!Number.isSafeInteger(depth) || depth < 1 || depth > maxDepth) throw new Error('Traversal depth is out of bounds.');
  if (!Number.isSafeInteger(pageSize) || pageSize < 1 || pageSize > maxPageSize) throw new Error('Traversal page size is out of bounds.');
  startNodeIds.forEach(assertId);
  edgeTypes?.forEach(assertId);
  if (startNodeIds.length === 0) return { kind: 'traversal', nodes: [], edges: [] };
  const offset = decodeCursor(operation);
  const arrow = direction === 'incoming' ? '<-[:' + edgeLabel + '*1..' + depth + ']-'
    : direction === 'outgoing' ? '-[:' + edgeLabel + '*1..' + depth + ']->'
      : '-[:' + edgeLabel + '*1..' + depth + ']-';
  const statement = 'MATCH (s:' + nodeLabel + ') WHERE s.ankhId IN $ids MATCH p=(s)' + arrow + '() WHERE ALL(r IN relationships(p) WHERE $anyType OR r.ankhType IN $types) UNWIND relationships(p) AS e WITH DISTINCT e ORDER BY e.ankhId SKIP $offset LIMIT $limit RETURN e.ankhId, startNode(e).ankhId, endNode(e).ankhId, e.ankhType, e.ankhData';
  const result = await client.query('POST', '', {
    statement,
    parameters: { ids: startNodeIds, types: edgeTypes ?? [], anyType: edgeTypes === undefined, offset, limit: pageSize + 1 },
  });
  const records = rows(result);
  const hasMore = records.length > pageSize;
  const edges: GraphEdge[] = records.slice(0, pageSize).map(row => ({
    id: readString(row[0]),
    sourceNodeId: readString(row[1]),
    targetNodeId: readString(row[2]),
    type: readString(row[3]),
    properties: decodeProperties(row[4]),
  }));
  const ids = [...new Set([...startNodeIds, ...edges.flatMap(edge => [edge.sourceNodeId, edge.targetNodeId])])];
  const nodes = await readNodesAsync(client, ids);
  const nextCursor = hasMore ? encodeCursor(operation, offset + pageSize) : undefined;
  return { kind: 'traversal', nodes, edges, ...(nextCursor === undefined ? {} : { nextCursor }) };
}

/*** Encode one opaque traversal position bound to its exact query scope. */
function encodeCursor(operation: GraphTraversalOperation, offset: number): string {
  return Buffer.from(JSON.stringify({
    v: 1, offset, ids: operation.startNodeIds, direction: operation.direction,
    maxDepth: operation.maxDepth, edgeTypes: operation.edgeTypes ?? null,
  })).toString('base64url');
}

/*** Reject stale, cross-scope or unbounded pagination cursors. */
function decodeCursor(operation: GraphTraversalOperation): number {
  if (operation.cursor === undefined) return 0;
  let value: unknown;
  try { value = JSON.parse(Buffer.from(operation.cursor, 'base64url').toString('utf8')); }
  catch { throw new Error('Invalid traversal cursor.'); }
  if (!isRecord(value) || value.v !== 1 || !Number.isSafeInteger(value.offset) ||
    typeof value.offset !== 'number' || value.offset < 0 || value.offset > maxOffset ||
    JSON.stringify(value.ids) !== JSON.stringify(operation.startNodeIds) ||
    value.direction !== operation.direction || value.maxDepth !== operation.maxDepth ||
    JSON.stringify(value.edgeTypes) !== JSON.stringify(operation.edgeTypes ?? null)) {
    throw new Error('Invalid traversal cursor.');
  }
  return value.offset;
}

/*** Serialize the complete portable property map without relying on provider-native nested props. */
function encodeProperties(value: GraphPropertyObject): string {
  if (!isRecord(value)) throw new Error('Graph properties must be an object.');
  const encoded = JSON.stringify(value);
  if (typeof encoded !== 'string') throw new Error('Graph properties are not serializable.');
  return encoded;
}

/*** Restore graph properties exactly as represented by the portable JSON contract. */
function decodeProperties(value: unknown): GraphPropertyObject {
  if (typeof value !== 'string') throw new Error('Neo4j graph property payload is invalid.');
  let decoded: unknown;
  try { decoded = JSON.parse(value); } catch { throw new Error('Neo4j graph property payload is invalid.'); }
  if (!isRecord(decoded)) throw new Error('Neo4j graph property payload is invalid.');
  return decoded as GraphPropertyObject;
}

/*** Restrict IDs and symbolic edge types to nonempty strings without injecting Cypher tokens. */
function assertId(id: string): void {
  if (typeof id !== 'string' || id.length === 0 || id.length > 512) throw new Error('Invalid graph identity.');
}

/*** Narrow protocol values without echoing unsafe provider payloads. */
function readString(value: unknown): string {
  if (typeof value !== 'string') throw new Error('Neo4j returned an invalid graph identity.');
  return value;
}

/*** Validate query rows before a provider result crosses the portable graph boundary. */
function rows(result: QueryResponse): readonly (readonly unknown[])[] {
  if (result.reply.data?.values === undefined) throw new Error('Neo4j query result is missing data.');
  return result.reply.data.values;
}

/*** Narrow an untrusted JSON value to a non-array object. */
function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/*** Post Cypher through one server-only transport, redacting error data by construction. */
class Neo4jClient {
  private readonly url: string;
  private readonly auth: string;
  private readonly fetcher: typeof fetch;
  private readonly timeout: number;

  constructor(options: Neo4jGraphDatabaseOptions) {
    const endpoint = new URL(options.endpoint);
    const loopback = ['localhost', '127.0.0.1', '[::1]'].includes(endpoint.hostname);
    if (endpoint.protocol !== 'https:' && !(loopback && endpoint.protocol === 'http:')) {
      throw new Error('Neo4j requires HTTPS except for loopback development.');
    }
    if (endpoint.pathname !== '/' || endpoint.search || endpoint.hash || !options.username || !options.password) {
      throw new Error('Invalid Neo4j endpoint or credentials configuration.');
    }
    const database = options.database ?? 'neo4j';
    if (!/^[A-Za-z0-9_-]+$/u.test(database)) throw new Error('Invalid Neo4j database name.');
    this.url = endpoint.origin + '/db/' + encodeURIComponent(database) + '/query/v2';
    this.auth = 'Basic ' + Buffer.from(options.username + ':' + options.password).toString('base64');
    this.fetcher = options.fetcher ?? fetch;
    this.timeout = options.timeoutMs ?? 10000;
    if (!Number.isSafeInteger(this.timeout) || this.timeout < 1 || this.timeout > 120000) {
      throw new Error('Invalid Neo4j request timeout.');
    }
  }

  /*** Execute a Query API v2 request, returning only structured data and cluster-affinity routing. */
  async query(method: 'POST' | 'DELETE', suffix: string, body?: QueryBody, affinity?: string): Promise<QueryResponse> {
    let response: Response;
    try {
      response = await this.fetcher(this.url + suffix, {
        method,
        headers: {
          Accept: 'application/json',
          'Content-Type': 'application/json',
          Authorization: this.auth,
          ...(affinity === undefined ? {} : { 'neo4j-cluster-affinity': affinity }),
        },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
        signal: AbortSignal.timeout(this.timeout),
      });
    } catch {
      throw new Error('Neo4j request failed or timed out.');
    }
    let parsed: unknown;
    try { parsed = await response.json(); } catch { throw new Error('Neo4j returned invalid JSON.'); }
    if (!isRecord(parsed)) throw new Error('Neo4j returned invalid response structure.');
    const errors = Array.isArray(parsed.errors) ? parsed.errors : [];
    if (!response.ok || errors.length > 0) {
      const code = errors[0];
      const safeCode = isRecord(code) && typeof code.code === 'string' && /^[A-Za-z0-9._-]+$/u.test(code.code)
        ? code.code : 'provider_error';
      throw new Error('Neo4j query failed (HTTP ' + response.status + ', code ' + safeCode + ').');
    }
    const data = isRecord(parsed.data) && Array.isArray(parsed.data.values) && parsed.data.values.every(Array.isArray)
      ? { values: parsed.data.values as unknown[][] }
      : undefined;
    const transaction = isRecord(parsed.transaction) && typeof parsed.transaction.id === 'string'
      ? { id: parsed.transaction.id } : undefined;
    const affinityValue = response.headers.get('neo4j-cluster-affinity');
    return { reply: { ...(data === undefined ? {} : { data }), ...(transaction === undefined ? {} : { transaction }) },
      ...(affinityValue === null ? {} : { affinity: affinityValue }) };
  }
}

/*** Submit a statement inside the current transaction while preserving affinity. */
async function txQuery(client: Neo4jClient, context: MutationContext, statement: QueryBody): Promise<QueryResponse> {
  return client.query('POST', '/tx/' + context.txId, statement, context.affinity);
}
