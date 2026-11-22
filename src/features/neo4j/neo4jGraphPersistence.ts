import type {
  GraphPersistenceCapability,
  GraphPersistenceOperation,
  GraphPersistencePort,
  GraphPersistenceResult,
  GraphPersistenceSession,
} from '@ankhorage/graph/persistence';
import { assertGraphPersistenceOperation } from '@ankhorage/graph/persistence';

import { executeNeo4jMutationAsync } from './executeNeo4jMutationAsync.js';
import { executeNeo4jTraversalAsync, readNeo4jNodesAsync } from './executeNeo4jTraversalAsync.js';
import { getNeo4jNodeLabel } from './neo4jStorage.js';
import type { Neo4jGraphPersistenceOptions } from './neo4jTransport.js';
import type { Neo4jQuery } from './neo4jTransport.js';
import { createNeo4jTransport } from './neo4jTransport.js';

export type { Neo4jGraphPersistenceOptions } from './neo4jTransport.js';

const capabilities: readonly GraphPersistenceCapability[] = ['mutate', 'nodes', 'traverse'];

/*** Create the provider-neutral Graph persistence port backed by Neo4j Query API v2. */
export function createNeo4jGraphPersistencePort(
  options: Neo4jGraphPersistenceOptions,
): GraphPersistencePort {
  const transport = createNeo4jTransport(options);
  return { openAsync: () => Promise.resolve(createSession(transport)) };
}

/*** Prepare global node and relationship identity constraints without affecting ordinary reads. */
export async function prepareNeo4jGraphPersistenceAsync(
  options: Neo4jGraphPersistenceOptions,
): Promise<void> {
  const transport = createNeo4jTransport(options);
  const nodeLabel = getNeo4jNodeLabel();
  await transport.queryAsync('POST', '', {
    statement:
      'CREATE CONSTRAINT ankh_graph_node_id IF NOT EXISTS FOR (n:' +
      nodeLabel +
      ') REQUIRE n.ankhId IS UNIQUE',
  });
  await transport.queryAsync('POST', '', {
    statement:
      'CREATE CONSTRAINT ankh_graph_edge_id IF NOT EXISTS FOR ()-[e]-() REQUIRE e.ankhId IS UNIQUE',
  });
}

/*** Scope a stateful provider connection to an explicit closeable Graph persistence session. */
function createSession(
  transport: ReturnType<typeof createNeo4jTransport>,
): GraphPersistenceSession {
  let closed = false;
  const reader = { queryAsync: (query: Neo4jQuery) => transport.queryAsync('POST', '', query) };
  return {
    capabilities,
    executeAsync: async (operation: GraphPersistenceOperation): Promise<GraphPersistenceResult> => {
      if (closed) throw new Error('Graph session is closed.');
      assertGraphPersistenceOperation(operation);
      if (operation.kind === 'nodes') {
        return { kind: 'nodes', nodes: await readNeo4jNodesAsync(reader, operation.nodeIds) };
      }
      if (operation.kind === 'traverse') return executeNeo4jTraversalAsync(reader, operation);
      return executeTransactionAsync(transport, operation.mutations);
    },
    closeAsync: () => {
      closed = true;
      return Promise.resolve();
    },
  };
}

/*** Commit a whole mutation batch or issue a best-effort rollback before surfacing the failure. */
async function executeTransactionAsync(
  transport: ReturnType<typeof createNeo4jTransport>,
  mutations: Parameters<typeof executeNeo4jMutationAsync>[1],
): Promise<GraphPersistenceResult> {
  if (mutations.length === 0) return { kind: 'mutated', mutationCount: 0 };
  const begin = await transport.queryAsync('POST', '/tx', {});
  if (begin.transactionId === undefined || !/^[A-Za-z0-9_-]+$/u.test(begin.transactionId)) {
    throw new Error('Neo4j did not return a valid transaction ID.');
  }
  const queryAsync = (query: Neo4jQuery) =>
    transport.queryAsync('POST', '/tx/' + begin.transactionId, query, begin.affinity);
  try {
    const mutationCount = await executeNeo4jMutationAsync({ queryAsync }, mutations);
    await transport.queryAsync(
      'POST',
      '/tx/' + begin.transactionId + '/commit',
      {},
      begin.affinity,
    );
    return { kind: 'mutated', mutationCount };
  } catch (error) {
    await rollbackAsync(transport, begin.transactionId, begin.affinity);
    throw error;
  }
}

/*** Never report a failed transaction as committed, even when provider rollback itself also fails. */
async function rollbackAsync(
  transport: ReturnType<typeof createNeo4jTransport>,
  transactionId: string,
  affinity?: string,
): Promise<void> {
  try {
    await transport.queryAsync('DELETE', '/tx/' + transactionId, undefined, affinity);
  } catch {
    // The original mutation failure remains authoritative.
  }
}
