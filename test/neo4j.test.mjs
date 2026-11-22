import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  createNeo4jGraphPersistencePort,
  prepareNeo4jGraphPersistenceAsync,
} from '../dist/index.js';
import { executeNeo4jMutationAsync } from '../dist/features/neo4j/executeNeo4jMutationAsync.js';
const settings = {
  endpoint: 'https://neo4j.example.com',
  username: 'neo4j',
  password: 'S3cret!',
  database: 'neo4j',
};
const answer = (rows = [], opts = {}) =>
  new Response(JSON.stringify({ data: { fields: [], values: rows }, ...opts }), {
    status: 202,
    headers: { 'Content-Type': 'application/json', 'neo4j-cluster-affinity': 'server-1' },
  });

test('schema enforces unique node and relationship ids', async () => {
  const calls = [];
  await prepareNeo4jGraphPersistenceAsync({
    ...settings,
    fetcher: async (url, request) => {
      calls.push({ url, payload: JSON.parse(request.body) });
      return answer();
    },
  });
  assert.equal(calls.length, 2);
  assert.match(calls[0].payload.statement, /CREATE CONSTRAINT.*NODE/i);
  assert.match(calls[1].payload.statement, /CREATE CONSTRAINT.*AnkhGraphEdgeIdentity/i);
  assert.doesNotMatch(calls[1].payload.statement, /FOR \(\)-\[/u);
});

test('atomic mutations use one transaction with affinity and stable typed edge id', async () => {
  const calls = [];
  const fetcher = async (url, request) => {
    const path = new URL(url).pathname;
    calls.push({
      path,
      headers: request.headers,
      body: request.body ? JSON.parse(request.body) : undefined,
      method: request.method,
    });
    if (path.endsWith('/tx')) return answer([], { transaction: { id: 't1' } });
    if (path.endsWith('/commit')) return answer();
    if (request.body?.includes('MATCH (identity:AnkhGraphEdgeIdentity')) return answer([]);
    if (request.body?.includes('RETURN isNew')) return answer([[true]]);
    return answer([['upserted']]);
  };
  const port = createNeo4jGraphPersistencePort({ ...settings, fetcher });
  const session = await port.openAsync();
  const res = await session.executeAsync({
    kind: 'mutate',
    mutations: [
      {
        kind: 'upsertNode',
        node: {
          id: 'a',
          labels: ['Company'],
          data: { properties: { kind: 'company' }, document: { nested: { validFrom: null } } },
        },
      },
      {
        kind: 'upsertNode',
        node: { id: 'b', labels: ['Person'], data: { properties: { kind: 'person' } } },
      },
      {
        kind: 'upsertEdge',
        edge: {
          id: 'role-1',
          source: 'b',
          target: 'a',
          type: 'ROLE_AT',
          data: { properties: { validTo: 'open' } },
        },
      },
    ],
  });
  assert.deepEqual(res, { kind: 'mutated', mutationCount: 3 });
  assert.equal(calls.length, 8); // BEGIN + node lookup/write + node lookup/write + edge lookup/write + COMMIT
  assert.equal(calls.at(-1).path.endsWith('/tx/t1/commit'), true);
  assert.equal(calls.at(-2).headers['neo4j-cluster-affinity'], 'server-1');
  assert.equal(JSON.parse(calls[2].body.parameters.data).document.nested.validFrom, null);
  assert.ok(calls.every((x) => JSON.stringify(x).indexOf('S3cret!') === -1));
  await session.closeAsync();
  await assert.rejects(session.executeAsync({ kind: 'nodes', nodeIds: [] }), /closed/);
});

test('conformance atomic-rollback rolls back missing endpoints instead of reporting success', async () => {
  const calls = [];
  const fetcher = async (url, request) => {
    const path = new URL(url).pathname;
    calls.push({ path, method: request.method });
    if (path.endsWith('/tx')) return answer([], { transaction: { id: 't2' } });
    if (path.endsWith('/tx/t2') && request.body?.includes('RETURN e.ankhId')) return answer([]);
    if (request.body?.includes('MATCH (identity:AnkhGraphEdgeIdentity')) return answer([]);
    if (request.body?.includes('RETURN isNew')) return answer([]);
    return answer([['ok']]);
  };
  const session = await createNeo4jGraphPersistencePort({ ...settings, fetcher }).openAsync();
  await assert.rejects(
    session.executeAsync({
      kind: 'mutate',
      mutations: [
        {
          kind: 'upsertEdge',
          edge: {
            id: 'e',
            type: 'WORKS_AT',
            source: 'missing',
            target: 'b',
            data: { properties: {} },
          },
        },
      ],
    }),
    /endpoints/,
  );
  assert.equal(calls.at(-1).method, 'DELETE');
});

test('conformance stable-edge-identity rejects an existing native edge with changed endpoints or type', async () => {
  const transaction = {
    queryAsync: async () => ({
      values: [['a', 'b', 'WORKS_AT', JSON.stringify({ properties: {} })]],
    }),
  };
  await assert.rejects(
    executeNeo4jMutationAsync(transaction, [
      {
        kind: 'upsertEdge',
        edge: {
          id: 'edge-1',
          source: 'a',
          target: 'c',
          type: 'WORKS_AT',
          data: { properties: {} },
        },
      },
    ]),
    /identity cannot change/,
  );
});

test('conformance idempotent-mutation-count reports zero for an unchanged node upsert', async () => {
  const data = { properties: { active: true } };
  const transaction = {
    queryAsync: async () => ({ values: [[JSON.stringify(data), ['AnkhGraphNode', 'Person']]] }),
  };
  const mutationCount = await executeNeo4jMutationAsync(transaction, [
    { kind: 'upsertNode', node: { id: 'person-1', labels: ['Person'], data } },
  ]);
  assert.equal(mutationCount, 0);
});

test('conformance node-delete-safety preserves ordered batches and rejects attached nodes', async () => {
  const statements = [];
  const transaction = {
    queryAsync: async ({ statement }) => {
      statements.push(statement);
      return { values: statement.includes('LIMIT 1') ? [['edge-1']] : [] };
    },
  };
  await assert.rejects(
    executeNeo4jMutationAsync(transaction, [
      { kind: 'deleteNode', nodeId: 'person-1' },
      { kind: 'deleteEdge', edgeId: 'edge-1' },
    ]),
    /requires deleting attached edges/,
  );
  assert.match(statements[0], /DELETE n/u);
  assert.equal(
    statements.some((statement) => statement.includes('DELETE e, identity')),
    false,
  );
});

test('read nodes preserves order and nested portable properties', async () => {
  const fetcher = async () =>
    answer([
      [
        'b',
        ['AnkhGraphNode', 'Person'],
        JSON.stringify({ properties: { score: 1 }, document: { a: [1, { p: null }] } }),
      ],
      ['a', ['AnkhGraphNode', 'Company'], JSON.stringify({ properties: { x: 'yes' } })],
    ]);
  const session = await createNeo4jGraphPersistencePort({ ...settings, fetcher }).openAsync();
  const r = await session.executeAsync({ kind: 'nodes', nodeIds: ['a', 'missing', 'b'] });
  assert.deepEqual(r, {
    kind: 'nodes',
    nodes: [
      { id: 'a', labels: ['Company'], data: { properties: { x: 'yes' } } },
      {
        id: 'b',
        labels: ['Person'],
        data: { properties: { score: 1 }, document: { a: [1, { p: null }] } },
      },
    ],
  });
});

test('conformance traversal-inclusion-and-order and cursor-consistency retain endpoint nodes and scope', async () => {
  const statements = [];
  const fetcher = async (url, req) => {
    if (req.body?.includes('MATCH (s:AnkhGraphNode)')) {
      statements.push(JSON.parse(req.body).statement);
      return answer([
        ['e1', 'a', 'b', 'ROLE_AT', JSON.stringify({ properties: { validFrom: '2023-01-01' } })],
        ['e2', 'a', 'b', 'ROLE_AT', JSON.stringify({ properties: { validTo: 'open' } })],
      ]);
    }
    return answer([
      ['a', ['AnkhGraphNode', 'Person'], JSON.stringify({ properties: {} })],
      ['b', ['AnkhGraphNode', 'Company'], JSON.stringify({ properties: {} })],
    ]);
  };
  const session = await createNeo4jGraphPersistencePort({ ...settings, fetcher }).openAsync();
  const q = {
    kind: 'traverse',
    startNodeIds: ['a'],
    direction: 'outgoing',
    maxDepth: 2,
    pageSize: 1,
    types: ['ROLE_AT'],
  };
  const r = await session.executeAsync(q);
  assert.equal(r.kind, 'traversal');
  assert.equal(r.edges.length, 1);
  assert.deepEqual(
    r.edges.map((edge) => edge.id),
    ['e1'],
  );
  assert.deepEqual(
    r.nodes.map((node) => node.id),
    ['a', 'b'],
  );
  assert.equal(r.cursorConsistency, 'live');
  assert.ok(r.nextCursor);
  assert.match(statements[0], /ORDER BY e\.ankhIdentityId/u);
  assert.match(statements[0], /RETURN e\.ankhIdentityId/u);
  await assert.rejects(
    session.executeAsync({ ...q, direction: 'incoming', cursor: r.nextCursor }),
    /cursor/,
  );
});

test('zero-depth traversal returns only start nodes without issuing a variable-length Cypher query', async () => {
  const statements = [];
  const fetcher = async (url, req) => {
    const statement = JSON.parse(req.body).statement;
    statements.push(statement);
    return answer([
      ['a', ['AnkhGraphNode', 'Person'], JSON.stringify({ properties: { active: true } })],
    ]);
  };
  const session = await createNeo4jGraphPersistencePort({ ...settings, fetcher }).openAsync();
  const result = await session.executeAsync({
    kind: 'traverse',
    startNodeIds: ['a'],
    direction: 'outgoing',
    maxDepth: 0,
    pageSize: 1,
  });
  assert.deepEqual(result, {
    kind: 'traversal',
    nodes: [{ id: 'a', labels: ['Person'], data: { properties: { active: true } } }],
    edges: [],
    cursorConsistency: 'live',
  });
  assert.equal(
    statements.some((statement) => statement.includes('MATCH p=')),
    false,
  );
});

test('no plaintext secret is leaked from a provider error response', async () => {
  const fetcher = async () =>
    new Response(
      JSON.stringify({
        errors: [{ code: 'Neo.ClientError.Statement.SyntaxError', message: 'sensitive S3cret!' }],
      }),
      { status: 400 },
    );
  const s = await createNeo4jGraphPersistencePort({ ...settings, fetcher }).openAsync();
  await assert.rejects(s.executeAsync({ kind: 'nodes', nodeIds: ['a'] }), (error) => {
    assert.equal(error.message.includes('S3cret!'), false);
    return true;
  });
});
