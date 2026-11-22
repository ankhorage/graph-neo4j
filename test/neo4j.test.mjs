import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  createNeo4jGraphPersistencePort,
  prepareNeo4jGraphPersistenceAsync,
} from '../dist/index.js';
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
  assert.match(calls[1].payload.statement, /CREATE CONSTRAINT.*EDGE/i);
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
    if (request.body?.includes('MATCH (a:AnkhGraphNode)-[e {ankhId')) return answer([]);
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

test('missing endpoints roll back instead of reporting success', async () => {
  const calls = [];
  const fetcher = async (url, request) => {
    const path = new URL(url).pathname;
    calls.push({ path, method: request.method });
    if (path.endsWith('/tx')) return answer([], { transaction: { id: 't2' } });
    if (path.endsWith('/tx/t2') && request.body?.includes('RETURN e.ankhId')) return answer([]);
    if (request.body?.includes('MATCH (a:AnkhGraphNode)-[e {ankhId')) return answer([]);
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

test('bounded traversal cursor does not accept changes in query scope', async () => {
  const fetcher = async (url, req) => {
    if (req.body?.includes('MATCH (s:AnkhGraphNode)'))
      return answer([
        ['e1', 'a', 'b', 'ROLE_AT', JSON.stringify({ properties: { validFrom: '2023-01-01' } })],
        ['e2', 'a', 'b', 'ROLE_AT', JSON.stringify({ properties: { validTo: 'open' } })],
      ]);
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
  assert.ok(r.nextCursor);
  await assert.rejects(
    session.executeAsync({ ...q, direction: 'incoming', cursor: r.nextCursor }),
    /cursor/,
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
