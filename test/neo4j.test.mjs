import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createNeo4jGraphDatabasePort, prepareNeo4jGraphDatabaseAsync } from '../dist/index.js';
const settings = { endpoint: 'https://neo4j.example.com', username: 'neo4j', password: 'S3cret!', database: 'neo4j' };
const answer = (rows = [], opts = {}) => new Response(JSON.stringify({ data: {fields: [],values: rows}, ...opts }), { status: 202, headers: {'Content-Type': 'application/json', 'neo4j-cluster-affinity': 'server-1'} });

test('schema enforces unique node and relationship ids', async () => {
  const calls = [];
  await prepareNeo4jGraphDatabaseAsync({...settings, fetcher: async (url, request) => {
    calls.push({url, payload: JSON.parse(request.body)});
    return answer();
  }});
  assert.equal(calls.length, 2);
  assert.match(calls[0].payload.statement, /CREATE CONSTRAINT.*NODE/i);
  assert.match(calls[1].payload.statement, /CREATE CONSTRAINT.*EDGE/i);
});

test('atomic mutations use one transaction with affinity and stable typed edge id', async () => {
  const calls = [];
  const fetcher = async (url, request) => {
    const path = new URL(url).pathname;
    calls.push({path,headers:request.headers,body:request.body ? JSON.parse(request.body) : undefined,method: request.method});
    if (path.endsWith('/tx')) return answer([], {transaction:{id:'t1'}});
    if (path.endsWith('/commit')) return answer();
    if (request.body?.includes('MATCH (a:AnkhGraphNode)-[e:ANKH_EDGE')) return answer([]);
    return answer([['upserted']]);
  };
  const port = createNeo4jGraphDatabasePort({...settings,fetcher});
  const session = await port.openAsync();
  const res=await session.executeAsync({kind:'mutate',mutations:[
    {kind:'upsertNode',node:{id:'a',properties:{kind:'company',nested:{validFrom:null}}}},
    {kind:'upsertNode',node:{id:'b',properties:{kind:'person'}}},
    {kind:'upsertEdge',edge:{id:'role-1',sourceNodeId:'b',targetNodeId:'a',type:'ROLE_AT',properties:{validTo:null}}},
  ]});
  assert.deepEqual(res,{kind:'mutated',mutationCount:3});
  assert.equal(calls.length,6); // BEGIN + node + node + lookup + edge + COMMIT
  assert.equal(calls.at(-1).path.endsWith('/tx/t1/commit'),true);
  assert.equal(calls.at(-2).headers['neo4j-cluster-affinity'],'server-1');
  assert.equal(JSON.parse(calls[1].body.parameters.data).nested.validFrom,null);
  assert.ok(calls.every(x=>JSON.stringify(x).indexOf('S3cret!')===-1));
  await session.closeAsync();
  await assert.rejects(session.executeAsync({kind:'nodes',nodeIds:[]}),/closed/);
});

test('missing endpoints roll back instead of reporting success',async()=>{
 const calls=[];
 const fetcher=async(url,request)=>{
  const path=new URL(url).pathname;
  calls.push({path,method:request.method});
  if(path.endsWith('/tx'))return answer([],{transaction:{id:'t2'}});
  if(path.endsWith('/tx/t2')&&request.body?.includes('RETURN e.ankhId'))return answer([]);
  if(request.body?.includes('MATCH (a:AnkhGraphNode)-[e:ANKH_EDGE'))return answer([]);
  return answer([['ok']]);
 };
 const session=await createNeo4jGraphDatabasePort({...settings,fetcher}).openAsync();
 await assert.rejects(session.executeAsync({kind:'mutate',mutations:[{kind:'upsertEdge',edge:{id:'e',type:'WORKS_AT',sourceNodeId:'missing',targetNodeId:'b',properties:{}}}]}),/endpoints/);
 assert.equal(calls.at(-1).method,'DELETE');
});

test('read nodes preserves order and nested portable properties',async()=>{
 const fetcher=async()=>answer([['b',JSON.stringify({a:[1,{p:null}]})], ['a',JSON.stringify({x:'yes'})]]);
 const session=await createNeo4jGraphDatabasePort({...settings,fetcher}).openAsync();
 const r=await session.executeAsync({kind:'nodes',nodeIds:['a','missing','b']});
 assert.deepEqual(r,{kind:'nodes',nodes:[{id:'a',properties:{x:'yes'}},{id:'b',properties:{a:[1,{p:null}]}}]});
});

test('bounded traversal cursor does not accept changes in query scope',async()=>{
 const fetcher=async(url,req)=>{
  if(req.body?.includes('MATCH (s:AnkhGraphNode)')) return answer([
   ['e1','a','b','ROLE_AT',JSON.stringify({validFrom:'2023-01-01'})],
   ['e2','a','b','ROLE_AT',JSON.stringify({validTo:null})],
  ]);
  return answer([['a','{}'],['b','{}']]);
 };
 const session=await createNeo4jGraphDatabasePort({...settings,fetcher}).openAsync();
 const q={kind:'traverse',startNodeIds:['a'],direction:'outgoing',maxDepth:2,pageSize:1,edgeTypes:['ROLE_AT']};
 const r=await session.executeAsync(q);
 assert.equal(r.kind,'traversal');assert.equal(r.edges.length,1);assert.ok(r.nextCursor);
 await assert.rejects(session.executeAsync({...q,direction:'incoming',cursor:r.nextCursor}),/cursor/);
});

test('no plaintext secret is leaked from a provider error response',async()=>{
 const fetcher=async()=>new Response(JSON.stringify({errors:[{code:'Neo.ClientError.Statement.SyntaxError',message:'sensitive S3cret!'}]}),{status:400});
 const s=await createNeo4jGraphDatabasePort({...settings,fetcher}).openAsync();
 await assert.rejects(s.executeAsync({kind:'nodes',nodeIds:['a']}),error=>{
  assert.equal(error.message.includes('S3cret!'),false);
  return true;
 });
});
