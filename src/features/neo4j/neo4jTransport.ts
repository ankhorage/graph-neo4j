/*** Server-only settings for one Neo4j Query API v2 backend. */
export interface Neo4jGraphPersistenceOptions {
  readonly endpoint: string;
  readonly username: string;
  readonly password: string;
  readonly database?: string;
  readonly timeoutMs?: number;
  readonly fetcher?: typeof fetch;
}

export interface Neo4jQuery {
  readonly statement?: string;
  readonly parameters?: Readonly<Record<string, unknown>>;
}

export interface Neo4jReply {
  readonly values: readonly (readonly unknown[])[];
  readonly transactionId?: string;
  readonly affinity?: string;
}

/*** Create the Query API v2 transport with endpoint validation and redacted provider failures. */
export function createNeo4jTransport(options: Neo4jGraphPersistenceOptions) {
  const config = createTransportConfig(options);
  return {
    queryAsync: async (
      method: 'POST' | 'DELETE',
      suffix: string,
      query?: Neo4jQuery,
      affinity?: string,
    ): Promise<Neo4jReply> => requestAsync(config, method, suffix, query, affinity),
  };
}

interface TransportConfig {
  readonly url: string;
  readonly authorization: string;
  readonly fetcher: typeof fetch;
  readonly timeoutMs: number;
}

/*** Validate immutable provider connection settings before any request can be sent. */
function createTransportConfig(options: Neo4jGraphPersistenceOptions): TransportConfig {
  const endpoint = new URL(options.endpoint);
  assertEndpoint(endpoint, options);
  const database = options.database ?? 'neo4j';
  if (!/^[A-Za-z0-9_-]+$/u.test(database)) throw new Error('Invalid Neo4j database name.');
  const timeoutMs = options.timeoutMs ?? 10_000;
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 120_000) {
    throw new Error('Invalid Neo4j request timeout.');
  }
  return {
    url: endpoint.origin + '/db/' + encodeURIComponent(database) + '/query/v2',
    authorization:
      'Basic ' + Buffer.from(options.username + ':' + options.password).toString('base64'),
    fetcher: options.fetcher ?? fetch,
    timeoutMs,
  };
}

/*** Restrict provider endpoints to a complete HTTPS authority or explicit loopback development. */
function assertEndpoint(endpoint: URL, options: Neo4jGraphPersistenceOptions): void {
  const allowsHttp = ['localhost', '127.0.0.1', '[::1]'].includes(endpoint.hostname);
  if (endpoint.protocol !== 'https:' && !(allowsHttp && endpoint.protocol === 'http:')) {
    throw new Error('Neo4j requires HTTPS except for loopback development.');
  }
  if (
    endpoint.pathname !== '/' ||
    endpoint.search ||
    endpoint.hash ||
    !options.username ||
    !options.password
  ) {
    throw new Error('Invalid Neo4j endpoint or credentials configuration.');
  }
}

/*** Execute one provider request without exposing response bodies or credentials in errors. */
async function requestAsync(
  config: TransportConfig,
  method: 'POST' | 'DELETE',
  suffix: string,
  query?: Neo4jQuery,
  affinity?: string,
): Promise<Neo4jReply> {
  const response = await fetchResponseAsync(config, method, suffix, query, affinity);
  const responseBody = await readResponseAsync(response);
  assertProviderSuccess(response, responseBody);
  return toNeo4jReply(response, responseBody);
}

/*** Send the request with bounded cancellation and no retry that could duplicate a mutation. */
async function fetchResponseAsync(
  config: TransportConfig,
  method: 'POST' | 'DELETE',
  suffix: string,
  query?: Neo4jQuery,
  affinity?: string,
): Promise<Response> {
  try {
    return await config.fetcher(config.url + suffix, {
      method,
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json',
        Authorization: config.authorization,
        ...(affinity === undefined ? {} : { 'neo4j-cluster-affinity': affinity }),
      },
      ...(query === undefined ? {} : { body: JSON.stringify(query) }),
      signal: AbortSignal.timeout(config.timeoutMs),
    });
  } catch {
    throw new Error('Neo4j request failed or timed out.');
  }
}

/*** Parse only JSON records returned by Query API v2. */
async function readResponseAsync(response: Response): Promise<Record<string, unknown>> {
  try {
    const value: unknown = await response.json();
    if (isRecord(value)) return value;
  } catch {
    // Provider payloads are deliberately not included in public errors.
  }
  throw new Error('Neo4j returned invalid JSON.');
}

/*** Reject HTTP and structured provider errors by code only. */
function assertProviderSuccess(response: Response, value: Record<string, unknown>): void {
  const errors: readonly unknown[] = Array.isArray(value.errors) ? value.errors : [];
  if (response.ok && errors.length === 0) return;
  const firstError = errors.at(0);
  const code =
    isRecord(firstError) &&
    typeof firstError.code === 'string' &&
    /^[A-Za-z0-9._-]+$/u.test(firstError.code)
      ? firstError.code
      : 'provider_error';
  throw new Error('Neo4j query failed (HTTP ' + response.status + ', code ' + code + ').');
}

/*** Normalize a successful provider payload at the adapter boundary. */
function toNeo4jReply(response: Response, value: Record<string, unknown>): Neo4jReply {
  const data = isRecord(value.data) ? value.data : undefined;
  const values =
    data !== undefined && Array.isArray(data.values) && data.values.every(Array.isArray)
      ? (data.values as readonly (readonly unknown[])[])
      : [];
  const transaction = isRecord(value.transaction) ? value.transaction : undefined;
  const transactionId =
    transaction !== undefined && typeof transaction.id === 'string' ? transaction.id : undefined;
  const affinity = response.headers.get('neo4j-cluster-affinity') ?? undefined;
  return {
    values,
    ...(transactionId === undefined ? {} : { transactionId }),
    ...(affinity === undefined ? {} : { affinity }),
  };
}

/*** Identify a JSON-like record without accepting arrays. */
function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
