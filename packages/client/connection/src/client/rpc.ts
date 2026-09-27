/** Browser caller for generic Connection unary RPC channels. */

import {
  RpcId,
  type ClientRequest,
  type RpcId as RpcIdType,
} from '../rpc.ts'
import type { ClientConnectionRpc, ConnectionRpcResult } from '../rpc.ts'
import { randomUuid } from './random-uuid.ts'

const CHANNEL_PATTERN = /^\/[A-Za-z0-9._~-]+$/
const ENDPOINT_SEGMENT_PATTERN = /^[A-Za-z0-9_$.-]+$/

/** Sent rpcIds remembered per caller, so a mismatching response can be attributed to a replayed earlier request. */
const RECENT_SENT_RPC_IDS = 32

/**
 * Transport this caller posts through; same signature as the global `fetch`.
 * Receives the document-relative route so a carrier resolves it against its own base.
 */
export type RpcFetch = (input: string | URL, init: RequestInit) => Promise<Response>

/** Worker-local opener for decoded Gateway Remote streams; `uplink` carries the Client's items for the stream. */
export type RpcStreamOpen = (
  endpoint: string,
  payload: unknown,
  signal: AbortSignal,
  uplink?: AsyncIterable<unknown>,
) => AsyncIterable<unknown>

/**
 * The response envelope named another request's rpcId, so its payload cannot be trusted for this call.
 * `staleReplay` records whether the foreign rpcId belongs to one of this caller's own recent
 * requests — the signature of a cached or replayed body — or to an unrecognized source. `attempt`
 * counts the surfaced call's attempts, so a reported error also identifies the running build.
 */
class RpcIdMismatchError extends Error {
  constructor(endpoint: string, sent: string, got: string, staleReplay: boolean, attempt: number) {
    super(`rpcId mismatch for ${endpoint}: sent ${sent}, got ${got} (attempt ${attempt}/3, ${staleReplay ? 'stale replay of an earlier request' : 'unrecognized response body'})`)
    this.name = 'RpcIdMismatchError'
  }
}

/**
 * Create the browser-backed generic RPC caller.
 * @param doFetch - transport override; defaults to the page's global fetch.
 * @param openStream - optional worker-local Gateway stream carrier.
 * @returns caller that owns request correlation and response-envelope validation.
 */
export function createWebConnectionRpc(doFetch?: RpcFetch, openStream?: RpcStreamOpen): ClientConnectionRpc {
  const send: RpcFetch = doFetch ?? ((input, init) => globalThis.fetch(input, init))
  const sentRpcIds: RpcIdType[] = []
  return {
    async call(channel, endpoint, payload, signal) {
      assertTarget(channel, endpoint)
      // The channel key is absolute; a page posts the document-relative form, and
      // a carrier that resolves against the Host root accepts the same form.
      const route = `${channel}/${endpoint}`.slice(1)
      const attempt = async (attemptNo: number): Promise<ConnectionRpcResult<unknown>> => {
        const rpcId = RpcId(randomUuid())
        sentRpcIds.push(rpcId)
        if (sentRpcIds.length > RECENT_SENT_RPC_IDS) sentRpcIds.shift()
        const message: ClientRequest = {
          type: 'client-request',
          rpcId,
          method: endpoint,
          payload,
        }
        // A per-attempt unique route query keeps any URL-keyed cache or replay
        // between the page and the Host from serving another call's body; the
        // Host resolves the endpoint from the path alone.
        const response = await send(
          `${route}?rpcId=${rpcId}`,
          {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify(message),
            ...signal === undefined ? {} : { signal },
          },
        )
        if (!response.ok) {
          throw new Error(`transport failure for ${channel}/${endpoint}: HTTP ${response.status}`)
        }
        const mediaType = response.headers.get('content-type')?.split(';', 1)[0]?.trim().toLowerCase()
        const full = mediaType === 'multipart/form-data'
          ? await parseBinaryResponse(response)
          : parseConnectionResponse(await response.json())
        signal?.throwIfAborted()
        if (full.rpcId !== rpcId) {
          throw new RpcIdMismatchError(endpoint, rpcId, full.rpcId, sentRpcIds.includes(full.rpcId), attemptNo)
        }
        return full.result
      }
      const maxMismatchRetries = 2
      for (let retry = 0; ; retry++) {
        try {
          return await attempt(retry + 1)
        } catch (error) {
          // A mismatching response was crossed or replayed by an instrumenting
          // layer between the page and the Host (fetch-wrapping shell, browser
          // injection, or embedded-browser instrumentation) outside any URL
          // key. The call itself was unaffected, so fresh-id retries with a
          // short delay outlast the crossing window; a final mismatch surfaces
          // with its replay attribution. An aborted call re-enters the attempt
          // and fails on its signal there.
          if (!(error instanceof RpcIdMismatchError) || retry >= maxMismatchRetries) throw error
          await new Promise<void>((resolve) => {
            setTimeout(resolve, 30 * (retry + 1))
          })
        }
      }
    },
    ...openStream === undefined ? {} : {
      open(channel, endpoint, payload, signal, uplink) {
        assertTarget(channel, endpoint)
        if (channel !== '/api') {
          throw new Error(`connection: worker-local streams require the /api channel, got ${JSON.stringify(channel)}`)
        }
        return openStream(endpoint, payload, signal, uplink)
      },
    },
  }
}

async function parseBinaryResponse(response: Response): Promise<ReturnType<typeof parseConnectionResponse>> {
  const body = await response.formData()
  const fields = new Map<string, string | Blob>()
  for (const [name, value] of body) {
    if (fields.has(name)) throw new TypeError('connection: invalid binary response fields')
    fields.set(name, value)
  }
  const metadata = fields.get('metadata')
  fields.delete('metadata')
  if (typeof metadata !== 'string') {
    throw new TypeError('connection: invalid binary response fields')
  }
  const envelope: unknown = JSON.parse(metadata)
  const full = parseConnectionResponse(envelope)
  if (!full.result.ok || !isRecord(envelope) || !Array.isArray(envelope.attachments) || envelope.attachments.length === 0) {
    throw new TypeError('connection: invalid binary response result')
  }
  const root = { value: full.result.value }
  for (const attachment of envelope.attachments) {
    if (!isRecord(attachment) || attachment.codec !== 'bytes' || typeof attachment.part !== 'string'
      || !Array.isArray(attachment.path)) {
      throw new TypeError('connection: invalid binary response attachment')
    }
    const data = fields.get(attachment.part)
    fields.delete(attachment.part)
    if (!(data instanceof Blob)) {
      throw new TypeError('connection: invalid binary response fields')
    }
    let parent: object = root
    let key: string | number = 'value'
    for (const segment of attachment.path) {
      const value: unknown = Reflect.get(parent, key)
      if (typeof value !== 'object' || value === null) {
        throw new TypeError('connection: invalid binary response path')
      }
      if (Array.isArray(value)) {
        if (typeof segment !== 'number' || !Number.isSafeInteger(segment) || segment < 0 || segment >= value.length) {
          throw new TypeError('connection: invalid binary response path')
        }
      } else if (typeof segment !== 'string') {
        throw new TypeError('connection: invalid binary response path')
      }
      if (!Object.hasOwn(value, segment)) throw new TypeError('connection: invalid binary response path')
      parent = value
      key = segment
    }
    if (Reflect.get(parent, key) !== null) throw new TypeError('connection: invalid binary response placeholder')
    Object.defineProperty(parent, key, {
      value: new Uint8Array(await data.arrayBuffer()), enumerable: true, writable: true, configurable: true,
    })
  }
  if (fields.size !== 0) throw new TypeError('connection: invalid binary response fields')
  return {
    rpcId: full.rpcId,
    result: { ok: true, value: root.value },
  }
}

function parseConnectionResponse(value: unknown): {
  readonly rpcId: RpcIdType
  readonly result: ConnectionRpcResult<unknown>
} {
  if (!isRecord(value) || value.type !== 'server-response' || typeof value.rpcId !== 'string') {
    throw new TypeError('connection: invalid server-response envelope')
  }
  const result = value.result
  if (!isRecord(result)) throw new TypeError('connection: invalid server-response result')
  if (result.ok === true) {
    return {
      rpcId: RpcId(value.rpcId),
      result: { ok: true, value: result.value },
    }
  }
  if (result.ok !== false || !isRecord(result.error)) {
    throw new TypeError('connection: invalid server-response result')
  }
  const error = result.error
  if (typeof error.code !== 'string' || typeof error.message !== 'string' || !isRecord(error.details)) {
    throw new TypeError('connection: invalid server-response failure')
  }
  return {
    rpcId: RpcId(value.rpcId),
    result: {
      ok: false,
      error: { code: error.code, message: error.message, details: error.details },
    },
  }
}

function isRecord(value: unknown): value is Record<PropertyKey, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function assertTarget(channel: string, endpoint: string): void {
  const segments = endpoint.split('/')
  if (!CHANNEL_PATTERN.test(channel)
    || segments.some(segment =>
      segment === '' || segment === '.' || segment === '..' || !ENDPOINT_SEGMENT_PATTERN.test(segment))) {
    throw new Error(`connection: invalid RPC target ${JSON.stringify(`${channel}/${endpoint}`)}`)
  }
}
