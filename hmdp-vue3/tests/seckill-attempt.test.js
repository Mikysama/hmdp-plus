import test from 'node:test'
import assert from 'node:assert/strict'
import { createAttemptStore, pollAttempt } from '../src/utils/seckillAttempt.js'
import { normalizeRequestError } from '../src/utils/requestError.js'

const memoryStorage = () => {
  const values = new Map()
  return {
    getItem: (key) => values.get(key) ?? null,
    setItem: (key, value) => values.set(key, value)
  }
}

test('an unresolved request survives reload and retries with the same ID, scoped to user and voucher', () => {
  const storage = memoryStorage()
  const first = createAttemptStore(storage).getOrCreate(
    'u1',
    'v1',
    () => 'request-1'
  )
  const restored = createAttemptStore(storage)
  assert.equal(
    restored.getOrCreate('u1', 'v1', () => 'request-2').requestId,
    first.requestId
  )
  assert.equal(
    restored.getOrCreate('u2', 'v1', () => 'request-3').requestId,
    'request-3'
  )
  assert.equal(
    restored.getOrCreate('u1', 'v2', () => 'request-4').requestId,
    'request-4'
  )
})

test('NOT_FOUND preserves uncertainty while definitive failure permits a new intent', () => {
  const store = createAttemptStore(memoryStorage())
  store.getOrCreate('u', 'v', () => 'original')
  store.saveResult('u', 'v', { requestId: 'original', status: 'NOT_FOUND' })
  assert.equal(
    store.getOrCreate('u', 'v', () => 'replacement').requestId,
    'original'
  )
  store.saveResult('u', 'v', {
    requestId: 'original',
    status: 'FAILED',
    reasonCode: 'EXPIRED'
  })
  assert.equal(
    store.getOrCreate('u', 'v', () => 'replacement').requestId,
    'replacement'
  )
})

test('success remains bound to its order until cancellation; late results cannot corrupt a new attempt', () => {
  const store = createAttemptStore(memoryStorage())
  store.getOrCreate('u', 'v', () => 'first')
  store.saveResult('u', 'v', {
    requestId: 'first',
    orderId: '90071992547409999',
    status: 'SUCCEEDED'
  })
  assert.equal(store.getOrCreate('u', 'v', () => 'second').requestId, 'first')
  store.saveResult('u', 'v', { requestId: 'first', status: 'NOT_FOUND' })
  assert.equal(store.read('u', 'v').status, 'SUCCEEDED')
  store.markCancelled('u', 'v', 'different-order')
  assert.equal(store.read('u', 'v').status, 'SUCCEEDED')
  store.markCancelled('u', 'v', '90071992547409999')
  store.getOrCreate('u', 'v', () => 'second')
  store.saveResult('u', 'v', { requestId: 'first', status: 'SUCCEEDED' })
  assert.equal(store.read('u', 'v').requestId, 'second')
})

test('failed persistent storage refuses a new request before any submission can occur', () => {
  const store = createAttemptStore({
    getItem: () => null,
    setItem: () => {
      throw new Error('full')
    }
  })
  assert.throws(() => store.getOrCreate('u', 'v', () => 'request'), /full/)
})

test('cancellation can resolve a persisted unknown intent after its acceptance response was lost', () => {
  const store = createAttemptStore(memoryStorage())
  store.getOrCreate('u', 'v', () => 'lost-response')
  store.saveResult('u', 'v', {
    requestId: 'lost-response',
    orderId: 'owned-order',
    status: 'CANCELLED'
  })
  assert.equal(store.read('u', 'v').status, 'CANCELLED')
  assert.equal(
    store.getOrCreate('u', 'v', () => 'new-purchase').requestId,
    'new-purchase'
  )
})

test('polling uses one-second intervals and a 15-second window, retaining an unresolved request', async () => {
  let time = 0
  let calls = 0
  const results = []
  const outcome = await pollAttempt({
    query: async () => {
      calls++
      return { requestId: 'r', status: 'NOT_FOUND' }
    },
    onResult: (result) => results.push(result),
    now: () => time,
    sleep: async (ms) => {
      assert.equal(ms, 1000)
      time += ms
    }
  })
  assert.equal(outcome, null)
  assert.equal(calls, 15)
  assert.equal(results.length, 15)
  assert.equal(time, 15000)
})

test('polling survives temporary failures but stops immediately on a persisted terminal result', async () => {
  let time = 0
  let calls = 0
  const outcome = await pollAttempt({
    query: async () => {
      if (++calls === 1) throw Object.assign(new Error(), { status: 503 })
      return { status: 'SUCCEEDED', orderId: 'o' }
    },
    now: () => time,
    sleep: async (ms) => {
      time += ms
    }
  })
  assert.equal(outcome.status, 'SUCCEEDED')
  assert.equal(calls, 2)
})

test('authentication failures escape polling rather than becoming a false timeout', async () => {
  await assert.rejects(
    pollAttempt({
      query: async () => {
        throw Object.assign(new Error('login'), { status: 401 })
      }
    }),
    /login/
  )
})

test('each query receives its remaining deadline so a slow response cannot add a fresh timeout window', async () => {
  let time = 0
  const budgets = []
  await pollAttempt({
    query: async (remainingMs) => {
      budgets.push(remainingMs)
      time += 6000
      return { status: 'PROCESSING' }
    },
    now: () => time,
    sleep: async (ms) => {
      time += ms
    }
  })
  assert.deepEqual(budgets, [15000, 8000, 1000])
})

test('HTTP errors preserve status, stable code and retry information', () => {
  for (const [status, code, retryable] of [
    [401, 'UNAUTHENTICATED', false],
    [403, 'FORBIDDEN', false],
    [429, 'RATE_LIMITED', true],
    [503, 'UNAVAILABLE', true]
  ]) {
    const err = normalizeRequestError({ response: { status, data: {} } })
    assert.ok(err instanceof Error)
    assert.equal(err.status, status)
    assert.equal(err.code, code)
    assert.equal(err.retryable, retryable)
  }
  const specific = normalizeRequestError({
    response: { status: 503, data: { code: 'REBUILDING', errorMsg: '恢复中' } }
  })
  assert.equal(specific.code, 'REBUILDING')
  assert.equal(specific.message, '恢复中')
})

test('Kafka acknowledged intent keeps QUEUED across NOT_FOUND and reload', () => {
  const storage = memoryStorage()
  const store = createAttemptStore(storage)
  store.getOrCreate('u', 'v', () => 'queued-request')
  store.saveResult('u', 'v', {
    requestId: 'queued-request',
    orderId: '123',
    status: 'QUEUED'
  })
  store.saveResult('u', 'v', {
    requestId: 'queued-request',
    status: 'NOT_FOUND'
  })
  assert.equal(store.read('u', 'v').status, 'QUEUED')
  assert.equal(
    createAttemptStore(storage).getOrCreate('u', 'v', () => 'new-id').requestId,
    'queued-request'
  )
})
