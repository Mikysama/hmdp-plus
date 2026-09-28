const terminalStatuses = new Set(['SUCCEEDED', 'FAILED', 'CANCELLED'])
export const isTerminalAttempt = (attempt) =>
  terminalStatuses.has(attempt?.status)
export const isPendingAttempt = (attempt) =>
  !!attempt && !isTerminalAttempt(attempt)

// A persisted intent is written before networking. Do not silently fall back to
// memory: a reload would then lose the ID used for an uncertain server commit.
export function createAttemptStore(storage) {
  const key = (userId, voucherId) => {
    if (!userId || !voucherId) throw new Error('请先确认登录用户身份')
    return `seckill:v2:attempt:${encodeURIComponent(userId)}:${encodeURIComponent(voucherId)}`
  }
  const read = (userId, voucherId) => {
    const raw = storage.getItem(key(userId, voucherId))
    if (!raw) return null
    const attempt = JSON.parse(raw)
    if (
      !attempt.requestId ||
      attempt.userId !== String(userId) ||
      attempt.voucherId !== String(voucherId)
    ) {
      throw new Error('本地抢购记录损坏，请先查询订单，勿重复提交')
    }
    return attempt
  }
  const write = (userId, voucherId, attempt) => {
    storage.setItem(key(userId, voucherId), JSON.stringify(attempt))
    return attempt
  }
  const getOrCreate = (
    userId,
    voucherId,
    newId = () => globalThis.crypto.randomUUID()
  ) => {
    const existing = read(userId, voucherId)
    if (existing && !['FAILED', 'CANCELLED'].includes(existing.status))
      return existing
    return write(userId, voucherId, {
      userId: String(userId),
      voucherId: String(voucherId),
      requestId: newId(),
      status: 'UNKNOWN',
      createdAt: Date.now()
    })
  }
  const saveResult = (userId, voucherId, result) => {
    const existing = read(userId, voucherId)
    if (!existing || existing.requestId !== result?.requestId) return existing
    if (isTerminalAttempt(existing) && result.status !== 'CANCELLED')
      return existing
    return write(userId, voucherId, {
      ...existing,
      status: result.status === 'NOT_FOUND' ? existing.status : result.status,
      orderId:
        result.orderId != null ? String(result.orderId) : existing.orderId,
      reasonCode: result.reasonCode,
      expiresAt: result.expiresAt ?? existing.expiresAt
    })
  }
  const markCancelled = (userId, voucherId, orderId) => {
    const existing = read(userId, voucherId)
    if (!existing || existing.orderId !== String(orderId)) return existing
    return saveResult(userId, voucherId, {
      requestId: existing.requestId,
      status: 'CANCELLED'
    })
  }
  return { read, getOrCreate, saveResult, markCancelled }
}

export async function pollAttempt({
  query,
  onResult = () => {},
  now = Date.now,
  sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
  isActive = () => true,
  delay = 1000,
  timeoutMs = 15000
}) {
  const end = now() + timeoutMs
  while (isActive() && now() < end) {
    try {
      const result = await query(end - now())
      if (!isActive()) return null
      if (result) onResult(result)
      if (isTerminalAttempt(result)) return result
    } catch (error) {
      if (error.status === 401 || error.status === 403) throw error
      // Network failures do not establish a FAILED order status.
    }
    const remaining = end - now()
    if (remaining > 0 && isActive()) await sleep(Math.min(delay, remaining))
  }
  return null
}
