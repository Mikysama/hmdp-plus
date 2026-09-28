const defaults = {
  401: ['UNAUTHENTICATED', '请先登录'],
  403: ['FORBIDDEN', '无权执行此操作'],
  429: ['RATE_LIMITED', '请求过于频繁，请稍后重试'],
  503: ['UNAVAILABLE', '服务暂时不可用，请稍后重试']
}

export function normalizeRequestError(original) {
  const status = original.response?.status ?? 0
  const body = original.response?.data || {}
  const [fallbackCode, fallbackMessage] = defaults[status] || [
    'REQUEST_FAILED',
    '网络或服务异常，请稍后重试'
  ]
  const error = new Error(body.errorMsg || body.message || fallbackMessage)
  error.status = status
  error.code = body.code || body.reasonCode || fallbackCode
  error.retryable = status === 0 || status === 429 || status >= 500
  error.cause = original
  return error
}
