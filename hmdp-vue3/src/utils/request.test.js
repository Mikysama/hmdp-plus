import { describe, expect, it, vi } from 'vitest'
import {
  clearUnauthorizedSession,
  getBusinessErrorMessage,
  getRequestErrorMessage
} from './requestHelpers'

describe('request helpers', () => {
  it('extracts backend business errors', () => {
    expect(
      getBusinessErrorMessage({ success: false, errorMsg: '密码错误' })
    ).toBe('密码错误')
    expect(getBusinessErrorMessage({ success: true })).toBeNull()
  })

  it('uses a safe network error fallback', () => {
    expect(
      getRequestErrorMessage({ response: { data: { errorMsg: '服务错误' } } })
    ).toBe('服务错误')
    expect(getRequestErrorMessage({})).toBe('网络异常，请稍后重试')
  })

  it('clears the session and redirects on unauthorized responses', () => {
    const store = { resetSession: vi.fn() }
    const navigate = vi.fn()

    clearUnauthorizedSession(store, '/blogEdit', navigate)

    expect(store.resetSession).toHaveBeenCalledOnce()
    expect(navigate).toHaveBeenCalledWith('/login')
  })
})
