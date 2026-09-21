import { beforeEach, describe, expect, it } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { useUserStore } from './user'

describe('user store', () => {
  beforeEach(() => setActivePinia(createPinia()))

  it('clears token and user information together', () => {
    const store = useUserStore()
    store.setToken('token-value')
    store.setUserInfo({ id: '1' })

    store.resetSession()

    expect(store.token).toBe('')
    expect(store.userInfo).toEqual({})
  })
})
