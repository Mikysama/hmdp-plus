import { defineStore } from 'pinia'
import { ref } from 'vue'

// 用户模块
export const useUserStore = defineStore(
  'Hmdp-User',
  () => {
    const token = ref('') // 定义 token
    const setToken = (t) => {
      token.value = t
    } // 设置 token
    const getToken = () => token.value

    // 创建个人信息的ref
    const userInfo = ref({})
    const getUserInfo = () => userInfo.value
    const setUserInfo = (obj) => (userInfo.value = obj)
    const resetUserInfo = () => {
      userInfo.value = {}
    }
    const resetSession = () => {
      token.value = ''
      userInfo.value = {}
    }

    return {
      token,
      setToken,
      getToken,
      userInfo,
      getUserInfo,
      setUserInfo,
      resetUserInfo,
      resetSession
    }
  },
  {
    persist: true // 持久化
  }
)
