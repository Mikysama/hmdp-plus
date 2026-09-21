// 引入axios
import axios from 'axios'
import JSONbig from 'json-bigint'
import { useUserStore } from '@/stores'
import router from '@/router'
import {
  clearUnauthorizedSession,
  getBusinessErrorMessage,
  getRequestErrorMessage
} from './requestHelpers'
const baseURL = '/api'

const instance = axios.create({
  baseURL,
  timeout: 10000,
  // 使用 json-bigint 将超过安全整数范围的数以字符串存储，避免精度丢失
  transformResponse: [
    function (data) {
      try {
        // axios 传入的 data 是原始字符串
        const parser = JSONbig({ storeAsString: true })
        return data ? parser.parse(data) : data
      } catch {
        // 非 JSON 或解析失败，按原始返回
        return data
      }
    }
  ]
})
// 请求拦截器
instance.interceptors.request.use(
  (config) => {
    const userStore = useUserStore()
    if (userStore.token) {
      config.headers.Authorization = `${userStore.token}`
    }
    return config
  },
  (error) => Promise.reject(error)
)
//响应拦截器
instance.interceptors.response.use(
  (response) => {
    const data = response.data
    const message = getBusinessErrorMessage(data)
    if (message) {
      ElMessage.error(message)
      return Promise.reject(new Error(message))
    }
    return data
  },
  (error) => {
    if (error.response?.status === 401) {
      const userStore = useUserStore()
      clearUnauthorizedSession(
        userStore,
        router.currentRoute.value.path,
        (path) => router.push(path)
      )
      ElMessage.error('请先登录')
      return Promise.reject(error)
    }
    const message = getRequestErrorMessage(error)
    ElMessage.error(message)
    return Promise.reject(error)
  }
)
export default instance
export { baseURL }
