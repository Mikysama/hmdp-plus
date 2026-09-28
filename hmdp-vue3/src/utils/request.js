// 引入axios
import axios from 'axios'
import JSONbig from 'json-bigint'
import { useUserStore } from '@/stores'
import router from '@/router'
import { ElMessage } from 'element-plus'
import { normalizeRequestError } from './requestError'
const baseURL = '/api'
const instance = axios.create({
  // TODO 1.设置基础地址和超时时间
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
    // TODO 2.请求头里添加token
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
    return response.data
  },
  (error) => {
    const normalized = normalizeRequestError(error)
    if (normalized.status === 401) {
      router.push('/login')
    }
    if (!error.config?.silentError) ElMessage.error(normalized.message)
    return Promise.reject(normalized)
  }
)
export default instance
export { baseURL }
