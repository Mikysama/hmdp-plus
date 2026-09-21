export const getBusinessErrorMessage = (data) =>
  data?.success === false ? data.errorMsg || '请求处理失败' : null

export const getRequestErrorMessage = (error) =>
  error.response?.data?.errorMsg || error.message || '网络异常，请稍后重试'

export const clearUnauthorizedSession = (userStore, currentPath, navigate) => {
  userStore.resetSession()
  if (currentPath !== '/login') {
    navigate('/login')
  }
}
