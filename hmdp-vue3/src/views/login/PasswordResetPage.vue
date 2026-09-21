<script setup>
import { ref } from 'vue'
import { ArrowLeft } from '@element-plus/icons-vue'
import { userGetCode, userResetPassword } from '@/api/user'
import router from '@/router'

const formRef = ref()
const sending = ref(false)
const codeButtonText = ref('发送验证码')
const form = ref({ phone: '', code: '', newPassword: '' })

const rules = {
  phone: [
    { required: true, message: '请输入手机号', trigger: 'blur' },
    { pattern: /^1[3-9]\d{9}$/, message: '手机号格式不正确', trigger: 'blur' }
  ],
  code: [{ required: true, message: '请输入验证码', trigger: 'blur' }],
  newPassword: [
    { required: true, message: '请输入新密码', trigger: 'blur' },
    { min: 8, max: 64, message: '密码长度必须为8到64位', trigger: 'blur' }
  ]
}

const sendCode = async () => {
  try {
    await formRef.value.validateField('phone')
    const res = await userGetCode(form.value.phone)
    form.value.code = res.data
    sending.value = true
    let seconds = 60
    codeButtonText.value = `${seconds}s后重发`
    const timer = window.setInterval(() => {
      seconds--
      codeButtonText.value = `${seconds}s后重发`
      if (seconds <= 0) {
        window.clearInterval(timer)
        sending.value = false
        codeButtonText.value = '发送验证码'
      }
    }, 1000)
  } catch {
    sending.value = false
  }
}

const submit = async () => {
  try {
    await formRef.value.validate()
    await userResetPassword(form.value)
    ElMessage.success('密码设置成功')
    router.push('/login')
  } catch {
    // 表单或请求错误已由组件和响应拦截器提示
  }
}
</script>

<template>
  <div class="login-container">
    <div class="header">
      <div class="header-back-btn" @click="router.back()">
        <el-icon><ArrowLeft /></el-icon>
      </div>
      <div class="header-title">设置/重置密码</div>
    </div>
    <div class="content">
      <el-form ref="formRef" :model="form" :rules="rules" label-width="0">
        <el-form-item prop="phone">
          <el-input v-model="form.phone" placeholder="请输入手机号" />
        </el-form-item>
        <el-form-item prop="code">
          <div class="code-row">
            <el-input v-model="form.code" placeholder="请输入验证码" />
            <el-button :disabled="sending" @click="sendCode">{{
              codeButtonText
            }}</el-button>
          </div>
        </el-form-item>
        <el-form-item prop="newPassword">
          <el-input
            v-model="form.newPassword"
            type="password"
            show-password
            placeholder="8-64位新密码"
          />
        </el-form-item>
        <el-button class="submit" @click="submit">确认设置</el-button>
      </el-form>
    </div>
  </div>
</template>

<style scoped>
@import '@/assets/css/login.css';

.code-row {
  display: flex;
  width: 100%;
  gap: 8px;
}

.submit {
  width: 100%;
  background: #f63;
  color: #fff;
}
</style>
