<script setup>
import { computed } from 'vue'
import { UserOutlined, LockOutlined } from '@ant-design/icons-vue'

// 错误文案：若后端在 401 时回显错误，或 302 时把错误写入 ?error= 查询参数，则读取之
const error = computed(() => {
  if (typeof window === 'undefined') return ''
  const e = new URLSearchParams(window.location.search).get('error')
  return e || ''
})
</script>

<template>
  <div class="login-wrap">
    <div class="login-card">
      <div class="login-header">
        <h1 class="login-title">JNFS 运维监控中心</h1>
        <span class="login-subtitle">请登录以访问 Dashboard</span>
      </div>

      <a-alert
        v-if="error"
        type="error"
        show-icon
        :message="error"
        style="margin-bottom: 16px"
      />

      <!--
        后端 POST /login 是表单刷新式（成功 302 跳 /，失败 401），
        因此这里用原生 form 直接提交，不要用 JSON fetch。
        不套 a-form：HTML 不允许 form 嵌套，浏览器会忽略内层 <form>，
        仅用 a-form-item 做布局（a-form-item 不输出 form 标签），
        input 的 name 仍归属外层原生 form 提交。
      -->
      <form method="POST" action="/login" class="login-form">
        <div class="login-field">
          <a-input
            name="username"
            placeholder="用户名"
            autocomplete="username"
            autofocus
            size="large"
          >
            <template #prefix>
              <UserOutlined style="color: rgba(0,0,0,0.25)" />
            </template>
          </a-input>
        </div>
        <div class="login-field">
          <a-input-password
            name="password"
            placeholder="密码"
            autocomplete="current-password"
            size="large"
          >
            <template #prefix>
              <LockOutlined style="color: rgba(0,0,0,0.25)" />
            </template>
          </a-input-password>
        </div>
        <a-button type="primary" html-type="submit" size="large" block>
          登录
        </a-button>
      </form>
    </div>
  </div>
</template>

<style scoped>
.login-wrap {
  min-height: 100vh;
  display: flex;
  justify-content: center;
  align-items: center;
  background: linear-gradient(135deg, #3498db 0%, #2c3e50 100%);
}
.login-card {
  width: 400px;
  max-width: 90vw;
  padding: 40px 32px 32px;
  background: #fff;
  border-radius: 8px;
  box-shadow: 0 8px 24px rgba(0, 0, 0, 0.15);
}
.login-header {
  text-align: center;
  margin-bottom: 32px;
}
.login-title {
  margin: 0 0 8px;
  font-size: 1.5rem;
  color: #2c3e50;
  font-weight: 600;
}
.login-subtitle {
  display: block;
  color: rgba(0, 0, 0, 0.45);
  font-size: 0.85rem;
}
.login-form {
  /* no extra layout needed — fields stack naturally */
}
.login-field {
  margin-bottom: 20px;
}
</style>
