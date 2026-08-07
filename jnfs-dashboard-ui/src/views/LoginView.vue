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
    <a-row justify="center" align="middle" style="min-height: 100vh">
      <a-col :xs="22" :sm="16" :md="12" :lg="8" :xl="6">
        <a-card style="text-align: center">
          <h1 style="margin: 0 0 0.5rem 0; font-size: 1.4rem; color: #2c3e50">
            JNFS 运维监控中心
          </h1>
          <a-typography-text type="secondary" style="display: block; margin-bottom: 1.5rem; font-size: 0.85rem;">
            请登录以访问 Dashboard
          </a-typography-text>

          <a-alert
            v-if="error"
            type="error"
            show-icon
            :message="error"
            style="margin-bottom: 1rem; text-align: left"
          />

          <!--
            后端 POST /login 是表单刷新式（成功 302 跳 /，失败 401），
            因此这里用原生 form 直接提交，不要用 JSON fetch。
            不套 a-form：HTML 不允许 form 嵌套，浏览器会忽略内层 <form>，
            仅用 a-form-item 做布局（a-form-item 不输出 form 标签），
            input 的 name 仍归属外层原生 form 提交。
          -->
          <form method="POST" action="/login">
            <a-form-item label="用户名">
              <a-input
                name="username"
                placeholder="请输入用户名"
                autocomplete="username"
                autofocus
              >
                <template #prefix>
                  <UserOutlined />
                </template>
              </a-input>
            </a-form-item>
            <a-form-item label="密码">
              <a-input-password
                name="password"
                placeholder="请输入密码"
                autocomplete="current-password"
              >
                <template #prefix>
                  <LockOutlined />
                </template>
              </a-input-password>
            </a-form-item>
            <a-button type="primary" html-type="submit" block>登录</a-button>
          </form>
        </a-card>
      </a-col>
    </a-row>
  </div>
</template>

<style scoped>
.login-wrap {
  min-height: 100vh;
  background: linear-gradient(135deg, #3498db 0%, #2c3e50 100%);
}
</style>
