<script setup>
import { computed } from 'vue'
import { authEnabled } from './api/client'
import LoginView from './views/LoginView.vue'
import Dashboard from './views/Dashboard.vue'

// 仅当 pathname==='/login' 且启用了鉴权时渲染登录页
const isLogin = computed(
  () => window.location.pathname === '/login' && authEnabled
)
</script>

<template>
  <LoginView v-if="isLogin" />
  <Dashboard v-else />
</template>
