<script setup>
import { ref, computed, watch } from 'vue'
import {
  KeyOutlined,
  LogoutOutlined,
  DesktopOutlined,
  ClusterOutlined,
  FileTextOutlined
} from '@ant-design/icons-vue'
import {
  authEnabled,
  storageMode,
  apiGet
} from '../api/client'
import NodesView from './NodesView.vue'
import FilesView from './FilesView.vue'
import GroupsView from './redundancy/GroupsView.vue'
import SyncView from './redundancy/SyncView.vue'
import AlertsView from './redundancy/AlertsView.vue'
import ChangePasswordModal from '../components/ChangePasswordModal.vue'

// 单一激活键：'nodes' | 'files' | 'redundancy/groups' | 'redundancy/sync' | 'redundancy/alerts'
const activeKey = ref('nodes')
// 左侧菜单展开态（冗余存储管理子菜单默认展开）
const openKeys = ref(['redundancy'])

// 改密弹窗
const pwdVisible = ref(false)
function openPwd() {
  pwdVisible.value = true
}

// 登出：后端清 Cookie 后跳 /login
function logout() {
  window.location.href = '/logout'
}

// 告警红点计数（由 AlertsView 上报）
const alertCount = ref(0)

// 共享数据：节点列表与冗余组列表（由对应子视图加载后上报）
const nodes = ref([])
const groups = ref([])

// 当前是否处于冗余管理任一子页（用于展示单机模式空态提示）
const isRedundancy = computed(() => activeKey.value.startsWith('redundancy'))

// 节点页激活时，确保冗余组也加载一次（供 NodesView 判断节点是否归属任意组）；
// 单机模式下 503 直接忽略
const nodesActive = computed(() => activeKey.value === 'nodes')
async function ensureGroupsOnce() {
  if (groups.value.length > 0) return
  try {
    const d = await apiGet('/api/replication/groups')
    groups.value = d.groups || []
  } catch (e) {
    // 单机模式 503，忽略
  }
}
watch(nodesActive, a => {
  if (a) ensureGroupsOnce()
}, { immediate: true })

// 子页激活态（active 门控，切菜单即重启轮询）
const filesActive = computed(() => activeKey.value === 'files')
const groupsActive = computed(() => activeKey.value === 'redundancy/groups')
const syncActive = computed(() => activeKey.value === 'redundancy/sync')
const alertsActive = computed(() => activeKey.value === 'redundancy/alerts')

// 左侧菜单点击：key 即激活键
function onMenuClick({ key }) {
  activeKey.value = key
}
</script>

<template>
  <a-layout style="min-height: 100vh">
    <a-layout-header
      style="display: flex; justify-content: space-between; align-items: center; padding: 0 24px;"
    >
      <h1 style="margin: 0; color: #fff; font-size: 1.3rem;">JNFS 运维监控中心</h1>
      <a-space v-if="authEnabled" :size="12">
        <a-button @click="openPwd">
          <template #icon><KeyOutlined /></template>
          修改密码
        </a-button>
        <a-button @click="logout">
          <template #icon><LogoutOutlined /></template>
          登出
        </a-button>
      </a-space>
    </a-layout-header>

    <a-layout>
      <a-layout-sider width="220" :style="{ background: '#fff', borderRight: '1px solid #f0f0f0' }">
        <a-menu
          mode="inline"
          theme="light"
          :selectedKeys="[activeKey]"
          v-model:openKeys="openKeys"
          @click="onMenuClick"
          style="height: 100%; border-inline-end: none;"
        >
          <a-menu-item key="nodes">
            <template #icon><DesktopOutlined /></template>
            节点监控
          </a-menu-item>

          <a-menu-item key="files">
            <template #icon><FileTextOutlined /></template>
            文件管理
          </a-menu-item>

          <a-sub-menu key="redundancy">
            <template #icon><ClusterOutlined /></template>
            <template #title>冗余存储管理</template>
            <a-menu-item key="redundancy/groups">冗余组管理</a-menu-item>
            <a-menu-item key="redundancy/sync">对账同步</a-menu-item>
            <a-menu-item key="redundancy/alerts">
              <span>告警</span>
              <a-badge
                v-if="alertCount > 0"
                :count="alertCount"
                :offset="[8, -2]"
                :number-style="{ backgroundColor: '#c62828' }"
                style="margin-left: 8px"
              />
            </a-menu-item>
          </a-sub-menu>
        </a-menu>
      </a-layout-sider>

      <a-layout-content style="padding: 24px; background: #f4f7f6;">
        <!-- file 模式（已退役，防御性）：冗余不可用 -->
        <a-alert
          v-if="isRedundancy && storageMode === 'file'"
          type="warning"
          show-icon
          style="margin-bottom: 12px"
          message="单机 file 模式·无冗余：当前为单副本存储，冗余存储管理不可用（冗余 API 返回 503）。"
        />
        <!-- h2 模式：同机部署提示（副本在同机不同磁盘，无跨机物理隔离） -->
        <a-alert
          v-if="isRedundancy && storageMode === 'h2'"
          type="info"
          show-icon
          style="margin-bottom: 12px"
          message="H2 单机模式：副本建议部署在同一台机器的不同磁盘上；本模式不提供跨机器物理隔离，请确保磁盘独立可用。"
        />

        <NodesView
          v-if="activeKey === 'nodes'"
          :active="nodesActive"
          :groups="groups"
          @update:nodes="nodes = $event"
        />

        <FilesView v-else-if="activeKey === 'files'" :active="filesActive" />

        <GroupsView
          v-else-if="activeKey === 'redundancy/groups'"
          :active="groupsActive"
          :nodes="nodes"
          @update:groups="groups = $event"
        />

        <SyncView v-else-if="activeKey === 'redundancy/sync'" :active="syncActive" />

        <AlertsView
          v-else-if="activeKey === 'redundancy/alerts'"
          :active="alertsActive"
          :nodes="nodes"
          :groups="groups"
          @update:count="alertCount = $event"
        />
      </a-layout-content>
    </a-layout>

    <ChangePasswordModal v-model:visible="pwdVisible" />
  </a-layout>
</template>