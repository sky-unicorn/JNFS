<script setup>
import { ref, computed, watch } from 'vue'
import {
  KeyOutlined,
  LogoutOutlined,
  DesktopOutlined,
  ClusterOutlined
} from '@ant-design/icons-vue'
import {
  authEnabled,
  noRedundancy,
  storageMode,
  apiGet
} from '../api/client'
import NodesView from './NodesView.vue'
import GroupsView from './redundancy/GroupsView.vue'
import SyncView from './redundancy/SyncView.vue'
import PolicyView from './redundancy/PolicyView.vue'
import AlertsView from './redundancy/AlertsView.vue'
import ChangePasswordModal from '../components/ChangePasswordModal.vue'

const topTab = ref('nodes')
const subTab = ref('groups')

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

// 节点 tab 激活时，确保冗余组也加载一次（供 NodesView 判断节点是否归属任意组）；
// 单机模式下 503 直接忽略
const nodesActive = computed(() => topTab.value === 'nodes')
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

// 子页激活态（active 门控，切 tab 即重启轮询）
const groupsActive = computed(
  () => topTab.value === 'redundancy' && subTab.value === 'groups'
)
const syncActive = computed(
  () => topTab.value === 'redundancy' && subTab.value === 'sync'
)
const policyActive = computed(
  () => topTab.value === 'redundancy' && subTab.value === 'policy'
)
const alertsActive = computed(
  () => topTab.value === 'redundancy' && subTab.value === 'alerts'
)
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

    <a-layout-content style="padding: 24px; background: #f4f7f6;">
      <div style="max-width: 1200px; margin: 0 auto;">
        <a-tabs v-model:active-key="topTab">
          <a-tab-pane key="nodes">
            <template #tab>
              <span><DesktopOutlined /> 节点监控</span>
            </template>
            <NodesView
              :active="nodesActive"
              :groups="groups"
              @update:nodes="nodes = $event"
            />
          </a-tab-pane>

          <a-tab-pane key="redundancy">
            <template #tab>
              <span><ClusterOutlined /> 冗余存储管理</span>
            </template>

            <!-- 单机嵌入式（file/h2）模式：无冗余提示 -->
            <a-alert
              v-if="noRedundancy"
              type="info"
              show-icon
              style="margin-bottom: 12px"
              :message="`单机嵌入式·无冗余：当前 ${storageMode.toUpperCase()} 模式为单副本存储，冗余存储管理不可用（冗余 API 返回 503）。`"
            />

            <a-tabs v-model:active-key="subTab">
              <a-tab-pane key="groups" tab="冗余组管理">
                <GroupsView
                  :active="groupsActive"
                  :nodes="nodes"
                  @update:groups="groups = $event"
                />
              </a-tab-pane>

              <a-tab-pane key="sync" tab="对账同步">
                <SyncView :active="syncActive" />
              </a-tab-pane>

              <a-tab-pane key="policy" tab="同步策略">
                <PolicyView :active="policyActive" />
              </a-tab-pane>

              <a-tab-pane key="alerts">
                <template #tab>
                  <span>
                    告警
                    <a-badge
                      :count="alertCount"
                      :show-zero="false"
                      :offset="[8, -2]"
                      :number-style="{ backgroundColor: '#c62828' }"
                    />
                  </span>
                </template>
                <AlertsView
                  :active="alertsActive"
                  :nodes="nodes"
                  :groups="groups"
                  @update:count="alertCount = $event"
                />
              </a-tab-pane>
            </a-tabs>
          </a-tab-pane>
        </a-tabs>
      </div>
    </a-layout-content>

    <ChangePasswordModal v-model:visible="pwdVisible" />
  </a-layout>
</template>