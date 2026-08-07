<script setup>
import { ref, computed, watch, onUnmounted, h } from 'vue'
import { Modal, message } from 'ant-design-vue'
import {
  apiGet,
  apiPost,
  formatBytes,
  storageMode
} from '../api/client'
import StatCard from '../components/StatCard.vue'

/** HTML 实体转义：用于 Modal.confirm 拼接 innerHTML 之前先对动态值（nodeId / groupId）做转义 */
function escapeHtml(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
  }[c]))
}
/** 渲染带 innerHTML 的 VNode（Modal.confirm 字符串 content 会按纯文本转义） */
function confirmHtml(html) {
  return h('div', { innerHTML: html })
}

const props = defineProps({
  // 是否激活（顶层 tab 为 nodes 时才轮询）
  active: { type: Boolean, default: false },
  // 冗余组（Dashboard 注入，用于判断节点是否归属任意组，决定排空/晋升按钮可见性）
  groups: { type: Array, default: () => [] }
})
const emit = defineEmits(['update:nodes'])

const nodes = ref([])
const loading = ref(false)
const securityConfigured = ref(null)
const connectFailed = ref(false)

// 节点是否归属任意冗余组：直接基于响应式 props.groups 计算，避免绕道模块级非响应式缓存
const groupNodeIds = computed(() => {
  const s = new Set()
  ;(props.groups || []).forEach(g =>
    (g.nodeIds || []).forEach(id => s.add(id))
  )
  return s
})
function inAnyGroup(nodeId) {
  return groupNodeIds.value.has(nodeId)
}

/* ===================== 加载节点（轮询 2s） ===================== */
async function loadNodes() {
  loading.value = true
  try {
    const data = await apiGet('/api/nodes')
    nodes.value = data || []
    connectFailed.value = false
    emit('update:nodes', nodes.value)
  } catch (e) {
    nodes.value = []
    connectFailed.value = true
    // 高频轮询，连接失败不弹全局错（表格空态呈现）
  } finally {
    loading.value = false
  }
}

async function loadSecurity() {
  try {
    const d = await apiGet('/api/security')
    securityConfigured.value = d.securityConfigured
  } catch (e) {
    // 安全状态加载失败保持 '加载中...'
  }
}

/* ===================== 统计 ===================== */
const activeCount = computed(
  () =>
    nodes.value.filter(n => n.status === 'online' && n.drainStatus !== 1)
      .length
)
const totalFreeSpace = computed(() =>
  nodes.value
    .filter(n => n.status === 'online' && n.drainStatus !== 1)
    .reduce((s, n) => s + n.freeSpace, 0)
)
const securityText = computed(() => {
  if (securityConfigured.value === null) return '加载中...'
  return securityConfigured.value ? '已配置 (自定义令牌)' : '⚠ 使用默认令牌'
})
const securityColor = computed(() => {
  if (securityConfigured.value === null) return '#e67e22'
  return securityConfigured.value ? '#2e7d32' : '#e67e22'
})

/* ===================== 操作 ===================== */
function drainNode(nodeId) {
  Modal.confirm({
    title: '确认排空节点',
    content: confirmHtml(
      '将节点 <b>' + escapeHtml(nodeId) + '</b> 标记为排空？<br>' +
      '· 后续上传不再选中该节点（新写入选路排除）<br>' +
      '· 已有数据可继续读（读可用性不受影响）<br>' +
      '· 物理数据由同步任务后台搬运，本操作不删除数据'
    ),
    okText: '确定',
    cancelText: '取消',
    onOk: async () => {
      try {
        const res = await apiPost(
          '/api/nodes/' + encodeURIComponent(nodeId) + '/drain',
          { drain: true }
        )
        message.success(res.message || '已标记排空')
        loadNodes()
      } catch (err) {
        message.error('排空失败: ' + err.message)
      }
    }
  })
}

function recoverNode(nodeId) {
  Modal.confirm({
    title: '确认恢复节点',
    content: confirmHtml(
      '取消节点 <b>' + escapeHtml(nodeId) + '</b> 的排空状态？<br>' +
      '该节点将重新进入新上传的选路候选。'
    ),
    okText: '确定',
    cancelText: '取消',
    onOk: async () => {
      try {
        const res = await apiPost(
          '/api/nodes/' + encodeURIComponent(nodeId) + '/drain',
          { drain: false }
        )
        message.success(res.message || '已恢复')
        loadNodes()
      } catch (err) {
        message.error('恢复失败: ' + err.message)
      }
    }
  })
}

// 晋升需要 groupId：从组列表中查找该节点所属组
async function promoteNode(nodeId) {
  try {
    const d = await apiGet('/api/replication/groups')
    const groups = d.groups || []
    let belongs = null
    for (let i = 0; i < groups.length; i++) {
      const g = groups[i]
      if ((g.nodeIds || []).includes(nodeId)) {
        belongs = g
        break
      }
    }
    if (!belongs) {
      message.warning('节点 ' + nodeId + ' 未归属任何冗余组，无法晋升')
      return
    }
    Modal.confirm({
      title: '确认晋升副本',
      content: confirmHtml(
        '将把冗余组 <b>' + escapeHtml(belongs.groupId) + '</b> 中位于节点 <b>' +
        escapeHtml(nodeId) + '</b> 上的副本提升为 PRIMARY，<br>' +
        '该组原有的 PRIMARY 副本将被降级为 SECONDARY。'
      ),
      okText: '确定',
      cancelText: '取消',
      onOk: async () => {
        try {
          const res = await apiPost(
            '/api/nodes/' + encodeURIComponent(nodeId) + '/promote',
            {}
          )
          message.success(res.message || '晋升完成')
        } catch (err) {
          message.error('晋升失败: ' + err.message)
        }
      }
    })
  } catch (err) {
    message.error('无法加载冗余组: ' + err.message)
  }
}

/* ===================== 表格列 ===================== */
const columns = [
  { title: '节点ID', dataIndex: 'nodeId', width: '18%' },
  { title: '节点地址', dataIndex: 'address', width: '18%' },
  { title: '剩余空间', dataIndex: 'freeSpace', key: 'freeSpace', width: '14%' },
  { title: '最后心跳时间', dataIndex: 'lastHeartbeat', key: 'lastHeartbeat', width: '20%' },
  { title: '状态', dataIndex: 'status', key: 'status', width: '12%' },
  { title: '操作', dataIndex: 'action', key: 'action', width: '18%' }
]

/* ===================== 轮询（active 门控） ===================== */
let timer = null
function startPolling() {
  loadSecurity()
  loadNodes()
  timer = setInterval(loadNodes, 2000)
}
function stopPolling() {
  if (timer) {
    clearInterval(timer)
    timer = null
  }
}

watch(
  () => props.active,
  a => {
    if (a) startPolling()
    else stopPolling()
  },
  { immediate: true }
)

onUnmounted(stopPolling)

defineExpose({ loadNodes })
</script>

<template>
  <div>
    <a-row :gutter="[16, 16]" style="margin-bottom: 16px">
      <a-col :xs="12" :lg="6">
        <StatCard title="活跃存储节点" :value="activeCount" color="#2c3e50" />
      </a-col>
      <a-col :xs="12" :lg="6">
        <StatCard title="全网剩余容量" :value="formatBytes(totalFreeSpace)" color="#2c3e50" />
      </a-col>
      <a-col :xs="12" :lg="6">
        <StatCard title="安全状态" :value="securityText" :color="securityColor" />
      </a-col>
      <a-col :xs="12" :lg="6">
        <StatCard title="存储模式" :value="storageMode.toUpperCase()" color="#2c3e50" />
      </a-col>
    </a-row>

    <a-card>
      <a-table
        :columns="columns"
        :data-source="nodes"
        :loading="loading"
        :pagination="false"
        row-key="nodeId"
        size="middle"
        :locale="{ emptyText: connectFailed ? '无法连接到服务器' : '暂无节点连接' }"
      >
        <template #bodyCell="{ column, record }">
          <template v-if="column.key === 'freeSpace'">
            {{ formatBytes(record.freeSpace) }}
          </template>
          <template v-else-if="column.key === 'lastHeartbeat'">
            {{ record.lastHeartbeat ? new Date(record.lastHeartbeat).toLocaleString() : '-' }}
          </template>
          <template v-else-if="column.key === 'status'">
            <a-tag v-if="record.status === 'online' && record.drainStatus === 1" color="orange">排空中</a-tag>
            <a-tag v-else-if="record.status === 'online'" color="green">在线</a-tag>
            <a-tag v-else color="red">离线</a-tag>
          </template>
          <template v-else-if="column.key === 'action'">
            <a-space :size="8">
              <!-- 非冗余组节点不渲染排空按钮 -->
              <a-button
                v-if="inAnyGroup(record.nodeId) && record.drainStatus === 1"
                type="primary"
                size="small"
                @click="recoverNode(record.nodeId)"
              >恢复</a-button>
              <a-button
                v-else-if="inAnyGroup(record.nodeId) && record.status === 'online'"
                size="small"
                @click="drainNode(record.nodeId)"
              >排空</a-button>
              <a-button
                v-else-if="inAnyGroup(record.nodeId)"
                size="small"
                disabled
              >排空</a-button>
              <a-button
                v-if="inAnyGroup(record.nodeId)"
                type="primary"
                size="small"
                @click="promoteNode(record.nodeId)"
              >晋升</a-button>
            </a-space>
          </template>
        </template>
      </a-table>
    </a-card>

    <a-typography-text type="secondary" style="display: block; text-align: right; margin-top: 8px; font-size: 0.8rem;">
      数据每 2 秒自动刷新
    </a-typography-text>
  </div>
</template>