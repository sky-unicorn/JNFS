<script setup>
import { ref, computed, watch, onUnmounted, h } from 'vue'
import { Modal, message } from 'ant-design-vue'
import { SettingOutlined } from '@ant-design/icons-vue'
import { apiGet, apiPost, noRedundancy, storageMode } from '../../api/client'
import StatCard from '../../components/StatCard.vue'
import PolicyView from './PolicyView.vue'

/** HTML 实体转义：用于 Modal.confirm 拼接 innerHTML 之前先对动态值（taskId）做转义 */
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
  active: { type: Boolean, default: false }
})

const summary = ref({ totalPending: 0, syncedToday: 0, failed: 0, currentJobs: 0 })
const failedJobs = ref([])
const loading = ref(false)
const triggering = ref(false)
const loadError = ref('')

/* ===================== 加载同步状态（轮询 5s） ===================== */
async function loadSync() {
  loading.value = true
  loadError.value = ''
  try {
    const data = await apiGet('/api/replication/sync')
    summary.value = data.summary || {}
    failedJobs.value = data.failedJobs || []
  } catch (err) {
    summary.value = { totalPending: 0, syncedToday: 0, failed: 0, currentJobs: 0 }
    failedJobs.value = []
    loadError.value = err.message
    if (!noRedundancy) {
      message.error('加载同步状态失败: ' + err.message)
    }
  } finally {
    loading.value = false
  }
}

/* ===================== 进度计算 ===================== */
const total = computed(() => (summary.value.totalPending || 0) + (summary.value.syncedToday || 0))
const syncPercent = computed(() =>
  total.value > 0 ? Math.round(((summary.value.syncedToday || 0) * 100) / total.value) : 0
)
const progressStatus = computed(() => (syncPercent.value >= 100 ? 'success' : 'active'))

/* ===================== 操作 ===================== */
function triggerSync() {
  Modal.confirm({
    title: '手动触发全量对账',
    content: confirmHtml(
      '将立即触发一次全量对账同步？<br>系统会扫描所有待同步任务并在核心窗口内执行。'
    ),
    okText: '确定',
    cancelText: '取消',
    onOk: async () => {
      triggering.value = true
      try {
        const res = await apiPost('/api/replication/sync', {})
        message.success(res.message || '已触发全量对账')
        loadSync()
      } catch (err) {
        message.error('触发失败: ' + err.message)
      } finally {
        triggering.value = false
      }
    }
  })
}

function retryTask(taskId) {
  Modal.confirm({
    title: '重试失败任务',
    content: confirmHtml(
      '重置任务 <b>' + escapeHtml(taskId) + '</b> 的失败计数器，并将其移回等待队列？'
    ),
    okText: '确定',
    cancelText: '取消',
    onOk: async () => {
      try {
        const res = await apiPost(
          '/api/replication/sync/retry/' + encodeURIComponent(taskId),
          {}
        )
        message.success(res.message || '任务已重置')
        loadSync()
      } catch (err) {
        message.error('重试失败: ' + err.message)
      }
    }
  })
}

const columns = [
  { title: '任务ID', dataIndex: 'taskId', width: '16%' },
  { title: '文件哈希', dataIndex: 'fileHash', key: 'fileHash', width: '24%' },
  { title: '源节点', dataIndex: 'sourceNode', width: '16%' },
  { title: '目标节点', dataIndex: 'targetNode', width: '16%' },
  { title: '失败次数', dataIndex: 'retryCount', key: 'retryCount', width: '12%' },
  { title: '操作', dataIndex: 'action', key: 'action', width: '16%' }
]

const emptyText = computed(() => {
  if (noRedundancy && loadError.value) {
    return `单机嵌入式·无冗余（${storageMode.toUpperCase()} 模式，对账同步不可用）`
  }
  if (loadError.value) return loadError.value
  return '暂无失败任务'
})

/* ===================== 轮询（active 门控） ===================== */
let timer = null
function startPolling() {
  loadSync()
  timer = setInterval(loadSync, 5000)
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

defineExpose({ loadSync })

/* ===================== 同步策略子窗口 ===================== */
const policyVisible = ref(false)
function openPolicy() {
  policyVisible.value = true
}
</script>

<template>
  <div>
    <div class="section-bar">
      <h2>对账同步</h2>
      <a-space>
        <a-button
          :disabled="noRedundancy"
          @click="openPolicy"
        >
          <template #icon><SettingOutlined /></template>
          同步策略
        </a-button>
        <a-button
          type="primary"
          :loading="triggering"
          :disabled="noRedundancy"
          @click="triggerSync"
        >手动触发全量对账</a-button>
      </a-space>
    </div>

    <a-row :gutter="[16, 16]" style="margin-bottom: 16px">
      <a-col :xs="12" :lg="6">
        <StatCard title="待同步任务" :value="summary.totalPending || 0" color="#2c3e50" />
      </a-col>
      <a-col :xs="12" :lg="6">
        <StatCard title="已完成" :value="summary.syncedToday || 0" color="#2e7d32" />
      </a-col>
      <a-col :xs="12" :lg="6">
        <StatCard title="同步失败" :value="summary.failed || 0" color="#c62828" />
      </a-col>
      <a-col :xs="12" :lg="6">
        <StatCard title="当前执行中" :value="summary.currentJobs || 0" color="#0277bd" />
      </a-col>
    </a-row>

    <a-card style="margin-bottom: 16px">
      <h3 style="margin: 0 0 0.5rem 0; font-size: 1rem; color: #2c3e50">同步进度</h3>
      <a-progress
        :percent="syncPercent"
        :status="progressStatus"
        :show-info="false"
      />
      <a-typography-text type="secondary" style="font-size: 0.85rem;">
        {{ syncPercent }}% ({{ summary.syncedToday || 0 }}/{{ total }})
      </a-typography-text>
    </a-card>

    <a-card>
      <a-table
        :columns="columns"
        :data-source="failedJobs"
        :loading="loading"
        :pagination="false"
        row-key="taskId"
        size="middle"
        :locale="{ emptyText }"
      >
        <template #bodyCell="{ column, record }">
          <template v-if="column.key === 'fileHash'">
            <a-typography-text code>{{ record.fileHash }}</a-typography-text>
          </template>
          <template v-else-if="column.key === 'retryCount'">
            <a-tag color="red">{{ record.retryCount || 0 }}</a-tag>
          </template>
          <template v-else-if="column.key === 'action'">
            <a-button
              type="primary"
              size="small"
              @click="retryTask(record.taskId)"
            >重试</a-button>
          </template>
        </template>
      </a-table>
    </a-card>

    <a-typography-text type="secondary" style="display: block; text-align: right; margin-top: 8px; font-size: 0.8rem;">
      数据每 5 秒自动刷新
    </a-typography-text>

    <a-modal
      v-model:open="policyVisible"
      title="同步策略"
      width="720px"
      :footer="null"
      destroyOnClose
    >
      <PolicyView :active="policyVisible" />
    </a-modal>
  </div>
</template>

<style scoped>
.section-bar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 1rem;
}
.section-bar h2 {
  margin: 0;
  font-size: 1.1rem;
  color: #2c3e50;
}
</style>