<script setup>
import { ref, computed, watch, onUnmounted } from 'vue'
import { message } from 'ant-design-vue'
import { apiGet, hostOf, noRedundancy, storageMode } from '../../api/client'
import StatCard from '../../components/StatCard.vue'

const props = defineProps({
  active: { type: Boolean, default: false },
  nodes: { type: Array, default: () => [] },
  groups: { type: Array, default: () => [] }
})
const emit = defineEmits(['update:count'])

const activeAlerts = ref([])
const resolvedAlerts = ref([])
const loading = ref(false)
const loadError = ref('')

/* ===================== 派生告警逻辑（复刻 deriveAndRenderAlerts） ===================== */
const alertRows = computed(() => {
  const rows = []
  // 严重：连续失败（active 任务的所有失败任务）
  ;(activeAlerts.value || []).forEach(t => {
    rows.push({
      level: 'critical',
      levelText: '严重',
      content: `节点 ${t.sourceNode || '-'} 连续 ${t.retryCount || 0} 次同步失败（任务 ${t.taskId || '-'}）`,
      node: t.sourceNode || '-',
      triggered: '-',
      resolved: '-'
    })
  })

  // 警告：冗余组内同 host
  const nodeMap = {}
  ;(props.nodes || []).forEach(n => {
    nodeMap[n.nodeId] = n
  })
  ;(props.groups || []).forEach(g => {
    const ids = g.nodeIds || []
    const hosts = {}
    ids.forEach(id => {
      const n = nodeMap[id]
      if (!n) return
      const h = hostOf(n.address)
      if (!hosts[h]) hosts[h] = []
      hosts[h].push(id)
    })
    Object.keys(hosts).forEach(h => {
      if (hosts[h].length > 1) {
        rows.push({
          level: 'warning',
          levelText: '警告',
          content: `冗余组 ${g.groupId} 内节点 ${hosts[h].join(', ')} 位于同一主机 (${h})`,
          node: hosts[h].join(', '),
          triggered: '-',
          resolved: '-'
        })
      }
    })
  })
  return rows
})

const activeCount = computed(() => alertRows.value.length)
const resolvedCount = computed(() => resolvedAlerts.value.length)

// 上报红点给顶层的告警 tab
watch(
  activeCount,
  c => emit('update:count', c),
  { immediate: true }
)

/* ===================== 加载告警（轮询 5s） ===================== */
async function loadAlerts() {
  loading.value = true
  loadError.value = ''
  try {
    const data = await apiGet('/api/replication/alerts')
    activeAlerts.value = data.active || []
    resolvedAlerts.value = data.resolved || []
  } catch (err) {
    activeAlerts.value = []
    resolvedAlerts.value = []
    loadError.value = err.message
    if (!noRedundancy) {
      message.error('加载告警失败: ' + err.message)
    }
  } finally {
    loading.value = false
  }
}

const columns = [
  { title: '级别', dataIndex: 'level', key: 'level', width: '12%' },
  { title: '内容', dataIndex: 'content', key: 'content', width: '48%' },
  { title: '相关节点', dataIndex: 'node', key: 'node', width: '18%' },
  { title: '触发时间', dataIndex: 'triggered', width: '11%' },
  { title: '恢复时间', dataIndex: 'resolved', width: '11%' }
]

const emptyText = computed(() => {
  if (noRedundancy && loadError.value) {
    return `单机嵌入式·无冗余（${storageMode.toUpperCase()} 模式，告警不可用）`
  }
  if (loadError.value) return loadError.value
  return '暂无活跃告警'
})

/* ===================== 轮询（active 门控） ===================== */
let timer = null
function startPolling() {
  loadAlerts()
  timer = setInterval(loadAlerts, 5000)
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

defineExpose({ loadAlerts })
</script>

<template>
  <div>
    <a-row :gutter="[16, 16]" style="margin-bottom: 16px">
      <a-col :xs="12" :lg="12">
        <StatCard title="活跃告警" :value="activeCount" color="#c62828" />
      </a-col>
      <a-col :xs="12" :lg="12">
        <StatCard title="已恢复告警" :value="resolvedCount" color="#2e7d32" />
      </a-col>
    </a-row>

    <a-card>
      <a-table
        :columns="columns"
        :data-source="alertRows"
        :loading="loading"
        :pagination="false"
        size="middle"
        :locale="{ emptyText }"
      >
        <template #bodyCell="{ column, record }">
          <template v-if="column.key === 'level'">
            <a-tag :color="record.level === 'critical' ? 'red' : 'orange'">
              {{ record.levelText }}
            </a-tag>
          </template>
        </template>
      </a-table>
    </a-card>

    <a-typography-text type="secondary" style="display: block; text-align: right; margin-top: 8px; font-size: 0.8rem;">
      数据每 5 秒自动刷新
    </a-typography-text>
  </div>
</template>