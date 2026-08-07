<script setup>
import { ref, computed, watch, onUnmounted, h } from 'vue'
import { Modal, message } from 'ant-design-vue'
import {
  apiGet,
  apiDelete,
  noRedundancy,
  storageMode,
  hostOf
} from '../../api/client'
import GroupModal from '../../components/GroupModal.vue'
import NodeChips from '../../components/NodeChips.vue'

/** HTML 实体转义：用于 Modal.confirm 拼接 innerHTML 之前先对动态值（groupId）做转义 */
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
  active: { type: Boolean, default: false },
  // 节点列表（Dashboard 注入，用于渲染 chips / 同主机判定）
  nodes: { type: Array, default: () => [] }
})
const emit = defineEmits(['update:groups'])

const groups = ref([])
const loading = ref(false)
const loadError = ref('')

const modalVisible = ref(false)
const editingGroup = ref(null)

/* ===================== 加载冗余组（轮询 5s） ===================== */
async function loadGroups() {
  loading.value = true
  loadError.value = ''
  try {
    const d = await apiGet('/api/replication/groups')
    groups.value = d.groups || []
    emit('update:groups', groups.value)
  } catch (err) {
    groups.value = []
    emit('update:groups', [])
    loadError.value = err.message
    // 单机模式 503 显示友好空态，不弹红色错误
    if (!noRedundancy) {
      message.error('加载冗余组失败: ' + err.message)
    }
  } finally {
    loading.value = false
  }
}

/* ===================== 状态计算 ===================== */
const nodeMap = computed(() => {
  const m = {}
  ;(props.nodes || []).forEach(n => {
    m[n.nodeId] = n
  })
  return m
})

function groupStatus(g) {
  const ids = g.nodeIds || []
  const aliveCount = ids.filter(id => {
    const n = nodeMap.value[id]
    return n && n.status === 'online' && n.drainStatus !== 1
  }).length
  const degraded = aliveCount < ids.length
  const hostCount = {}
  let dup = false
  ids.forEach(id => {
    const n = nodeMap.value[id]
    if (!n) return
    const h = hostOf(n.address)
    hostCount[h] = (hostCount[h] || 0) + 1
    if (hostCount[h] > 1) dup = true
  })
  return { aliveCount, total: ids.length, degraded, dup }
}

/* ===================== 操作 ===================== */
function openCreate() {
  editingGroup.value = null
  modalVisible.value = true
}

function openEdit(g) {
  editingGroup.value = g
  modalVisible.value = true
}

function onSaved() {
  loadGroups()
}

function deleteGroup(groupId) {
  Modal.confirm({
    title: '确认删除冗余组',
    content: confirmHtml(
      '删除冗余组 <b>' + escapeHtml(groupId) + '</b>？<br>' +
      '该操作不会立即迁移已存数据，仅解除节点分组关系。'
    ),
    okText: '删除',
    cancelText: '取消',
    okButtonProps: { danger: true },
    onOk: async () => {
      try {
        await apiDelete('/api/replication/groups/' + encodeURIComponent(groupId))
        message.success('冗余组已删除')
        loadGroups()
      } catch (err) {
        message.error('删除失败: ' + err.message)
      }
    }
  })
}

const columns = [
  { title: '组ID', dataIndex: 'groupId', width: '20%' },
  { title: '节点成员', dataIndex: 'nodeIds', key: 'nodeIds', width: '45%' },
  { title: '状态', dataIndex: 'status', key: 'status', width: '20%' },
  { title: '操作', dataIndex: 'action', key: 'action', width: '15%' }
]

const emptyText = computed(() => {
  if (noRedundancy && loadError.value) {
    return `单机嵌入式·无冗余（${storageMode.toUpperCase()} 模式，冗余管理不可用）`
  }
  if (loadError.value) return loadError.value
  return '暂无冗余组，点击右上角创建'
})

/* ===================== 轮询（active 门控） ===================== */
let timer = null
function startPolling() {
  loadGroups()
  timer = setInterval(loadGroups, 5000)
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

defineExpose({ loadGroups })
</script>

<template>
  <div>
    <div class="section-bar">
      <h2>冗余组管理</h2>
      <a-button
        type="primary"
        :disabled="noRedundancy"
        @click="openCreate"
      >+ 创建冗余组</a-button>
    </div>

    <a-card>
      <a-table
        :columns="columns"
        :data-source="groups"
        :loading="loading"
        :pagination="false"
        row-key="groupId"
        size="middle"
        :locale="{ emptyText }"
      >
        <template #bodyCell="{ column, record }">
          <template v-if="column.key === 'nodeIds'">
            <NodeChips :ids="record.nodeIds || []" :node-map="nodeMap" />
          </template>
          <template v-else-if="column.key === 'status'">
            <a-space :size="4" :wrap="true">
              <a-tag v-if="groupStatus(record).degraded" color="orange">
                ⚠ 冗余降级 ({{ groupStatus(record).aliveCount }}/{{ groupStatus(record).total }})
              </a-tag>
              <a-tag v-if="groupStatus(record).dup" color="orange">⚠ 同主机</a-tag>
              <a-tag v-if="!groupStatus(record).dup" color="green">正常</a-tag>
            </a-space>
          </template>
          <template v-else-if="column.key === 'action'">
            <a-space :size="8">
              <a-button type="primary" size="small" @click="openEdit(record)">编辑</a-button>
              <a-button danger size="small" @click="deleteGroup(record.groupId)">删除</a-button>
            </a-space>
          </template>
        </template>
      </a-table>
    </a-card>

    <GroupModal
      v-model:visible="modalVisible"
      :nodes="nodes"
      :groups="groups"
      :editing="editingGroup"
      @saved="onSaved"
    />
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