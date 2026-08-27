<script setup>
import { ref, computed, watch } from 'vue'
import { apiGet, formatBytes, storageMode } from '../api/client'

const props = defineProps({
  // 是否激活（顶层 tab 为 files 时才加载）
  active: { type: Boolean, default: false }
})

/* ===================== 状态 ===================== */
const loading = ref(false)
const loadError = ref('')
const rows = ref([])
const total = ref(0)
const page = ref(1)
const pageSize = ref(20)

// 筛选输入态 / 已生效态（点「查询」后对齐生效）
const filters = ref({ storageId: '', nodeId: null, fileType: null })
const applied = ref({ storageId: '', nodeId: null, fileType: null })

// 下拉数据源：存储节点（/api/nodes）、文件类型（/api/files/types）
const nodes = ref([])
const types = ref([])

const nodeOptions = computed(() =>
  nodes.value.map(n => ({
    value: n.nodeId,
    label: n.address || n.nodeId
  }))
)
const typeOptions = computed(() => {
  // 「未知」置顶：JNFS 中 file_type='unknown' 存值与 NULL 行展示均为未知，
  // 详见 FileMetadataDao#buildWhere 中 'unknown' 匹配存值 ∪ NULL 的语义。
  const list = types.value.map(t => ({ value: t, label: t === 'unknown' ? '未知' : t }))
  return [...list.filter(o => o.value === 'unknown'), ...list.filter(o => o.value !== 'unknown')]
})

/* ===================== 加载 ===================== */
async function loadFiles() {
  loading.value = true
  loadError.value = ''
  try {
    const params = new URLSearchParams()
    params.set('page', page.value)
    params.set('pageSize', pageSize.value)
    if (applied.value.nodeId) params.set('nodeId', applied.value.nodeId)
    if (applied.value.fileType) params.set('fileType', applied.value.fileType)
    if (applied.value.storageId) params.set('storageId', applied.value.storageId)
    const d = await apiGet('/api/files?' + params.toString())
    rows.value = d.files || []
    total.value = d.total || 0
  } catch (err) {
    rows.value = []
    total.value = 0
    loadError.value = err.message
  } finally {
    loading.value = false
  }
}

async function loadNodes() {
  try {
    const d = await apiGet('/api/nodes')
    nodes.value = d || []
  } catch (e) {
    nodes.value = []
  }
}

async function loadTypes() {
  try {
    const d = await apiGet('/api/files/types')
    types.value = d.types || []
  } catch (e) {
    types.value = []
  }
}

function loadAll() {
  loadNodes()
  loadTypes()
  loadFiles()
}

/* ===================== 筛选与分页 ===================== */
function applyFilters() {
  applied.value = { ...filters.value }
  page.value = 1
  loadFiles()
}

function resetFilters() {
  filters.value = { storageId: '', nodeId: null, fileType: null }
  applied.value = { storageId: '', nodeId: null, fileType: null }
  page.value = 1
  loadFiles()
}

function onTableChange(pager) {
  page.value = pager.current
  pageSize.value = pager.pageSize
  loadFiles()
}

/* ===================== 展示辅助 ===================== */
const TYPE_COLORS = {
  doc: 'blue', docx: 'blue', xls: 'green', xlsx: 'green', ppt: 'orange', pptx: 'orange',
  pdf: 'red', rtf: 'blue', odt: 'blue', ods: 'green', odp: 'orange',
  txt: 'default', md: 'default', csv: 'default', json: 'default', xml: 'default',
  html: 'default', css: 'default', yaml: 'default',
  js: 'geekblue', ts: 'geekblue', java: 'geekblue', py: 'geekblue', go: 'geekblue',
  c: 'geekblue', cpp: 'geekblue', sql: 'geekblue', sh: 'cyan', bat: 'cyan',
  zip: 'orange', rar: 'orange', '7z': 'orange', tar: 'orange', gz: 'orange', jar: 'orange',
  jpg: 'purple', jpeg: 'purple', png: 'purple', gif: 'purple', bmp: 'purple',
  svg: 'purple', webp: 'purple', ico: 'purple', tiff: 'purple',
  mp3: 'magenta', wav: 'magenta', flac: 'magenta', aac: 'magenta', m4a: 'magenta',
  mp4: 'magenta', avi: 'magenta', mkv: 'magenta', mov: 'magenta', webm: 'magenta',
  exe: 'volcano', dll: 'volcano', apk: 'volcano', bin: 'volcano',
  unknown: 'default'
}
function typeColor(t) {
  return TYPE_COLORS[t] || 'default'
}
function typeLabel(t) {
  return t === 'unknown' ? '未知' : t
}
function activeCount(record) {
  return (record.nodes || []).filter(n => n.status === 1).length
}
function nodeTagColor(n) {
  if (n.status !== 1) return 'red'
  return n.role === 0 ? 'blue' : 'green'
}
function nodeTagLabel(n) {
  const role = n.role === 0 ? '主' : '备'
  return (n.status === 1 ? role : '损坏') + ' ' + n.nodeId
}

/* ===================== 表格列 ===================== */
const columns = [
  { title: '存储ID', dataIndex: 'storageId', key: 'storageId', width: '22%' },
  { title: '类型', dataIndex: 'fileType', key: 'fileType', width: '8%' },
  { title: '大小', dataIndex: 'fileSize', key: 'fileSize', width: '10%' },
  { title: '存储节点', dataIndex: 'nodes', key: 'nodes' },
  { title: '副本数', dataIndex: 'replicaCount', key: 'replicaCount', width: '8%' },
  { title: '创建时间', dataIndex: 'createTime', key: 'createTime', width: '13%' },
  { title: '哈希', dataIndex: 'fileHash', key: 'fileHash', width: '18%' }
]

/* ===================== 激活门控 ===================== */
watch(
  () => props.active,
  a => {
    if (a) loadAll()
  },
  { immediate: true }
)
</script>

<template>
  <div>
    <a-alert
      v-if="storageMode === 'file'"
      type="warning"
      show-icon
      style="margin-bottom: 12px"
      message="单机 file 模式·元数据库未配置：文件管理不可用（文件 API 返回 503）。"
    />

    <a-card :bordered="false" style="margin-bottom: 16px">
      <a-form layout="inline">
        <a-form-item label="存储编号">
          <a-input
            v-model:value="filters.storageId"
            placeholder="storage_id 包含匹配"
            allow-clear
            style="width: 180px"
            @pressEnter="applyFilters"
          />
        </a-form-item>
        <a-form-item label="存储节点">
          <a-select
            v-model:value="filters.nodeId"
            :options="nodeOptions"
            placeholder="全部节点"
            allow-clear
            show-search
            option-filter-prop="label"
            style="width: 280px"
          />
        </a-form-item>
        <a-form-item label="文件类型">
          <a-select
            v-model:value="filters.fileType"
            :options="typeOptions"
            placeholder="全部类型"
            allow-clear
            show-search
            option-filter-prop="label"
            style="width: 140px"
          />
        </a-form-item>
        <a-form-item>
          <a-space>
            <a-button type="primary" @click="applyFilters">查询</a-button>
            <a-button @click="resetFilters">重置</a-button>
            <a-button :loading="loading" @click="loadFiles">刷新</a-button>
          </a-space>
        </a-form-item>
      </a-form>
    </a-card>

    <a-card :bordered="false">
      <a-table
        :columns="columns"
        :data-source="rows"
        :loading="loading"
        row-key="storageId"
        size="middle"
        :pagination="{
          current: page,
          pageSize: pageSize,
          total: total,
          showSizeChanger: true,
          pageSizeOptions: ['10', '20', '50', '100'],
          showTotal: t => '共 ' + t + ' 条'
        }"
        :locale="{ emptyText: loadError ? '加载失败: ' + loadError : '暂无已上传文件' }"
        @change="onTableChange"
      >
        <template #bodyCell="{ column, record }">
          <template v-if="column.key === 'storageId'">
            <a-typography-text :title="record.storageId" copyable style="font-size: 12px">
              {{ record.storageId }}
            </a-typography-text>
          </template>
          <template v-else-if="column.key === 'fileType'">
            <a-tag v-if="record.fileType" :color="typeColor(record.fileType)">
              {{ typeLabel(record.fileType) }}
            </a-tag>
            <a-typography-text v-else type="secondary">未知</a-typography-text>
          </template>
          <template v-else-if="column.key === 'fileSize'">
            <a-typography-text v-if="record.fileSize === null" type="secondary">未知</a-typography-text>
            <span v-else>{{ formatBytes(record.fileSize) }}</span>
          </template>
          <template v-else-if="column.key === 'nodes'">
            <a-space :wrap="true" :size="[4, 4]">
              <a-tooltip
                v-for="(n, i) in record.nodes"
                :key="i"
                :title="n.addr || n.nodeId"
              >
                <a-tag :color="nodeTagColor(n)">{{ nodeTagLabel(n) }}</a-tag>
              </a-tooltip>
            </a-space>
            <a-typography-text v-if="!record.nodes || record.nodes.length === 0" type="secondary">
              无副本
            </a-typography-text>
          </template>
          <template v-else-if="column.key === 'replicaCount'">
            <a-typography-text v-if="(record.nodes || []).some(n => n.status !== 1)" type="warning">
              {{ activeCount(record) }}/{{ (record.nodes || []).length }}
            </a-typography-text>
            <span v-else>{{ (record.nodes || []).length }} / {{ record.replicationFactor }}</span>
          </template>
          <template v-else-if="column.key === 'createTime'">
            {{ record.createTime ? new Date(record.createTime).toLocaleString() : '-' }}
          </template>
          <template v-else-if="column.key === 'fileHash'">
            <a-typography-text :title="record.fileHash" copyable ellipsis style="font-size: 12px; color: #999">
              {{ record.fileHash }}
            </a-typography-text>
          </template>
        </template>
      </a-table>
    </a-card>
  </div>
</template>
