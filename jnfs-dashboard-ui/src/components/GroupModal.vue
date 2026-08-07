<script setup>
import { ref, reactive, computed, watch } from 'vue'
import { message } from 'ant-design-vue'
import { apiPost, apiPut, hostOf } from '../api/client'

const props = defineProps({
  visible: { type: Boolean, default: false },
  // 当前所有节点（用于渲染勾选 + 离线 disabled）
  nodes: { type: Array, default: () => [] },
  // 当前所有冗余组（用于重叠校验）
  groups: { type: Array, default: () => [] },
  // 编辑态：传入要编辑的组对象（{groupId,nodeIds}）；新建传 null
  editing: { type: Object, default: null }
})
const emit = defineEmits(['update:visible', 'saved'])

const submitting = ref(false)

const form = reactive({
  groupId: '',
  nodeIds: []
})

const editingGroupId = computed(() => (props.editing ? props.editing.groupId : null))
const isEdit = computed(() => !!props.editing)

// 弹窗打开/编辑态变化时同步表单初始值
watch(
  () => [props.visible, props.editing],
  ([vis]) => {
    if (vis) {
      if (props.editing) {
        form.groupId = props.editing.groupId
        form.nodeIds = [...(props.editing.nodeIds || [])]
      } else {
        form.groupId = ''
        form.nodeIds = []
      }
    }
  },
  { immediate: true }
)

/**
 * 校验函数（复刻原 validateGroupNodeSelection）
 * @returns {{errors:string[], warnings:string[], valid:boolean}}
 */
function validate(selectedNodeIds, allNodesList, existingGroups, editingId) {
  const errors = []
  const warnings = []
  if (selectedNodeIds.length < 2) errors.push('至少选择 2 个节点')
  if (selectedNodeIds.length > 3) errors.push('最多选择 3 个节点')

  const nodeMap = {}
  allNodesList.forEach(n => {
    nodeMap[n.nodeId] = n
  })

  // 重叠检查（与除自身外的其他组）
  selectedNodeIds.forEach(nodeId => {
    existingGroups.forEach(group => {
      if (group.groupId === editingId) return
      const ids = group.nodeIds || []
      if (ids.includes(nodeId)) {
        errors.push('节点 ' + nodeId + ' 已属于冗余组 ' + group.groupId + '，不可重复分配')
      }
    })
  })

  // 同 host 检查（仅警告）
  const hosts = {}
  selectedNodeIds.forEach(id => {
    const n = nodeMap[id]
    if (!n) return
    const h = hostOf(n.address)
    if (!hosts[h]) hosts[h] = []
    hosts[h].push(id)
  })
  Object.keys(hosts).forEach(host => {
    if (hosts[host].length > 1) {
      warnings.push('节点 ' + hosts[host].join(', ') + ' 位于同一主机 (' + host + ')')
    }
  })

  return { errors, warnings, valid: errors.length === 0 }
}

const validation = computed(() =>
  validate(form.nodeIds, props.nodes, props.groups, editingGroupId.value)
)

const okDisabled = computed(
  () => !validation.value.valid || form.nodeIds.length === 0
)

function close() {
  emit('update:visible', false)
}

async function submitGroup() {
  if (!validation.value.valid) {
    message.error('校验未通过')
    return
  }
  submitting.value = true
  try {
    if (isEdit.value) {
      await apiPut('/api/replication/groups/' + encodeURIComponent(editingGroupId.value), {
        nodeIds: form.nodeIds
      })
      message.success('冗余组已更新')
    } else {
      const newId = form.groupId.trim()
      if (!newId) {
        message.error('请填写组ID')
        submitting.value = false
        return
      }
      await apiPost('/api/replication/groups', {
        groupId: newId,
        nodeIds: form.nodeIds
      })
      message.success('冗余组已创建')
    }
    close()
    emit('saved')
  } catch (err) {
    message.error((isEdit.value ? '更新失败: ' : '创建失败: ') + err.message)
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <a-modal
    :open="visible"
    :title="isEdit ? '编辑冗余组' : '创建冗余组'"
    :confirm-loading="submitting"
    :ok-button-props="{ disabled: okDisabled }"
    ok-text="确认"
    cancel-text="取消"
    @ok="submitGroup"
    @cancel="close"
  >
    <a-form layout="vertical">
      <a-form-item v-if="!isEdit" label="组ID">
        <a-input
          v-model:value="form.groupId"
          placeholder="组ID（编辑时不可改）"
        />
      </a-form-item>
      <a-form-item v-else label="组ID">
        <a-input :value="form.groupId" disabled />
      </a-form-item>

      <a-form-item label="勾选节点（2~3 个，离线节点不可选）">
        <div v-if="nodes.length === 0" style="color: #95a5a6; padding: 0.5rem;">
          暂无可用节点
        </div>
        <a-checkbox-group v-else v-model:value="form.nodeIds">
          <a-row>
            <a-col
              v-for="n in nodes"
              :key="n.nodeId"
              :span="24"
              style="padding: 4px 0"
            >
              <a-checkbox
                :value="n.nodeId"
                :disabled="n.status !== 'online'"
              >
                <span>{{ n.nodeId }}</span>
                <a-typography-text type="secondary" style="margin-left: 6px; font-size: 0.8rem;">
                  ({{ n.address }}{{ n.status !== 'online' ? ' · 离线' : '' }})
                </a-typography-text>
              </a-checkbox>
            </a-col>
          </a-row>
        </a-checkbox-group>
      </a-form-item>

      <div v-if="validation.errors.length || validation.warnings.length">
        <a-alert
          v-for="(e, i) in validation.errors"
          :key="'e' + i"
          type="error"
          :message="'✗ ' + e"
          show-icon
          banner
          style="margin-bottom: 6px"
        />
        <a-alert
          v-for="(w, i) in validation.warnings"
          :key="'w' + i"
          type="warning"
          :message="'⚠ ' + w"
          show-icon
          banner
          style="margin-bottom: 6px"
        />
      </div>
    </a-form>
  </a-modal>
</template>
