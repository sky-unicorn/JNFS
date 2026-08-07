<script setup>
import { computed } from 'vue'
import { hostOf } from '../api/client'

const props = defineProps({
  ids: { type: Array, default: () => [] },
  nodeMap: { type: Object, default: () => ({}) }
})

// 统计每个 host 上的节点数量（用于同主机判定）
const hostCount = computed(() => {
  const hc = {}
  props.ids.forEach(id => {
    const n = props.nodeMap[id]
    if (!n) return
    const h = hostOf(n.address)
    hc[h] = (hc[h] || 0) + 1
  })
  return hc
})

function isSameHost(id) {
  const n = props.nodeMap[id]
  if (!n) return false
  return (hostCount.value[hostOf(n.address)] || 0) > 1
}
</script>

<template>
  <a-space :wrap="true" :size="[4, 4]">
    <a-tag
      v-for="id in ids"
      :key="id"
      :color="isSameHost(id) ? 'orange' : 'blue'"
    >
      {{ id }}
    </a-tag>
  </a-space>
</template>
