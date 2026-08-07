<script setup>
import { computed } from 'vue'

const props = defineProps({
  value: { type: String, default: '00:00' }
})
const emit = defineEmits(['update:value'])

// 时下拉选项：00 ~ 23
const hourOptions = computed(() => {
  const arr = []
  for (let h = 0; h < 24; h++) {
    const v = (h < 10 ? '0' : '') + h
    arr.push({ label: v, value: v })
  }
  return arr
})

// 分下拉选项：00 / 15 / 30 / 45
const minuteOptions = computed(() =>
  ['00', '15', '30', '45'].map(m => ({ label: m, value: m }))
)

// 兼容 "01:00" / "01:00:00" / 缺省
function splitTime(t) {
  if (!t) return ['00', '00']
  const parts = String(t).split(':')
  return [parts[0] || '00', parts[1] || '00']
}

const hour = computed({
  get: () => splitTime(props.value)[0],
  set: h => emit('update:value', h + ':' + splitTime(props.value)[1])
})
const minute = computed({
  get: () => splitTime(props.value)[1],
  set: m => emit('update:value', splitTime(props.value)[0] + ':' + m)
})
</script>

<template>
  <a-space align="center" :size="4">
    <a-select
      v-model:value="hour"
      :options="hourOptions"
      style="width: 78px"
    />
    <span>:</span>
    <a-select
      v-model:value="minute"
      :options="minuteOptions"
      style="width: 78px"
    />
  </a-space>
</template>
