<script setup>
import { ref, reactive, watch } from 'vue'
import { Modal, message } from 'ant-design-vue'
import { apiGet, apiPut, noRedundancy } from '../../api/client'
import PolicyTimeSelect from '../../components/PolicyTimeSelect.vue'

const props = defineProps({
  active: { type: Boolean, default: false }
})

const saving = ref(false)
// 同步策略只加载一次（15053 协议）
let policyLoaded = false

const form = reactive({
  syncWindow: { start: '01:00', end: '03:00' },
  softDeadline: '03:00',
  rateLimitMbps: 50,
  maxConcurrency: 4
})

const rateLimitRules = [
  { required: true, message: '请填写限速' },
  {
    validator: (_, v) =>
      v === undefined || v === null || (Number.isInteger(v) && v >= 0)
        ? Promise.resolve()
        : Promise.reject(new Error('限速必须为非负整数'))
  }
]

const maxConcurrencyRules = [
  { required: true, message: '请填写最大并发数' },
  {
    validator: (_, v) =>
      Number.isInteger(v) && v >= 1 && v <= 10
        ? Promise.resolve()
        : Promise.reject(new Error('最大并发数须在 1~10 之间'))
  }
]

const formRef = ref(null)

/* ===================== 加载策略（仅一次，子 tab 激活时触发） ===================== */
async function loadPolicy() {
  if (policyLoaded) return
  try {
    const p = await apiGet('/api/replication/policy')
    if (!p) return
    if (p.syncWindow) {
      form.syncWindow.start = p.syncWindow.start || '01:00'
      form.syncWindow.end = p.syncWindow.end || '03:00'
    }
    form.softDeadline = p.softDeadline || '03:00'
    form.rateLimitMbps = p.rateLimitMbps !== undefined ? p.rateLimitMbps : 50
    form.maxConcurrency = p.maxConcurrency !== undefined ? p.maxConcurrency : 4
    policyLoaded = true
  } catch (err) {
    if (!noRedundancy) {
      message.error('加载策略失败: ' + err.message)
    }
  }
}

/* ===================== 保存策略 ===================== */
async function savePolicy() {
  try {
    await formRef.value.validate()
  } catch (e) {
    return
  }
  Modal.confirm({
    title: '保存同步策略',
    content: '确认保存当前同步策略配置？',
    okText: '确认',
    cancelText: '取消',
    onOk: async () => {
      saving.value = true
      try {
        const body = {
          syncWindow: {
            start: form.syncWindow.start,
            end: form.syncWindow.end
          },
          softDeadline: form.softDeadline,
          rateLimitMbps: Number(form.rateLimitMbps),
          maxConcurrency: Number(form.maxConcurrency)
        }
        await apiPut('/api/replication/policy', body)
        message.success('同步策略已保存')
      } catch (err) {
        message.error('保存失败: ' + err.message)
      } finally {
        saving.value = false
      }
    }
  })
}

// 子 tab 激活时加载一次（策略不轮询）
watch(
  () => props.active,
  a => {
    if (a) loadPolicy()
  },
  { immediate: true }
)

defineExpose({ loadPolicy })
</script>

<template>
  <div>
    <div class="section-bar">
      <h2>同步策略配置</h2>
      <a-button
        type="primary"
        :loading="saving"
        :disabled="noRedundancy"
        @click="savePolicy"
      >保存配置</a-button>
    </div>

    <a-card style="max-width: 640px">
      <a-form ref="formRef" :model="form" layout="vertical">
        <a-divider orientation="left" style="font-size: 0.95rem">核心同步窗口</a-divider>

        <a-form-item label="开始时间">
          <PolicyTimeSelect v-model:value="form.syncWindow.start" />
        </a-form-item>

        <a-form-item label="结束时间">
          <PolicyTimeSelect v-model:value="form.syncWindow.end" />
        </a-form-item>

        <a-form-item label="软截止时间">
          <PolicyTimeSelect v-model:value="form.softDeadline" />
          <a-typography-text type="secondary" style="margin-left: 8px; font-size: 0.8rem;">
            超出软截止仍执行，但标记告警
          </a-typography-text>
        </a-form-item>

        <a-divider orientation="left" style="font-size: 0.95rem">传输限制</a-divider>

        <a-form-item label="限速 (MB/s)" name="rateLimitMbps" :rules="rateLimitRules">
          <a-input-number
            v-model:value="form.rateLimitMbps"
            :min="0"
            :precision="0"
            style="width: 120px"
          />
          <a-typography-text type="secondary" style="margin-left: 8px; font-size: 0.8rem;">
            0 = 不限速
          </a-typography-text>
        </a-form-item>

        <a-form-item label="最大并发数" name="maxConcurrency" :rules="maxConcurrencyRules">
          <a-input-number
            v-model:value="form.maxConcurrency"
            :min="1"
            :max="10"
            :precision="0"
            style="width: 120px"
          />
          <a-typography-text type="secondary" style="margin-left: 8px; font-size: 0.8rem;">
            建议 1~10
          </a-typography-text>
        </a-form-item>
      </a-form>
    </a-card>
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