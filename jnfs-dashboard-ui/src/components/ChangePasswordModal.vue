<script setup>
import { ref, reactive } from 'vue'
import { message } from 'ant-design-vue'

const props = defineProps({
  visible: { type: Boolean, default: false }
})
const emit = defineEmits(['update:visible'])

const submitting = ref(false)
const formRef = ref(null)

const form = reactive({
  oldPassword: '',
  newPassword: ''
})

const rules = {
  oldPassword: [{ required: true, message: '请填写旧密码' }],
  newPassword: [
    { required: true, message: '请填写新密码' },
    { min: 4, message: '新密码至少 4 位' }
  ]
}

function close() {
  emit('update:visible', false)
  form.oldPassword = ''
  form.newPassword = ''
  formRef.value && formRef.value.clearValidate && formRef.value.clearValidate()
}

async function submitChangePassword() {
  try {
    await formRef.value.validate()
  } catch (e) {
    return
  }
  submitting.value = true
  try {
    // 改密走 application/x-www-form-urlencoded（与原 POST /api/change-password 一致）
    const res = await fetch('/api/change-password', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      credentials: 'same-origin',
      body:
        'oldPassword=' +
        encodeURIComponent(form.oldPassword) +
        '&newPassword=' +
        encodeURIComponent(form.newPassword)
    })
    if (res.redirected) {
      window.location.href = res.url
      return
    }
    let data
    try {
      data = await res.json()
    } catch (e) {
      throw new Error('非 JSON 响应（HTTP ' + res.status + '）')
    }
    if (data.success) {
      message.success(data.message || '修改成功')
      close()
      setTimeout(() => {
        window.location.href = '/login'
      }, 800)
    } else {
      message.error('修改失败: ' + (data.error || '未知错误'))
    }
  } catch (err) {
    message.error('请求失败: ' + err.message)
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <a-modal
    :open="visible"
    title="修改密码"
    :confirm-loading="submitting"
    ok-text="确认修改"
    cancel-text="取消"
    @ok="submitChangePassword"
    @cancel="close"
  >
    <a-form ref="formRef" :model="form" :rules="rules" layout="vertical">
      <a-form-item label="旧密码" name="oldPassword">
        <a-input-password
          v-model:value="form.oldPassword"
          placeholder="旧密码"
          autocomplete="current-password"
        />
      </a-form-item>
      <a-form-item label="新密码（至少 4 位）" name="newPassword">
        <a-input-password
          v-model:value="form.newPassword"
          placeholder="新密码（至少 4 位）"
          autocomplete="new-password"
        />
      </a-form-item>
    </a-form>
  </a-modal>
</template>
