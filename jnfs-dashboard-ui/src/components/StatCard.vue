<script setup>
defineProps({
  title: { type: String, required: true },
  value: { type: [String, Number], default: '-' },
  color: { type: String, default: '#2c3e50' }
})
</script>

<template>
  <a-card style="text-align: center">
    <a-statistic
      :title="title"
      :value="value"
      :value-style="{ color, fontSize: '1.5rem', fontWeight: 'bold' }"
    />
  </a-card>
</template>
