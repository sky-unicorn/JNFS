// 读取 Java 端在 index.html 占位行注入的配置；缺失时回落到旧默认值（单机无鉴权）
const cfg = window.__JNFS_CONFIG__ || {
  storageMode: 'file',
  noRedundancy: true,
  authEnabled: false
}

export const config = cfg
export const storageMode = cfg.storageMode
export const noRedundancy = cfg.noRedundancy
export const authEnabled = cfg.authEnabled

/**
 * 统一 fetch 封装：
 *   - credentials: 'same-origin' 携带鉴权 cookie
 *   - 响应被后端 302 重定向（res.redirected===true）→ 整页跳到目标 URL（一般即 /login）
 *   - Content-Type 非 JSON → 抛错
 *   - !res.ok 或 data.success===false → 取 errors.join('; ') / data.error / HTTP 状态 抛错
 */
export async function apiFetch(url, options = {}) {
  const opts = { credentials: 'same-origin', ...options }
  const res = await fetch(url, opts)
  if (res.redirected) {
    window.location.href = res.url
    throw new Error('redirected')
  }
  const ct = res.headers.get('Content-Type') || ''
  if (!ct.includes('application/json')) {
    throw new Error('非 JSON 响应（HTTP ' + res.status + '）')
  }
  const data = await res.json()
  if (!res.ok || data.success === false) {
    const errs =
      data.errors && data.errors.length
        ? data.errors.join('; ')
        : data.error || 'HTTP ' + res.status
    throw new Error(errs)
  }
  return data
}

export function apiGet(u) {
  return apiFetch(u)
}

export function apiPost(u, body) {
  return apiFetch(u, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body || {})
  })
}

export function apiPut(u, body) {
  return apiFetch(u, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body || {})
  })
}

export function apiDelete(u) {
  return apiFetch(u, { method: 'DELETE' })
}

/** 字节数转可读字符串（与原 JS formatBytes 行为一致） */
export function formatBytes(bytes, decimals = 2) {
  if (bytes === 0) return '0 Bytes'
  const k = 1024
  const dm = decimals < 0 ? 0 : decimals
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i]
}

/** 提取地址中的 host 部分 */
export function hostOf(address) {
  return (address || '').split(':')[0]
}

/**
 * 节点是否归属任意冗余组：保留为纯函数入口供非响应式场景使用；
 * Vue 模板请改用基于响应式 props.groups 派生的判断（见 NodesView）。
 */
export function nodeInAnyGroup(groups, nodeId) {
  return (groups || []).some(g => (g.nodeIds || []).includes(nodeId))
}
