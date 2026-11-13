<script setup>
import { onMounted, onUnmounted, ref } from 'vue'
import { getJSON, postJSON } from '../api'

const streams = ref([])
const streamId = ref(1)
const targetCursor = ref(1)
const windowEndSeq = ref(4)
const failAtSeq = ref(null)
const preview = ref(null)
const tasks = ref([])
const last = ref(null)
const err = ref('')
const busy = ref(false)
const autoRefresh = ref(false)
let timer = null

async function refreshStreams() {
  streams.value = await getJSON('/api/streams')
}
async function refreshTasks() {
  tasks.value = await getJSON('/api/replay/tasks')
}

async function doPreview() {
  err.value = ''
  preview.value = null
  try {
    preview.value = await postJSON('/api/replay/preview', {
      stream_id: Number(streamId.value),
      target_cursor: Number(targetCursor.value),
      window_end_seq: windowEndSeq.value === null || windowEndSeq.value === ''
        ? null
        : Number(windowEndSeq.value),
    })
  } catch (e) {
    err.value = String(e.message || e)
  }
}

async function runReplay() {
  // 前端再挡一层：窗口上界必填，不允许发起无上界整库重放
  if (windowEndSeq.value === null || windowEndSeq.value === '' || Number(windowEndSeq.value) <= 0) {
    err.value = 'window_end_seq（窗口上界）必填，禁止无上界整库重放'
    return
  }
  busy.value = true
  err.value = ''
  try {
    const body = {
      stream_id: Number(streamId.value),
      target_cursor: Number(targetCursor.value),
      window_end_seq: Number(windowEndSeq.value),
    }
    if (failAtSeq.value !== null && failAtSeq.value !== '') body.fail_at_seq = Number(failAtSeq.value)
    last.value = await postJSON('/api/replay/tasks', body)
    await Promise.all([refreshTasks(), refreshStreams()])
    if (last.value.status === 'failed') {
      err.value = `任务失败：${last.value.report?.failure?.error || '未知错误'}（游标停在可恢复点 ${last.value.cursor_after}）`
    }
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}

function toggleAuto() {
  autoRefresh.value = !autoRefresh.value
  if (autoRefresh.value) {
    timer = setInterval(refreshTasks, 1500)
  } else {
    clearInterval(timer)
    timer = null
  }
}

function statusTag(s) {
  if (s === 'succeeded') return 'tag ok'
  if (s === 'failed') return 'tag danger'
  return 'tag warn'
}

onMounted(async () => {
  await refreshStreams()
  if (streams.value.length) streamId.value = streams.value[0].id
  await refreshTasks()
})
onUnmounted(() => timer && clearInterval(timer))
</script>
<template>
  <div class="page">
    <h1>游标回拨重放窗</h1>
    <p class="muted">
      在有界 seq 窗口内把游标回拨并重放：仅窗内事件再次 Apply，窗外行纹丝不动；
      upsert 覆盖、delete 幂等，成功后游标复位高水位，失败停在可恢复点。
    </p>

    <div class="panel">
      <h3>发起回拨</h3>
      <div class="row-actions">
        <label>流
          <select v-model.number="streamId">
            <option v-for="s in streams" :key="s.id" :value="s.id">
              {{ s.id }} · {{ s.name }}（游标 {{ s.apply_cursor }}）
            </option>
          </select>
        </label>
        <label>回拨目标 target_cursor
          <input v-model.number="targetCursor" type="number" min="0" style="width: 7rem" />
        </label>
        <label>窗口上界 window_end_seq（必填）
          <input v-model="windowEndSeq" type="number" min="1" style="width: 7rem" />
        </label>
        <label class="muted">失败注入 fail_at_seq（可选）
          <input v-model="failAtSeq" type="number" style="width: 7rem" placeholder="无" />
        </label>
        <button class="ghost" :disabled="busy" @click="doPreview">预览窗口</button>
        <button :disabled="busy" @click="runReplay">{{ busy ? '重放中…' : '创建并执行重放任务' }}</button>
      </div>
      <p class="muted" style="margin-bottom: 0">
        窗口 = <span class="mono">(target_cursor, window_end_seq]</span>；不提供上界的整库重放会被直接拒绝。
      </p>
    </div>

    <p v-if="err" class="tag danger">{{ err }}</p>

    <div class="panel" v-if="preview">
      <h3>窗口预览（未建任务）</h3>
      <p>
        候选事件 <strong>{{ preview.candidate_count }}</strong> 个 ·
        回拨前游标 {{ preview.cursor_before }} · 目标 {{ preview.target_cursor }} → 上界 {{ preview.window_end_seq }}
        <span class="tag" :class="preview.valid && !preview.entity_boundary_violations.length ? 'ok' : 'danger'">
          {{ preview.valid && !preview.entity_boundary_violations.length ? '可重放' : '存在实体键边界冲突' }}
        </span>
      </p>
      <p v-if="preview.entity_boundary_violations.length" class="tag danger">
        这些实体键在窗外还有更高 seq：{{ preview.entity_boundary_violations.join(', ') }}
      </p>
      <table v-if="preview.candidates.length">
        <thead><tr><th>seq</th><th>op</th><th>entity_key</th><th>event_ts</th><th>payload</th></tr></thead>
        <tbody>
          <tr v-for="e in preview.candidates" :key="e.seq">
            <td>{{ e.seq }}</td><td>{{ e.op }}</td><td>{{ e.entity_key }}</td>
            <td>{{ e.event_ts }}</td><td><pre class="mono">{{ JSON.stringify(e.payload) }}</pre></td>
          </tr>
        </tbody>
      </table>
    </div>

    <div class="panel" v-if="last">
      <h3>最近任务结果 #{{ last.id }}</h3>
      <p>
        <span :class="statusTag(last.status)">{{ last.status }}</span>
        · 窗口 [{{ last.target_cursor + 1 }}..{{ last.window_end_seq }}]
        · 候选 {{ last.candidate_count }}
        · 重放 {{ last.replayed_count }} / 跳过 {{ last.skipped_count }} / 隔离 {{ last.quarantined_count }}
      </p>
      <p>
        回拨前游标 {{ last.cursor_before }} → 目标 {{ last.target_cursor }} →
        最终游标 <strong>{{ last.cursor_after }}</strong>
        <span v-if="last.last_processed_seq">· 最后处理 seq {{ last.last_processed_seq }}</span>
      </p>
      <div v-if="last.report?.failure">
        <p class="tag danger">
          失败于 seq {{ last.report.failure.seq }}（{{ last.report.failure.entity_key }}）：{{ last.report.failure.error }}
        </p>
        <p class="muted">{{ last.report.failure.recovery_hint }}</p>
      </div>
      <div v-if="last.report?.cross_checks">
        <h4>交叉核对</h4>
        <table>
          <tbody>
            <tr><td>账本计数一致</td><td>{{ last.report.cross_checks.ledger?.matches_counters }}</td></tr>
            <tr><td>applied_seqs 不增不减</td><td>{{ last.report.cross_checks.applied_seqs?.unchanged }}
              （{{ last.report.cross_checks.applied_seqs?.before_count }} → {{ last.report.cross_checks.applied_seqs?.after_count }}）</td></tr>
            <tr><td>窗外 Sink 行未改动</td><td>{{ last.report.cross_checks.sink_outside_window_untouched }}</td></tr>
            <tr><td>窗内 Sink 收敛</td><td>{{ last.report.cross_checks.sink_window_converged ?? '失败任务不适用' }}</td></tr>
            <tr><td>游标页一致</td><td>{{ last.report.cross_checks.cursor_page?.matches }}
              （期望 {{ last.report.cross_checks.cursor_page?.expected_after }}）</td></tr>
            <tr><td>隔离区计数一致</td><td>{{ last.report.cross_checks.quarantine?.matches_counter }}</td></tr>
          </tbody>
        </table>
        <p :class="last.report.cross_checks_passed ? 'tag ok' : 'tag danger'">
          总体核对：{{ last.report.cross_checks_passed ? '通过' : '未通过' }}
        </p>
        <p class="muted">策略：{{ last.report.idempotency_policy }}</p>
      </div>
    </div>

    <div class="panel">
      <div class="row-actions">
        <h3 style="margin: 0">重放任务</h3>
        <button class="ghost" @click="refreshTasks">刷新</button>
        <button class="ghost" @click="toggleAuto">{{ autoRefresh ? '停止自动刷新' : '每 1.5s 自动刷新' }}</button>
      </div>
      <table>
        <thead>
          <tr>
            <th>ID</th><th>流</th><th>状态</th><th>窗口</th><th>候选</th>
            <th>重放</th><th>跳过</th><th>隔离</th><th>游标</th><th>时间</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="t in tasks" :key="t.id">
            <td>{{ t.id }}</td>
            <td>{{ t.stream_id }}</td>
            <td><span :class="statusTag(t.status)">{{ t.status }}</span></td>
            <td>{{ t.target_cursor }}→{{ t.window_end_seq }}</td>
            <td>{{ t.candidate_count }}</td>
            <td>{{ t.replayed_count }}</td>
            <td>{{ t.skipped_count }}</td>
            <td>{{ t.quarantined_count }}</td>
            <td>{{ t.cursor_before }}→{{ t.cursor_after }}</td>
            <td>{{ t.created_at }}</td>
          </tr>
          <tr v-if="!tasks.length"><td colspan="10" class="muted">暂无重放任务</td></tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
