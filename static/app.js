"use strict";

const $ = (id) => document.getElementById(id);
const state = {
  config: null,
  snapshot: null,
  previewTask: null,
  page: null,
  fullTask: null,
  pollTimer: null,
  busy: false,
};

const METHOD_TEXT = {
  stratified: "按分区分层：每个日期/区域分区按人口比例分配，保底 1 条，避免小分区消失。",
  stratified_rare: "分层 + 稀有补充：代表性通道负责无偏估计，第二阶段专门补稀有类型和新增类型，两通道权重不可相加。",
  kind: "按类型分层：每类至少 2 条、按 sqrt(N) 分配，单通道概率样本；索引对高频类型只保留稀疏帧。",
  uniform: "全局简单随机抽样，按文件人口严格比例分配；小分区可能一条都抽不到。",
  head: "文件前缀，非概率样本，没有设计权重，不能外推到全量。",
};

function fmt(n) {
  if (n === null || n === undefined) return "–";
  if (typeof n === "number") return Number.isInteger(n) ? n.toLocaleString() : n.toFixed(3);
  return String(n);
}
function pct(x) {
  if (x === null || x === undefined) return "–";
  return (x * 100).toFixed(2) + "%";
}
function el(tag, attrs = {}, children = []) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === "class") node.className = v;
    else if (k === "text") node.textContent = v;
    else node.setAttribute(k, v);
  }
  for (const child of children) node.append(child);
  return node;
}
function table(headers, rows, rowClass) {
  const t = el("table");
  const thead = el("thead");
  const tr = el("tr");
  headers.forEach((h) => tr.append(el("th", { text: h })));
  thead.append(tr);
  t.append(thead);
  const tbody = el("tbody");
  if (!rows.length) {
    const r = el("tr");
    r.append(el("td", { colspan: String(headers.length), class: "mono", text: "（无数据）" }));
    tbody.append(r);
  }
  rows.forEach((cells) => {
    const r = el("tr");
    if (rowClass) rowClass(r, cells);
    cells.forEach((cell) => {
      const td = el("td");
      if (cell && typeof cell === "object" && cell.node) {
        td.append(cell.node);
        if (cell.cls) td.className = cell.cls;
      } else {
        td.textContent = cell === null || cell === undefined ? "–" : String(cell);
        if (cell && typeof cell === "object" && cell.cls) td.className = cell.cls;
      }
      r.append(td);
    });
    tbody.append(r);
  });
  t.append(tbody);
  return t;
}
function replaceTable(id, headers, rows) {
  const host = $(id);
  host.replaceWith(Object.assign(table(headers, rows), { id }));
}
function pill(text, cls) {
  return el("span", { class: "pill " + (cls || ""), text });
}

async function api(path, options = {}) {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    cache: "no-store",
    ...options,
  });
  let body = null;
  const text = await res.text();
  try { body = text ? JSON.parse(text) : null; } catch { body = { error: text }; }
  if (!res.ok) {
    const err = new Error((body && body.error) || res.statusText);
    err.status = res.status;
    err.body = body;
    throw err;
  }
  return body;
}

function notice(message, level = "warn") {
  const host = $("alerts");
  const node = el("div", { class: "alert " + level, text: message });
  host.append(node);
  while (host.children.length > 6) host.firstChild.remove();
}
function clearNotices() { $("alerts").replaceChildren(); }

// ---------------------------------------------------------------- bootstrap
async function loadConfig() {
  state.config = await api("/api/config");
  const select = $("method");
  select.replaceChildren();
  state.config.methods.forEach((m) => select.append(el("option", { value: m, text: m })));
  select.value = state.config.default_method;
  $("budget").value = state.config.default_budget;
  $("page-size").value = state.config.default_page_size;
  $("method-hint").textContent = METHOD_TEXT[select.value] || "";
  select.addEventListener("change", () => {
    $("method-hint").textContent = METHOD_TEXT[select.value] || "";
  });
}

async function loadSnapshot() {
  const data = await api("/api/snapshot");
  state.snapshot = data.snapshot;
  $("snapshot-line").textContent =
    `快照 ${data.snapshot.snapshot_id.slice(0, 10)} · ${fmt(data.snapshot.file_count)} 文件 · ` +
    `${fmt(data.snapshot.record_count)} 条 · ${data.snapshot.partition_count} 分区 · 于 ${data.snapshot.created_at}`;
  const rows = (data.snapshots || []).slice().reverse().map((s) => [
    s.snapshot_id.slice(0, 10),
    s.note,
    fmt(s.file_count),
    fmt(s.record_count),
    s.author,
    s.created_at,
    s.snapshot_id === data.snapshot.snapshot_id ? "当前" : "",
  ]);
  replaceTable("snapshots", ["快照", "说明", "文件", "记录", "来源", "创建时间", "状态"], rows);
  $("merge-dates").value = state.snapshot.partitions.slice(-3).map((p) => p.date)
    .filter((d, i, arr) => arr.indexOf(d) === i).join(",");
}

// ------------------------------------------------------------------ preview
function setBusy(flag) {
  state.busy = flag;
  ["run", "full-query", "resume", "merge", "add-partition", "refresh"].forEach((id) => {
    const node = $(id);
    if (node) node.disabled = flag;
  });
  if (flag) {
    $("next").disabled = true;
    $("prev").disabled = true;
  } else if (state.page) {
    $("next").disabled = !state.page.has_next;
    $("prev").disabled = !state.page.has_prev;
  }
}

async function createPreview(event) {
  event.preventDefault();
  clearNotices();
  if (state.pollTimer) { clearInterval(state.pollTimer); state.pollTimer = null; }
  const payload = {
    method: $("method").value,
    budget: Number($("budget").value),
    page_size: Number($("page-size").value),
    seed: $("seed").value.trim() || "preview-v1",
  };
  if ($("use-snapshot").checked && state.snapshot) payload.snapshot_id = state.snapshot.snapshot_id;
  setBusy(true);
  try {
    const data = await api("/api/previews", { method: "POST", body: JSON.stringify(payload) });
    state.previewTask = data.task;
    state.page = data.page;
    state.fullTask = null;
    renderPage(data.page);
    $("compare-panel").hidden = true;
    pollPreview(data.task.id);
  } catch (err) {
    notice(err.message, "bad");
    setBusy(false);
  }
}

function pollPreview(taskId) {
  if (state.pollTimer) clearInterval(state.pollTimer);
  state.pollTimer = setInterval(async () => {
    try {
      const task = await api(`/api/tasks/${taskId}`);
      renderTaskStatus(task);
      if (["complete", "failed", "interrupted"].includes(task.status)) {
        clearInterval(state.pollTimer);
        state.pollTimer = null;
        setBusy(false);
        state.previewTask = task;
        if (task.status === "complete") {
          const page = await api(`/api/tasks/${taskId}/rows?offset=${state.page ? state.page.offset : 0}`);
          state.page = page;
          renderPage(page);
          await loadStats(taskId);
        } else {
          notice(`任务 ${task.status}：${task.error || "未知错误"}`, "bad");
        }
        await loadTasks();
      }
    } catch (err) {
      clearInterval(state.pollTimer);
      state.pollTimer = null;
      setBusy(false);
      notice("轮询任务失败：" + err.message, "bad");
    }
  }, 350);
}

function renderTaskStatus(task) {
  $("status-panel").hidden = false;
  const status = $("task-status");
  status.replaceChildren(
    el("span", { class: "status-" + task.status, text: task.status.toUpperCase() }),
    document.createTextNode(`  ${task.method} · seed=${task.seed} · budget=${fmt(task.budget)} · 尝试 ${task.attempts} · 快照 ${String(task.snapshot_id).slice(0, 10)}`)
  );
  const actions = $("task-actions");
  actions.replaceChildren();
  if (["failed", "interrupted"].includes(task.status)) {
    const retry = el("button", { type: "button", text: "从断点重试" });
    retry.addEventListener("click", () => retryTask(task.id));
    actions.append(retry);
  }
  if (task.stats && task.stats.read) {
    const r = task.stats.read;
    const wrap = $("read-budget");
    wrap.replaceChildren();
    const cards = [
      ["实际样本", fmt(task.stats.actual_rows), `计划 ${fmt(task.stats.planned_records)} · 偏差 ${fmt(task.stats.planned_vs_actual)}`],
      ["预算占用", pct(r.budget_used_ratio), `预算 ${fmt(r.budget_records)} 条`],
      ["物理读取行数", fmt(r.lines_read), `${fmt(r.seeks)} 次 seek · ${fmt(r.bytes_read)} 字节`],
      ["每样本读取行", fmt(r.physical_lines_per_sampled_row), "索引把随机读限制在 ~stride 行/样本"],
    ];
    cards.forEach(([k, v, s]) => {
      wrap.append(el("div", { class: "meter" }, [
        el("div", { class: "k", text: k }),
        el("div", { class: "v", text: v }),
        el("div", { class: "s", text: s }),
      ]));
    });
  }
}

async function retryTask(taskId) {
  setBusy(true);
  try {
    const task = await api(`/api/tasks/${taskId}/retry`, { method: "POST", body: "{}" });
    notice("已排队重试，已完成的分片会被复用。", "info");
    pollPreview(task.id);
  } catch (err) {
    notice(err.message, "bad");
    setBusy(false);
  }
}

function renderPage(page) {
  $("sample-panel").hidden = false;
  state.page = page;
  $("page-label").textContent = `${fmt(page.offset)}–${fmt(page.offset + page.rows.length)} / ${fmt(page.total_rows)}`;
  $("prev").disabled = !page.has_prev || state.busy;
  $("next").disabled = !page.has_next || state.busy;
  $("page-meta").replaceChildren(
    el("span", { text: `通道 ${Object.entries(page.rows.reduce((acc, r) => { acc[r.channel] = (acc[r.channel] || 0) + 1; return acc; }, {})).map(([k, v]) => `${k}=${v}`).join(" · ") || "–"}` }),
    el("span", { text: `样本摘要 ${String(page.sample_digest || "").slice(0, 12)}` }),
    el("span", { text: `令牌 ${page.page_token ? page.page_token.slice(0, 18) + "…" : "–"}` })
  );
  const rows = page.rows.map((row) => {
    const rec = row.record || {};
    const kindPill = pill(String(rec.kind), row.channel === "rare-supplement" ? "rare" : "");
    return [
      { node: kindPill },
      row.channel,
      row.partition,
      rec.event_id,
      fmt(row.weight),
      fmt(row.source_line),
      row.kind_population ? fmt(row.kind_population) : fmt(row.population),
      JSON.stringify(rec.payload || {}).slice(0, 110),
    ];
  });
  replaceTable("rows", ["类型", "通道", "分区", "event_id", "权重", "源行", "层人口", "payload"], rows);
  $("token-note").textContent = page.paged_by_token
    ? "本页由版本化令牌定位：令牌绑定任务/快照/种子/预算/页大小，篡改会被拒绝。"
    : "本页按 offset 直接读取；令牌续页是默认路径。";
}

async function nextPage(token) {
  if (!token || state.busy) return;
  setBusy(true);
  try {
    const page = await api(`/api/tasks/${state.previewTask.id}/rows?token=${encodeURIComponent(token)}`);
    renderPage(page);
  } catch (err) {
    notice(err.message + "（请重新发起预览，避免混合样本）", "bad");
  } finally {
    setBusy(false);
  }
}

// -------------------------------------------------------------------- stats
async function loadStats(taskId) {
  const stats = await api(`/api/tasks/${taskId}/stats`);
  $("stats-panel").hidden = false;
  $("stats-disclaimer").textContent =
    stats.not_full_result + " " + (stats.estimates && stats.estimates.method_note ? stats.estimates.method_note : "");
  const est = stats.estimates || {};
  let rows = [];
  if (est.estimable) {
    rows = est.kinds.map((r) => [
      r.kind,
      fmt(r.observed),
      fmt(r.estimated_count),
      `${fmt(r.count_ci95[0])} – ${fmt(r.count_ci95[1])}`,
      pct(r.proportion),
      fmt(r.stderr_proportion),
      fmt(r.strata_observed),
    ]);
  } else {
    rows = [[est.reason || "不可估计", "", "", "", "", "", ""]];
  }
  replaceTable("kind-table", ["类型", "样本观测", "估计总量", "95% CI", "占比", "占比标准误", "覆盖层"], rows);

  $("coverage-panel").hidden = false;
  const cov = stats.coverage || {};
  $("coverage-summary").replaceChildren(
    el("span", { text: `分区缺口 ${fmt((cov.uncovered_partitions || []).length)} 个 · 未覆盖记录 ${fmt(cov.uncovered_population)}（${pct(cov.uncovered_share)}）` }),
    el("span", { text: `类型总数 ${fmt(cov.kind_count)} · 稀有类型 ${fmt(cov.rare_kind_count)}` }),
    el("span", { text: `稀有通道命中 ${fmt(stats.rare?.rare_kinds_covered)}/${fmt(stats.rare?.rare_kinds_total)}` })
  );
  const partRows = (cov.partitions || []).map((p) => {
    const c = [];
    c.push(p.partition);
    c.push(fmt(p.population));
    c.push(fmt(p.planned));
    c.push(pct(p.population_share));
    c.push(pct(p.planned_share));
    c.push({ node: p.covered ? pill("covered") : pill("gap", "gap") });
    return c;
  });
  replaceTable("partition-table", ["分区", "人口", "计划样本", "人口占比", "样本占比", "状态"], partRows);
  const rareRows = ((stats.rare && stats.rare.rare_kinds) || []).map((r) => [
    r.kind,
    fmt(r.population),
    fmt(r.observations),
    fmt(r.observations_via_rare_channel),
    pct(r.share),
    { node: r.covered ? pill("covered") : pill("missed", "gap") },
  ]);
  replaceTable("rare-table", ["稀有类型", "人口", "观测(全部)", "其中稀有通道", "人口占比", "状态"], rareRows);
  const nk = stats.new_since_reference || {};
  $("new-since").replaceChildren(
    el("span", { text: "新增分区：" + ((nk.partitions || []).join(", ") || "无") }),
    el("span", { text: "新增类型：" + ((nk.kinds || []).join(", ") || "无") })
  );
  const warnings = stats.warnings || [];
  warnings.forEach((w) => notice(w, "warn"));
}

// -------------------------------------------------------------------- tasks
async function loadTasks() {
  const data = await api("/api/tasks?limit=25");
  const rows = data.tasks.map((t) => [
    t.id.slice(0, 8),
    t.kind,
    t.method || "–",
    { node: pill(t.status, t.status === "complete" ? "" : t.status === "failed" ? "gap" : "") },
    t.seed || "–",
    fmt(t.budget),
    fmt(t.actual_rows),
    fmt(t.attempts),
    t.created_at,
    t.error ? String(t.error).slice(0, 70) : "",
  ]);
  replaceTable("tasks", ["任务", "类型", "方法", "状态", "种子", "预算", "样本", "尝试", "创建", "错误"], rows);
  const ev = await api("/api/events?limit=40");
  $("events").replaceChildren(
    ...ev.events.map((e) => el("div", {
      text: `${e.at}  [${e.level}]${e.task_id ? " " + e.task_id.slice(0, 8) : ""}  ${e.message}`,
    }))
  );
}

// -------------------------------------------------------------------- full
async function runFullQuery() {
  if (!state.previewTask) { notice("先运行一次预览，再对照全量。", "warn"); return; }
  setBusy(true);
  try {
    const task = await api("/api/full-queries", {
      method: "POST",
      body: JSON.stringify({ snapshot_id: state.previewTask.snapshot_id, limit: 100 }),
    });
    notice("全量查询已启动（精确扫描同一快照）。", "info");
    let full = task;
    for (let i = 0; i < 600; i++) {
      full = await api(`/api/full-queries/${task.id}`);
      if (["complete", "failed"].includes(full.status)) break;
      await new Promise((r) => setTimeout(r, 300));
    }
    if (full.status !== "complete") {
      notice("全量查询失败：" + (full.error || full.status), "bad");
      setBusy(false);
      return;
    }
    state.fullTask = full;
    const data = await api(`/api/compare/${state.previewTask.id}/${full.id}`);
    renderCompare(data.comparison);
  } catch (err) {
    notice(err.message, "bad");
  } finally {
    setBusy(false);
    loadTasks();
  }
}

function renderCompare(cmp) {
  $("compare-panel").hidden = false;
  $("compare-summary").replaceChildren(
    el("span", { text: `全量记录 ${fmt(cmp.full_total)} · 预览快照人口 ${fmt(cmp.preview_population)}` }),
    el("span", { text: `完全未命中类型 ${fmt((cmp.missed_kinds || []).length)}：${(cmp.missed_kinds || []).join(", ") || "无"}` }),
    el("span", { text: `无估计类型（仅覆盖通道） ${fmt((cmp.unestimated_kinds || []).length)}` })
  );
  const rows = cmp.rows.map((r) => {
    const err = r.error === null ? "–" : fmt(r.error);
    const rel = r.relative_error === null ? "–" : pct(r.relative_error);
    const caught = r.caught_by_preview ? pill("seen") : pill("missed", "gap");
    return [
      r.kind,
      fmt(r.full_count),
      pct(r.full_share),
      r.preview_estimate === null ? "—（无估计）" : fmt(r.preview_estimate),
      r.preview_ci95 === null ? "–" : `${fmt(r.preview_ci95[0])} – ${fmt(r.preview_ci95[1])}`,
      err,
      rel,
      fmt(r.observed_in_preview),
      { node: caught },
    ];
  });
  replaceTable("compare-table", ["类型", "全量精确", "全量占比", "预览估计", "95% CI", "误差", "相对误差", "预览观测", "覆盖"], rows);
  $("compare-note").textContent = (cmp.estimator || "") + " " + (cmp.disclaimer || "");
}

// --------------------------------------------------------------- maintenance
async function mergeNow() {
  if (!state.snapshot) return;
  const dates = $("merge-dates").value.split(",").map((s) => s.trim()).filter(Boolean);
  if (!dates.length) { notice("请输入要合并的日期。", "warn"); return; }
  setBusy(true);
  try {
    const result = await api("/api/merge", {
      method: "POST",
      body: JSON.stringify({ dates, base_snapshot_id: state.snapshot.snapshot_id }),
    });
    $("maintenance-note").textContent =
      `已合并 ${result.merged_partitions.join(", ")} → 快照 ${result.snapshot_id.slice(0, 10)}，记录 ${fmt(result.record_count)}。旧快照仍可解析。`;
    await loadSnapshot();
    await loadTasks();
  } catch (err) {
    notice(err.message, "bad");
  } finally {
    setBusy(false);
  }
}

async function addPartition() {
  setBusy(true);
  try {
    const result = await api("/api/partitions", {
      method: "POST",
      body: JSON.stringify({
        date: $("new-date").value.trim(),
        kind: $("new-kind").value.trim(),
        weight: Number($("new-weight").value),
        records: 4000,
      }),
    });
    $("maintenance-note").textContent =
      `已写入分区 ${result.date}（${fmt(result.record_count)} 条，新类型 ${result.kind}）→ 快照 ${result.snapshot_id.slice(0, 10)}。再跑预览可看到“新增类型”与覆盖变化。`;
    await loadSnapshot();
    await loadTasks();
  } catch (err) {
    notice(err.message, "bad");
  } finally {
    setBusy(false);
  }
}

async function resumeIncomplete() {
  setBusy(true);
  try {
    const result = await api("/api/tasks/resume", { method: "POST", body: "{}" });
    if (!result.resumed.length) {
      notice("没有处于 failed/interrupted 的预览任务。", "info");
    } else {
      notice(`已恢复 ${result.resumed.length} 个任务：${result.resumed.map((x) => x.slice(0, 8)).join(", ")}`, "info");
      if (result.resumed.length === 1) pollPreview(result.resumed[0]);
    }
    await loadTasks();
  } catch (err) {
    notice(err.message, "bad");
  } finally {
    setBusy(false);
  }
}

async function refreshAll() {
  try {
    await loadSnapshot();
    await loadTasks();
    notice("已刷新。新分区会出现在快照中；已创建的预览仍绑定旧快照。", "info");
  } catch (err) {
    notice(err.message, "bad");
  }
}

// -------------------------------------------------------------------- wire
function wire() {
  $("preview-form").addEventListener("submit", createPreview);
  $("refresh").addEventListener("click", refreshAll);
  $("full-query").addEventListener("click", runFullQuery);
  $("resume").addEventListener("click", resumeIncomplete);
  $("merge").addEventListener("click", mergeNow);
  $("add-partition").addEventListener("click", addPartition);
  $("next").addEventListener("click", () => nextPage(state.page && state.page.next_token));
  $("prev").addEventListener("click", () => nextPage(state.page && state.page.prev_token));
}

(async function init() {
  wire();
  try {
    await loadConfig();
    await loadSnapshot();
    await loadTasks();
  } catch (err) {
    notice("初始化失败：" + err.message, "bad");
  }
})();
