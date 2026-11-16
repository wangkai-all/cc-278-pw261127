# 仓库预览服务（budgeted preview）

一个只依赖 Python 3.11 标准库的真实数据预览服务：在有限读取预算下对已提交的
NDJSON 快照抽样，返回**可重复、带设计权重、带覆盖缺口说明**的样本；续页由
HMAC 签名令牌绑定原始输入与抽样种子；预览统计永远标注为估计值，精确数字必须
另跑全量查询。

自带合成数据是**种子确定、可逐字节重建**的；页面是无构建步骤的原生 HTML/JS。

```bash
python3 -m warehouse bootstrap          # 写入 ./data 与 ./state（约 15s）
python3 -m warehouse serve --port 8099  # http://127.0.0.1:8099
```

`serve` 在数据目录为空时会自动完成同样的两阶段 bootstrap（先 v1 基线快照，再
写入 campaign 分区），所以也可以直接启动。生成的 `data/`、`state/` 都在
`.gitignore` 里，随时可以删掉重建。

---

## 1. 数据与快照

默认语料（`records_per_partition = 6000`）：

| 组成 | 分区 | 文件 | 记录 |
|---|---|---:|---:|
| 基线 v1（2026-11-01…11-14 × us/eu/apac） | 42 | 126 | 252,000 |
| campaign v2（11-15、11-16，含新类型与新 schema） | 6 | 18 | 49,500 |
| **合计** | **48** | **144** | **301,500** |

- 每条记录含 `event_id / ts / region / kind / user_id / schema_version / payload`，
  由 `sha256(seed, date, region, file_index, line_index)` 派生，同一种子永远得到
  相同字节（`python3 -m warehouse verify` 会重建并比较数据集摘要）。
- 每个分区 3 个文件（权重 0.42 / 0.34 / 0.24），文件名 `events-000NN.ndjson`。
- 快照是**内容寻址、不可变**的：`data/_catalog/snapshots/<id>.json` 记录每个文件
  的路径、大小、行数、类型直方图与 sha256；提交时把字节硬链接到
  `data/_blobs/xx/<sha256>` 固定下来，`CURRENT` 只是一个指针文件。
- 因此后续合并（删除原始文件、写入 `events-99999-merged.ndjson`）或新增分区都
  **不会**改变任何已完成任务、进行中任务或续页令牌所看到的输入。
- 新增的分区默认是“自 v1 基线以来新增”的：抽样时会把当前快照与**最旧快照**
  对比，标出 `new_partitions` / `new_kinds`，并对新类型走稀有补充通道。

## 2. 读取预算模型

预算单位是**样本记录数**（`budget`，默认 1200，范围 20…20000）。实际物理成本：

- 建索引时每个文件写入每 8 行一个字节 checkpoint（`CHECKPOINT_STRIDE`），并按
  `(kind, occurrence, sha256(file))` 做确定性的 bottom-k 抽样，每类最多保留 96 个
  行位置（`INDEX_OFFSETS_PER_KIND`）。
- 随机抽一行 = 1 次 seek 到最近 checkpoint + 顺读至多 ~7 行。实测每样本约
  4.1–4.6 行物理读取（stratified / uniform / stratified_rare），`head` 和 `kind`
  是 1.0×（顺序前缀 / 索引位置）。
- 全量精确计数会顺序扫描全部 301,500 行（约 1s），服务从不拿预览数字替代它。

32 万行语料上的实测（1200 条预算、本机 SSD）：进程内预览任务约 0.25s（`head`）
到 0.53s（`stratified_rare`）；完成后的第一页 API 约 2ms。全量精确扫描约 1s。
（命令行单次 `sample` 还包含 Python 启动与快照扫描，约 0.55–0.65s。）

## 3. 五种抽样方法

| 方法 | 分配方式 | 设计权重 | 估计 | 主要风险 |
|---|---|---|---|---|
| `stratified` | 按日期/区域分区人口比例，**每分区保底 1 条** | 文件层 `w = N_file / n_file` | Hájek | 低预算时精度换覆盖 |
| `stratified_rare` | 代表性通道同 `stratified` + 第二阶段补稀有/新增类型（最多 40% 预算，每类目标 12 条，新类型 6 条） | 两通道各自有权重，**不可相加** | 仅代表性通道可估计 | 稀有通道只保证覆盖，不保证精度 |
| `kind` | 按类型分层，每类至少 2 条、按 `sqrt(N_kind)` 分配，受稀疏索引帧上限约束 | 文件×类型层 `N / n` | **不发布**总体估计（稀疏帧是有偏的） | 高频类型受 96 个索引位置限制 |
| `uniform` | 严格按文件人口比例，不分层不保底 | 文件层 `N_file / n_file` | Hájek | 预算 < 文件数时小分区整块消失 |
| `head` | 按文件顺序读前缀 | 无 | **不发布**估计 | 低占比类型与后段分区完全缺席 |

补充说明：

- `stratified_rare` 的稀有补充会确定性选择索引位置；若该行已被代表性通道占用，
  会顺延到同类型帧的下一个未占用位置；整帧都被占用时记入
  `satisfied_by_other_channel` 并在 warnings 里说明“实际行数少于计划”，绝不复制
  同一行来凑数。
- 若某类型人口小于其目标观测数，补充量以其人口为上限。
- 计划实际只用了预算的一部分时（例如可用位置不足），任务会带
  `plan uses X of Y records` 警告。

### 权重与估计

代表性/均匀/kind 行的 `weight` 是层人口除以层样本量（`N_h / n_h`）。统计使用
Hájek 比率估计：

```
p_hat = Σ w_i·1[kind_i=k] / Σ w_i
```

方差用逐层 with-replacement 近似并带有限总体校正：

```
var(p_hat) = Σ_h (1 - n_h/N_h) · n_h/(n_h-1) · Σ_{i∈h} z_i²
z_i = w_i (y_i - p_hat) / Σ w
```

95% CI 为 `p_hat ± 1.96·se`，估计总量为 `p_hat × 快照总记录数`（总人口本身是
快照里的精确值）。

`head` 没有设计权重，`/api/tasks/<id>/stats` 会明确返回 `estimable: false`；
`kind` 和稀有补充通道的行不并入总体估计。**预览统计的分母不是全量结果**，
这一点写进了每个 stats 响应的 `not_full_result` 字段和页面横幅。

## 4. 覆盖缺口

每次计划都会计算（并持久化到任务 `plan.coverage`）：

- 每个分区的计划样本量与人口占比；`planned = 0` 即覆盖缺口，并汇总
  `uncovered_population` / `uncovered_share`。
- 每个类型的人口、占比、稀有权重标记、索引帧大小、是否被索引截断。
- 交付后还会统计稀有类型的实际观测（全部通道 / 仅稀有通道），以及
  `rare_kinds_covered / rare_kinds_total`。

预算 1200、32 万行语料的实测：`head` 只覆盖 1/48 分区（另外 47 个整块缺席），
`uniform` / `stratified` / `stratified_rare` / `kind` 覆盖 48/48。预算压到 20 条时
差异立刻显现：`head` 缺 47 个分区，`kind` 缺 42 个，`uniform` 缺 34 个，
`stratified` 缺 28 个，`stratified_rare` 缺 34 个（预算被稀有补充占用）。
完整表格见 [`reports/method-comparison.md`](reports/method-comparison.md)。

## 5. 版本化分页（续页不换样本）

`GET /api/tasks/<id>/rows` 返回 `page_token / next_token / prev_token`。令牌格式：

```
pv1.<base64url(canonical JSON payload)>.<base64url(HMAC-SHA256(payload)[:24])>
```

payload 绑定：`task_id, kind, snapshot_id, method, seed, budget, page_size, filter,
planned, sample_digest, offset, iat, v`。校验时：

- HMAC 不匹配 → 409 `stale_page_token`；
- 令牌属于其他任务、页大小被改、任务样本摘要变化 → 409；
- 超过 7 天 TTL → 409；
- 服务端只接受令牌里的 offset，**不接受**同时覆盖 offset 的请求参数。

直接把 `?offset=N` 当作兼容路径仍然可用，但响应会带 `paged_by_token: false`，
页面也会提示该页未经过令牌校验。任何续页都不会触发重新抽样：样本在任务完成时
已经按 `sample_digest` 落库。

## 6. 并发：预览、全量、合并

- 预览和全量查询都通过 `catalog.resolve(snapshot_id)` 绑定到**不可变快照**；
  计划也在任务创建时持久化，续页不依赖当前仓库状态。
- 合并（把若干日期的分区各压成一个文件）在 `merge_lock + scan_lock` 下执行，
  先 pin 新字节、再写新文件、最后提交快照；只有 `CURRENT` 仍指向发起时的基准
  快照时才推进（compare-and-swap），否则 409 让调用方重试。
- 合并**不会**因为存在进行中的预览或全量查询而被拒绝：在途任务持有快照与
  pinned 字节，合并后其行、令牌和 `sample_digest` 均保持不变
  （`tests/test_api.py::test_merge_while_preview_runs_does_not_disturb_it`）。
- 同一快照上的预览与全量扫描可以并行；`/api/compare/...` 只接受**同一快照**
  的两个任务，跨快照直接 409，避免拿合并后的全量去“对照”合并前的预览。
- 抽样执行按文件分片；每个文件的 RNG 由
  `(seed, snapshot_id, method, path, channel/kind)` 派生，所以失败重试、并发
  调度顺序、分片完成顺序都不影响最终样本。

## 7. 失败恢复

- 每个文件 shard 完成后立刻 checkpoint 到 SQLite（`task_shards` + `shard_rows`
  + `sample_rows`）。
- `POST /api/tasks/<id>/retry`：复用所有已完成 shard，只重跑失败/未开始的分片；
  冻结原 `snapshot_id`、`seed`、`budget`、`method`，`attempts += 1`。即使期间
  仓库新增了分区，重试也不会漂移到新快照。
- 进程重启时 `Store.mark_interrupted_tasks()` 把 `queued/running/assembling`
  标为 `interrupted`；`POST /api/tasks/resume` 批量重排可恢复任务。
- 任务创建支持 `fault_after_files`（仅用于测试/演示）注入故障，页面上的“恢复
  失败任务”按钮走同一个 resume 接口。

## 8. 可重建性与可重复性

- 数据：`python3 -m warehouse bootstrap --force` 从种子重建；`verify` 在临时目录
  重建并比较 `[[path, sha256, lines], …]` 摘要，默认语料摘要为
  `3a3ff161…33ab0`。
- 抽样：同一 `(snapshot, method, seed, budget)` 产生完全相同的行序与
  `sample_digest`；不同 seed 必然不同。页面与 API 都把 seed 显示在任务状态行，
  方便复现。
- 令牌密钥在 `state/token.key`（0600，首次启动生成）。换密钥或换快照后旧令牌
  全部失效，这是有意的。

## 9. HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/health` | 健康检查 |
| GET | `/api/config` | 方法列表、预算/页大小限制、稀有阈值 |
| GET | `/api/snapshot` | 当前快照、最近 20 个快照、存储统计 |
| GET | `/api/snapshots` | 全部快照索引 |
| GET | `/api/snapshot/files` | 当前快照的文件、行数、类型直方图、索引截断 |
| GET | `/api/events?limit=` | 任务事件流 |
| POST | `/api/previews` | `{method, seed, budget, page_size, snapshot_id?, fault_after_files?}` |
| GET | `/api/tasks?kind=&limit=` | 任务历史 |
| GET | `/api/tasks/<id>` | 任务详情（含 plan、stats、shards） |
| GET | `/api/tasks/<id>/stats` | 估计值 + 覆盖缺口 + 稀有覆盖 + 新增项 |
| GET | `/api/tasks/<id>/rows?token=` | 版本化分页；无 token 时退化到 `offset` |
| POST | `/api/tasks/<id>/retry` | 断点重试 |
| POST | `/api/tasks/resume` | 批量恢复 failed/interrupted 预览 |
| POST | `/api/full-queries` | `{kind?, limit?, snapshot_id?}` 精确全量扫描 |
| GET | `/api/full-queries/<id>` | 全量任务 + 前 `limit` 条摘录 |
| GET | `/api/compare/<preview>/<full>` | 同快照预览估计 vs 精确计数 |
| POST | `/api/merge` | `{dates, base_snapshot_id}` 合并日期分区 |
| POST | `/api/partitions` | `{date, kind, weight, records}` 写入新分区（可含新类型） |

错误统一为 `{"error": "..."}`；令牌问题额外带 `{"code": "stale_page_token"}`。

## 10. 页面

`static/index.html + app.js + styles.css`，无构建、无框架、无 `innerHTML`
（DOM 全部用 `textContent`/`createElement` 构造，配合 CSP）。可做：

- 选方法、调预算/页大小/种子，固定到当前快照；
- 查看任务状态、读取预算四张表（实际样本、预算占用、物理读取、每样本读取）；
- 类型估计表（含 95% CI 与“Estimates only”声明）、分区覆盖缺口、稀有类型覆盖、
  新增分区/类型；
- 用上一页/下一页走真实令牌续页；
- 启动全量查询并查看逐类型“精确 vs 估计”对照，明确区分
  `observed_in_preview`（含稀有通道）与 `preview_estimate`（仅代表性通道）；
- 合并分区、写入新分区、重试或恢复失败任务、查看事件流。

## 11. 测试与报告

```bash
python3 -m unittest discover -s tests -p 'test_*.py'   # 38 个测试，约 65s
python3 scripts/smoke.py --records 600                 # 起真实 HTTP 服务走完整流程
python3 -m warehouse.report --base . --budgets 20,200,1200
```

- `tests/test_generate.py`：生成确定性、campaign 新类型、逐字节重建、索引位置正确。
- `tests/test_sampling.py`：分配算法、SRSWOR 唯一性/确定性、各方法预算与权重、
  分区覆盖、稀有补充、索引截断、读取放大上界。
- `tests/test_api.py`：令牌绑定/篡改/过期/换页大小、合并期间续页、合并与预览并发、
  跨快照对照拒绝、故障重试复用分片、重启恢复、估计值标注、head 无估计、
  稀有通道不计入估计。
- `tests/ui_check.mjs`（需要 Node）：用 DOM shim 驱动真实 `app.js` 对运行中的服务
  走完整流程：

```bash
python3 -m warehouse --base /tmp/ui-demo serve --port 8145 &
node tests/ui_check.mjs http://127.0.0.1:8145
```

- `scripts/smoke.py`：临时目录里 bootstrap 小语料、起真实 HTTP 服务，依次验证
  preview → 令牌续页 → 统计声明 → 全量对照 → 注入故障后重试 → 新增分区 → 合并
  → 合并后旧令牌仍翻原样本。无需外部依赖。

`warehouse.report` 会用同一快照上的精确全量计数作为真值，对每种方法在多个预算下
输出读取成本、覆盖缺口、稀有类型观测和相对误差，写入
`reports/method-comparison.{json,md}`。

## 12. 已知边界

- 这是预览，不是查询引擎：抽样只按行位置/类型/分区做分层，没有任意过滤条件；
  `filter` 字段已进入令牌绑定但与全量查询的 `kind` 过滤是两套语义。
- 全量查询为了简单会顺序扫描整份快照，并把前 `limit` 条摘录存进 SQLite；它适合
  对照，不适合作为常规分页查询。
- 每个类型最多保留 96 个索引位置，人口远大于此的高频类型无法通过 `kind` 方法
  精确按比例展开；计划会显式给出帧上限警告。
- 合并会删除被合并的原始文件（pinned blob 仍在），因此原始文件路径只在旧快照
  的 `storage_path` 回退到 blob 时可用；这是有意的空间取舍。
- 令牌 TTL 7 天；过期的预览需要重新发起（用同一 seed 可复现同样样本）。
