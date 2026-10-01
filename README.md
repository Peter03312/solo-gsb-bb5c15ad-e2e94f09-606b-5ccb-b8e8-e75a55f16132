# 工厂校准时间线服务（Calibration Timeline Service）

一个面向工厂校准流水线的 FastAPI 服务。它接收**同一传感器**在校准批次中
**乱序、时间重叠且可能撤回**的测量片段修订（measurement-fragment revisions），
按照**版本因果关系**与**时间区间切分规则**，生成一条**确定性**的有效标定时间线；
当修订引用了尚未上传的父版本时，返回**可补传的缺口清单（gaps）**；
重复提交通过幂等键保证**幂等**；在真正提交前还可以**无副作用地预览**一次修订
会给所属批次带来的影响（preview）；还可对**两个批次的同一传感器**做
**差异对比审计（compare）**，对单个传感器的折叠时间线做
**质量门禁评估（quality gate）**，以及在指定半开时段内判断
**全部必需传感器何时同时满足标定条件的设备放行评估（release）**，以及对
**一组冗余传感器**做**可信多数投票（majority）**。出厂留档时还可把
**已完成批次封存（seal）**为一份不可改写的证据快照：冻结当时全部修订与
折叠结果并生成确定性证据摘要，之后可随时回查并核对实时批次是否仍与封存
一致。修订、幂等键映射与封存证据默认保存在**持久化的 SQLite 存储**中，
服务重启后仍然保留，并可由同一数据库文件上的**多个服务进程共享**；
也可通过配置切换回进程内存模式。可随时重置（重置会连同持久化数据一并
清除并返回准确计数）。

---

## 1. 一条命令启动

需要 Docker（含 Compose v2）：

```bash
docker compose up --build
```

服务监听并对外暴露 **`http://localhost:8080`**：

- 健康检查：<http://localhost:8080/health>
- 交互式 API 文档（Swagger UI）：<http://localhost:8080/docs>
- OpenAPI 描述：<http://localhost:8080/openapi.json>

Compose 默认以 **2 个 uvicorn worker 进程**启动，二者通过同一个 SQLite
数据库文件（WAL 模式，挂载在命名卷 `calibration-data` 上）共享全部状态：
无论请求落到哪个进程，修订、幂等键与封存证据都一致；写入在 SQLite
`BEGIN IMMEDIATE` 事务级串行，同一修订/幂等键的并发提交仍严格返回一个
`accepted`、其余 `duplicate`（或负载冲突 `409`）。数据随命名卷保留，
`docker compose restart` 或重建容器后不丢失；`docker compose down -v`
才会删除卷（或调用 `POST /admin/reset` 清空数据）。

停止：`Ctrl+C`（或另一终端执行 `docker compose down`）。

> 依赖在镜像构建阶段安装（见 `Dockerfile`），路由、日志等初始化在进程启动时
> 通过 FastAPI lifespan 完成，无需额外迁移步骤：SQLite 表结构在服务首次
> 启动时自动创建（幂等 `CREATE TABLE IF NOT EXISTS`）。

### 不用 Docker 时（本地 Python 3.11）

```bash
pip install -r requirements.txt
# 默认使用当前目录下的 SQLite 文件 data/calibration.db（父目录自动创建）
uvicorn app.main:app --host 0.0.0.0 --port 8080

# 多进程共享同一数据库（任意一种启动方式均可，指向同一路径即可）
uvicorn app.main:app --host 0.0.0.0 --port 8080 --workers 2

# 切回进程内存模式（重启即丢失）
CAL_STORAGE=memory uvicorn app.main:app --host 0.0.0.0 --port 8080
```

运行测试：

```bash
pip install -r requirements-dev.txt
pytest -q
```

> 测试套件默认强制使用内存后端（见 `tests/conftest.py` 中的
> `CAL_STORAGE=memory`），不会在仓库里创建数据库；持久化行为由
> `tests/test_persistence_sqlite.py` 单独覆盖（含两个真实 OS 进程共享、
> 双 uvicorn worker 冒烟、重启后封存/幂等回放与存储故障 503）。

---

## 2. 必要配置

所有配置均为可选，内置默认值即可直接运行。环境变量统一使用 `CAL_` 前缀
（也可在项目根目录放 `.env`，参考 `.env.example`）：

| 变量             | 默认值     | 说明                       |
| ---------------- | ---------- | -------------------------- |
| `CAL_HOST`       | `0.0.0.0` | 监听地址                   |
| `CAL_PORT`       | `8080`     | 监听/映射端口              |
| `CAL_LOG_LEVEL`  | `info`     | 日志级别                   |
| `CAL_STORAGE`    | `sqlite`   | 存储后端：`sqlite`（持久化、多进程共享）或 `memory`（进程内存，重启即失） |
| `CAL_SQLITE_PATH` | `data/calibration.db` | `sqlite` 模式的数据库文件路径；父目录自动创建，库旁会产生 `-wal`/`-shm` 文件。不允许填 `:memory:`（不共享、不持久） |
| `CAL_SQLITE_BUSY_TIMEOUT_MS` | `10000` | 等待其他进程释放 SQLite 写锁的毫秒数，超时按存储故障处理 |

**存储语义：**

- `sqlite`（默认）：修订、幂等键映射、封存证据全部落盘。服务重启或容器
  重建（保留数据卷）后仍可查询、重放；多个进程（如 `--workers N`）打开
  同一文件时共享同一份数据。每个写入接口在**一个 `BEGIN IMMEDIATE`
  事务**内完成“判定 + 写入 + 求值”，该事务同时是跨进程临界区：修订与
  其幂等键映射原子提交；封存的“查重 → 校验 `timeline_hash`/完整性 →
  冻结证据 → 落库”也在同一事务内原子完成，并发提交无法插入其间。
  只读接口在同一快照事务内取数，预览读取的是同一时点的批次快照且不落
  任何数据。
- `memory`：历史行为，所有状态仅存进程内存，重启或重置即清空。
- **存储故障不等于空批次**：数据库无法打开、读写失败时接口返回
  **`503`**（`{"detail": "storage unavailable: ..."}`），绝不会把故障
  伪装成 `404`/空结果；启动阶段无法初始化数据库则进程直接失败退出
  （fail fast），不会静默降级到空内存。

---

## 3. 领域语义（确定性规则）

- 修订按 `(batch_id, sensor_id)` 分组，每个修订有恰好一个父修订 `parent_id`；
  字面量 `"ROOT"` 表示因果根。修订 ID 不允许使用保留值 `"ROOT"`。
- 时间区间为**半开区间** `[start_ns, end_ns)`，单位为纳秒整数；
  相邻片段端点相接但不共享任何采样时刻，且要求 `end_ns > start_ns`。
- 修订可以**任意顺序**到达。只有能沿父链追溯到已知 `ROOT` 的修订才是
  **grounded（落地）**状态，参与时间线；
  - 父版本缺失 → 该修订及其后代为 **blocked**，不写入时间线，缺失父版本
    出现在 `gaps` 缺口清单中（含等待它的子版本 `awaited_by` 与引用来源
    `referenced_from`），可直接按清单补传；
  - 父链成环（含自环）→ 环上及其后代标记为 **invalid**，不参与时间线。
- **折叠（fold）顺序完全确定**：先按因果深度（距 ROOT 的跳数）升序，
  同深度按 `revision_id` 字典序升序；后应用者在重叠时间上覆盖先应用者。
  因此最终时间线与事件到达顺序无关（有排列不变性测试保证）。
- **撤回（withdrawn=true）是墓碑**：在其区间内把已有标定值擦除为
  `state="withdrawn"`、`value=null`；撤回修订的后代仍可在因果上重新写回值。
- **幂等**（持久化存储中跨重启、跨进程同样生效）：
  - 相同 `idempotency_key` + 完全相同负载 → 返回 `status="duplicate"`，状态不变；
  - 相同 `idempotency_key` 但负载不同（或同 `revision_id` 内容不同）→ `409 Conflict`；
  - 同一 `revision_id` + 相同内容即使换了幂等键也算重复，**新键会被登记**，
    修订与键映射在同一事务内原子保存。
- **提交预览（preview，见 4.2）**：传入与提交完全相同的负载，按相同的校验、
  幂等冲突与因果折叠规则计算，但**不写入任何事件、不占用幂等键、不创建批次**。
  预览返回本次为 `submittable`（可提交）还是 `duplicate`（重复）、预览前后的
  批次 `timeline_hash`、该传感器**有效状态/值/单位**发生变化的半开区间
  （`changed_intervals`；仅获胜来源修订变化不算有效值变化，不输出），以及
  **新增（`gaps_added`）与消除（`gaps_removed`）的缺失父修订清单**。
  尚不存在的批次按空批次预览；被阻塞的候选修订即使不改变时间线，也会在
  `gaps_added` 中反映新增缺口；补传缺失父修订时，原本被阻塞的后代被激活，
  其影响会体现在 `changed_intervals` 与 `gaps_removed` 中。相同修订或幂等键
  的冲突返回与提交一致的 `409`，非法负载返回 `422`；预览与并发提交各自基于
  同一事务（跨进程加锁）内的一致批次快照计算，且预览**不写库**：重启后
  同样查不到预览产生的批次或键占用。
- 结果中包含两个 SHA-256 内容哈希：每个传感器的 `segments_hash`
  和整批的 `timeline_hash`，可用于比对一致性与去重校验。
- 相邻且状态、值、单位、来源修订完全相同的片段会被合并；来源修订不同则保留边界。
- **采样点查询**（见 4.4）直接复用上述折叠结果判断单个纳秒时刻：
  落在 `calibrated` 片段上返回有效值与单位，落在 `withdrawn` 墓碑上返回
  `withdrawn` 且 `value=null`，所有已落地片段之外为 `uncovered`；
  若该时刻被某个 **blocked** 修订覆盖，则即使已有 grounded 片段也报告
  `blocked`（补传缺失祖先可能改变折叠赢家），并在 `missing_ancestors`
  中按字典序列出沿父链追溯到的、尚未上传的末端祖先，可直接照单补传。
  成环（invalid）修订不影响采样判定。
- **跨批次对比**（见 4.5）复用同一套确定性折叠结果：对两个批次中同一
  传感器的有效时间线做半开区间差异扫描，只比较有效状态
  （`state`/`value`/`unit`），与修订来源（revision_id）及到达顺序无关；
  任一侧存在缺口或成环修订时标记 `incomplete` 并给出原因。
- **质量门禁**（见 4.6）对单个传感器的折叠时间线做确定性扫描：以该传感器
  全部 grounded 与 blocked 修订的**并集跨度**为扫描窗口（成环 invalid 修订
  不参与），逐基本区间判定——`blocked` 覆盖优先（补传可能改变折叠赢家），
  记为缺口；`calibrated` 值落在闭区间 `[min_value, max_value]` 之外记为
  候选告警；`withdrawn` 墓碑与未覆盖空洞记为缺口。相邻候选告警按纳秒邻接
  合并为一段，**仅当持续时长达到 `min_duration_ns`** 才输出为 violation；
  缺口只与相同状态的相邻缺口合并，**绝不并入告警段**。`missing_policy`
  为 `ignore` 时缺口仅记录、不影响判定；为 `fail` 时存在任何缺口即判
  `fail`。响应中的 `timeline_hash` 是对扫描输入（折叠片段 + blocked 跨度）
  的 SHA-256：扫描输入不变则哈希不变，与门禁参数无关。
- **设备放行**（见 4.7）在同一次批次快照内对**多个必需传感器**做只读扫描：
  在半开查询窗口 `[window_start_ns, window_end_ns)` 内逐基本区间判定，
  **所有**传感器同时为 `calibrated`、单位与各自 `expected_unit` 完全一致、
  值落在各自闭区间 `[min_value, max_value]` 内时该区间才合格。判定沿用
  同一套因果折叠口径：被 **blocked** 修订覆盖优先判不合格（即使下方已有
  grounded 标定值），原因给出去重排序的 `missing_ancestors`，可直接照单
  补传；`withdrawn` 墓碑判 `withdrawn`、已落地片段之外判 `uncovered`、
  单位不一致判 `unit_mismatch`、值越界判 `out_of_bounds`（闭区间边界算
  合格）；成环（invalid）修订不参与。相邻合格区间无条件合并；相邻不合格
  区间仅在每传感器原因集合（sensor + reason）完全相同时合并，
  `out_of_bounds` 汇总区间极值、`unit_mismatch` 汇总出现过的单位、
  `blocked` 汇总缺失祖先。**仅时长达到 `min_duration_ns` 的完整连续合格
  段才出现在 `released_intervals`**；较短的合格段进入
  `unreleased_intervals` 并标 `qualified=true`、`too_short=true`；不合格
  段同样进入 `unreleased_intervals` 并按传感器列出原因。两个区间列表合并
  后按时间排序，恰好覆盖整个查询窗口（半开、按窗口裁剪）。响应中的
  `input_hash` 是对各必需传感器扫描输入（折叠片段 + blocked 跨度）的
  SHA-256：快照输入不变则哈希不变，与窗口和放行参数无关。
- **可信多数投票**（见 4.8）在同一次批次快照内对**至少两个不重复的冗余
  传感器**做只读扫描，在半开查询窗口 `[window_start_ns, window_end_ns)`
  内逐基本区间判定。票数门槛 `q` 必须是严格过半且不超过传感器总数
  （`floor(n/2)+1 <= q <= n`）。每个时段先按 **blocked 覆盖优先**规则
  遮蔽该传感器的已落地值；仅**未遮蔽、折叠赢家为 `calibrated`、单位与
  `expected_unit` 完全一致**的值可投票。可投票值中求**最大相容集合**
  （集合内 `max(value) - min(value) <= max_value_difference`，阈值边界闭
  区间）；人数最多者获胜，人数相同取成员传感器 ID 排序后字典序最小者。
  - 获胜集合人数 `>= q` → `qualified`，成员逐一带回值与单位；
  - 否则若“获胜集合人数 + 当前被 blocked 遮蔽的传感器数” `>= q` →
    `pending_backfill`（补传后多数可能达成），并在区间与每个 blocked
    传感器上给出去重排序、可直接照单补传的末端 `missing_ancestors`；
  - 其余 → `unqualified`。
  - 未投票传感器逐列原因：`blocked`（含 `missing_ancestors`）、
    `withdrawn`、`uncovered`、`unit_mismatch`（带回折叠单位）以及
    `value_divergent`（投了票但值不在获胜相容集合内，带回其值）。
  - 输出区间按时间排序、恰好连续覆盖整个查询窗口（半开、按窗口裁剪）；
    **仅相邻区间的判定（verdict）与证据（成员及每人原因/值/单位/缺失
    祖先）完全相同才合并**。整批 `status` 取区间判定的最高优先级：
    出现任一 `qualified` 即 `qualified`，否则出现任一 `pending_backfill`
    即 `pending_backfill`，否则 `unqualified`。成环（invalid）修订不参与。
    响应中的 `input_hash` 是对各传感器扫描输入（折叠片段 + blocked 跨度）
    的 SHA-256：快照输入不变则哈希不变，与窗口、单位、`q` 和阈值无关。
- **批次封存**（见 4.9）把已完成批次固定为一份可复核的出厂留档证据。
  封存请求携带批次号、调用方自定义的**封存号**以及调用方先前取得的
  `timeline_hash`；在**同一事务（跨进程加锁）的同一批次快照**内核对哈希且
  批次 `complete=true`（无 blocked/invalid 修订）后，冻结**当时全部修订**
  （按 `(sensor_id, revision_id)` 排序的完整记录）与**折叠结果**，并生成
  覆盖两者的确定性 SHA-256 **证据摘要**（`evidence_digest`，与修订到达
  顺序无关）。校验与冻结在**一个原子事务**内完成，并发提交无法穿插改写。
  相同批次下相同封存号 + 相同请求的重放返回**原快照**
  （`status="duplicate"`，即使实时批次已变化、即使发生过重启）；封存号
  相同但参数不同 → `409`；批次不存在 → `404`；哈希已变化或仍有
  blocked/invalid 修订 → `409`，且**不留下任何封存记录**（事务回滚）。
  封存证据持久化保存，重启后仍可回放原请求并取回原证据；封存后到达的
  修订只改变实时结果，**不能改写旧封存**；回查（见 4.10）比较**持久化
  封存证据**与当前批次重算的证据摘要并给出 `consistent`（实时证据是否
  仍与封存一致）。

---

## 4. HTTP 接口

### `POST /api/v1/batches/{batch_id}/events` — 提交一条测量片段修订

请求体：

```json
{
  "batch_id": "B42",
  "sensor_id": "S1",
  "revision_id": "v2",
  "parent_id": "v1",
  "idempotency_key": "evt-20260921-002",
  "start_ns": 100,
  "end_ns": 200,
  "value": 3.5,
  "unit": "C",
  "withdrawn": false
}
```

说明：

- 路径中的 `batch_id` 必须与请求体一致，否则 `422`；
- `withdrawn=false` 时 `value` 必填；`withdrawn=true` 时 `value` 可省略；
- 响应 `200` 中 `status` 为 `accepted` / `duplicate`，并**直接带回该批次当前
  完整结果**（时间线、缺口、计数与哈希），便于流水线即时判断是否可补传；
- 幂等键冲突或同修订 ID 内容冲突返回 `409`；参数非法返回 `422`；
- 持久化后端下，多个进程并发提交同一修订/幂等键时，写入在一个
  `BEGIN IMMEDIATE` 事务内串行：恰好一个请求得到 `accepted`，相同负载的
  其余请求得到 `duplicate`，负载不同的得到 `409`；存储不可用返回 `503`。

### `POST /api/v1/batches/{batch_id}/events:preview` — 提交前预览修订影响（不写入）

流水线在**真正提交测量修订前**用本接口预览它对所属批次的影响。请求体与
4.1 的提交接口**完全相同**（同样的字段与校验规则；路径 `batch_id` 仍须与
请求体一致），但本接口**不写入事件、不注册幂等键、不创建批次**，可以安全地
反复调用。

判定与计算完全复用提交路径的规则（同一次加锁内完成判定与求值，与并发提交
各自基于一致的批次快照）：

- 相同幂等键 + 相同负载，或相同 `revision_id` + 相同内容 →
  `status="duplicate"`，前后快照一致、变化清单为空；
- 相同幂等键但负载不同，或同 `revision_id` 内容不同 → 与提交一致的 `409`；
- 非法负载（含路径/请求体批次不一致）→ 与提交一致的 `422`；
- 其余情况 `status="submittable"`，按“当前批次 + 候选修订”的**虚拟快照**
  求值。尚不存在的批次按**空批次**预览（空批次 `timeline_hash` 为对 `[]` 的
  SHA-256）。

响应字段：

| 字段 | 说明 |
| --- | --- |
| `status` | `submittable`（新修订，可提交）/ `duplicate`（重复，提交后状态不变） |
| `before_timeline_hash` / `after_timeline_hash` | 预览前 / 预览后（虚拟提交后）的整批 `timeline_hash`，口径与批次结果一致 |
| `before` / `after` | 两个快照的**完整批次结果**（时间线、缺口、计数、各传感器哈希），便于直接比对 |
| `changed_intervals` | 候选修订所属传感器上，**有效状态/值/单位**发生变化的半开区间；仅来源修订（`revision_id`）变化不算有效值变化，不输出 |
| `gaps_added` / `gaps_removed` | 该传感器上**新增 / 消除的缺失父修订缺口**（完整 gap 对象，含 `awaited_by`/`referenced_from`） |

`changed_intervals` 中每个区间的 `kind`：

- `added`：此前未覆盖、预览后被覆盖（`before` 为 `null`）；
- `removed`：此前覆盖、预览后变为未覆盖（`after` 为 `null`；正常新增修订时
  不会出现，仅在形成父环等使修订失效的场景下可能出现）；
- `changed`：两侧都被覆盖，但 `state`/`value`/`unit` 不同。`before`/`after`
  均为 `{state, value, unit}`（`withdrawn` 时 `value`/`unit` 为 `null`）。

相邻且 `kind` 与两侧有效负载完全相同的区间会合并（忽略修订来源边界）。

注意：

- **被阻塞的候选修订**即使不进入折叠、时间线哈希不变，也会在 `gaps_added`
  中给出可补传的缺失父修订；
- **补传缺失父修订**会让此前 blocked 的后代被激活，其折叠影响体现在
  `changed_intervals`，被填补的缺口体现在 `gaps_removed`（链式补传时还可能同时
  在 `gaps_added` 中暴露出更上游的缺口）；
- 预览**绝不改变状态**：冲突的预览不会占用幂等键，之后仍可正常提交原始事件。

示例（尚不存在的批次 B42，先预览一个父修订缺失的子版本 v2）：

```bash
$ curl -s -X POST localhost:8080/api/v1/batches/B42/events:preview \
    -H 'Content-Type: application/json' \
    -d '{"batch_id":"B42","sensor_id":"S1","revision_id":"v2","parent_id":"v1",
         "idempotency_key":"e2","start_ns":100,"end_ns":200,"value":3.5,"unit":"C"}'
{
  "status": "submittable",
  "idempotency_key": "e2",
  "batch_id": "B42",
  "sensor_id": "S1",
  "revision_id": "v2",
  "before_timeline_hash": "4f53cda1…202b945",
  "after_timeline_hash": "7a87bce5…f404f7",
  "before": {"batch_id": "B42", "sensors": [], "gaps": [],
             "revisions_total": 0, "complete": false,
             "timeline_hash": "4f53cda1…202b945"},
  "after":  { … "revisions_blocked": 1, "complete": false, … },
  "changed_intervals": [],
  "gaps_added": [
    {"batch_id": "B42", "sensor_id": "S1", "missing_revision_id": "v1",
     "awaited_by": ["v2"], "referenced_from": "v2"}
  ],
  "gaps_removed": []
}
# 批次仍不存在（GET /result -> 404），幂等键 e2 也未被占用。
```

待 v2 真正提交后，预览补传父版本 v1（根）：缺口 `v1` 消除，此前 blocked 的
v2 被激活，`[0,100)=1.0 | [100,200)=3.5 | [200,300)=1.0` 三段均显示为
`added`：

```bash
$ curl -s -X POST localhost:8080/api/v1/batches/B42/events:preview \
    -H 'Content-Type: application/json' \
    -d '{"batch_id":"B42","sensor_id":"S1","revision_id":"v1","parent_id":"ROOT",
         "idempotency_key":"e1","start_ns":0,"end_ns":300,"value":1.0,"unit":"C"}'
{
  "status": "submittable",
  …
  "gaps_added": [],
  "gaps_removed": [
    {"batch_id": "B42", "sensor_id": "S1", "missing_revision_id": "v1",
     "awaited_by": ["v2"], "referenced_from": "v2"}
  ],
  "changed_intervals": [
    {"start_ns": 0,   "end_ns": 100, "kind": "added", "before": null,
     "after": {"state": "calibrated", "value": 1.0, "unit": "C"}},
    {"start_ns": 100, "end_ns": 200, "kind": "added", "before": null,
     "after": {"state": "calibrated", "value": 3.5, "unit": "C"}},
    {"start_ns": 200, "end_ns": 300, "kind": "added", "before": null,
     "after": {"state": "calibrated", "value": 1.0, "unit": "C"}}
  ]
}
```

### `GET /api/v1/batches/{batch_id}/result` — 查询批次结果

返回该批次所有传感器的有效时间线、缺口清单、修订计数与整批哈希；
批次不存在返回 `404`。

### `GET /api/v1/batches/{batch_id}/sensors/{sensor_id}/sample?timestamp_ns=N` — 查询单个采样时刻

按 `batch_id`、`sensor_id` 与纳秒时刻 `timestamp_ns`（非负整数，半开区间）
查询该时刻的有效标定状态：

| `state`       | 含义                                                                 |
| ------------- | -------------------------------------------------------------------- |
| `calibrated`  | 折叠赢家是已落地的值片段；`value`/`unit` 有效                         |
| `withdrawn`   | 折叠赢家是撤回墓碑；`value`/`unit` 为 `null`                          |
| `uncovered`   | 没有任何已落地片段覆盖该时刻                                          |
| `blocked`     | 该时刻被尚未落地的修订覆盖；补传后赢家可能改变，列出 `missing_ancestors` |

解析成功时（`calibrated` / `withdrawn`）还返回：

- `revision_id`：该时刻的获胜修订（确定性折叠中最后应用者）；
- `parent_chain`：从获胜修订回溯到根的**有序**父链，含首尾，
  例如 `["v3", "v2", "v1", "ROOT"]`。

`blocked` 时 `revision_id`、`value`、`unit` 为 `null`，`parent_chain` 为空，
`missing_ancestors` 按字典序列出去重后的缺失末端祖先。
未知批次或未知传感器返回 `404`；`timestamp_ns < 0` 返回 `422`。

示例（沿用第 5 节的 B42 / S1：v2 覆盖 [100,200)、v1 覆盖 [0,300)）：

```bash
$ curl -s 'localhost:8080/api/v1/batches/B42/sensors/S1/sample?timestamp_ns=150'
{
  "batch_id": "B42",
  "sensor_id": "S1",
  "timestamp_ns": 150,
  "state": "calibrated",
  "value": 3.5,
  "unit": "C",
  "revision_id": "v2",
  "parent_chain": ["v2", "v1", "ROOT"],
  "missing_ancestors": []
}
```

父版本 `v1` 缺失时查询同一时刻：

```json
{
  "batch_id": "B42",
  "sensor_id": "S1",
  "timestamp_ns": 150,
  "state": "blocked",
  "value": null,
  "unit": null,
  "revision_id": null,
  "parent_chain": [],
  "missing_ancestors": ["v1"]
}
```

### `GET /api/v1/batches/compare?left_batch_id=...&right_batch_id=...&sensor_id=...` — 跨批次对比审计

对**同一传感器**在两个批次中的有效时间线（复用第 3 节的确定性折叠结果）
做半开区间差异扫描。`intervals` 是两侧覆盖范围并集 `[min_start, max_end)`
的**连续、按时间排序的分区**：每个区间带 `kind` 以及两侧各自的有效负载
（`state`/`value`/`unit`，未覆盖的一侧为 `null`）；`kind` 与两侧负载完全
相同的相邻区间会被合并（合并与折叠片段的修订来源边界无关）。

| `kind`           | 含义                                                              |
| ---------------- | ----------------------------------------------------------------- |
| `equal`          | 两侧有效状态一致（都未覆盖也算一致，此时两侧负载均为 `null`）      |
| `left_only`      | 仅左批次覆盖该区间                                                 |
| `right_only`     | 仅右批次覆盖该区间                                                 |
| `value_mismatch` | 两侧均为 `calibrated`，但 `value` 或 `unit` 不同                   |
| `state_mismatch` | 一侧为 `calibrated`、另一侧为 `withdrawn`                          |

响应还包含：

- `left_segments_hash` / `right_segments_hash`：两侧折叠片段的内容哈希，
  与批次结果中的 `segments_hash` 一致，便于交叉校验；
- `incomplete` 与 `incomplete_reasons`：任一侧存在缺口（修订 blocked，
  `reason="gaps"`，含可补传的 `missing_revision_ids`）或成环修订
  （`reason="invalid_revisions"`）时 `incomplete=true`——补传后差异结果
  可能改变；差异本身仍按当前已落地的部分计算。

参数缺失或为空返回 `422`；任一批次不存在、或传感器在任一批次中不存在
返回 `404`。

示例（B42 与 B43 的 S1：B42 为 [0,300)=1.0 且 [100,200)=3.5，B43 整体为
[0,300)=1.0）：

```bash
$ curl -s 'localhost:8080/api/v1/batches/compare?left_batch_id=B42&right_batch_id=B43&sensor_id=S1'
{
  "sensor_id": "S1",
  "left_batch_id": "B42",
  "right_batch_id": "B43",
  "left_segments_hash": "…",
  "right_segments_hash": "…",
  "incomplete": false,
  "incomplete_reasons": [],
  "intervals": [
    {"start_ns": 0, "end_ns": 100, "kind": "equal",
     "left": {"state": "calibrated", "value": 1.0, "unit": "C"},
     "right": {"state": "calibrated", "value": 1.0, "unit": "C"}},
    {"start_ns": 100, "end_ns": 200, "kind": "value_mismatch",
     "left": {"state": "calibrated", "value": 3.5, "unit": "C"},
     "right": {"state": "calibrated", "value": 1.0, "unit": "C"}},
    {"start_ns": 200, "end_ns": 300, "kind": "equal",
     "left": {"state": "calibrated", "value": 1.0, "unit": "C"},
     "right": {"state": "calibrated", "value": 1.0, "unit": "C"}}
  ]
}
```

### `POST /api/v1/batches/{batch_id}/quality-gate` — 质量门禁评估

对指定批次中**单个传感器**的折叠时间线执行确定性质量门禁扫描（规则见第 3 节）。

请求体：

```json
{
  "sensor_id": "S1",
  "min_value": 0.0,
  "max_value": 3.0,
  "min_duration_ns": 100,
  "missing_policy": "ignore"
}
```

参数说明：

- `min_value` / `max_value`：允许值的**闭区间**上下限（有限浮点数，拒绝
  NaN/Inf），且必须满足 `min_value <= max_value`；
- `min_duration_ns`：最短持续时长（整数，≥ 1）。超界连续段合并后达到该
  时长才输出为 violation，未达阈值的短暂越界被抑制；
- `missing_policy`：缺失数据策略。`ignore` = 缺口（`withdrawn` /
  `uncovered` / `blocked`）仅记录；`fail` = 存在任何缺口即判 `fail`。

响应字段：

- `status`：`pass` / `fail`。存在 violation 即 `fail`；`missing_policy=fail`
  且存在缺口时亦为 `fail`；
- `violations`：合并后的超界区间，含 `start_ns`/`end_ns`/`duration_ns`
  及区间内观测到的极值 `min_value`/`max_value`；
- `gaps`：合并后的缺失数据区间，含 `state`；`blocked` 区间另附去重排序的
  `missing_ancestors`（可直接照单补传）；
- `scanned_start_ns` / `scanned_end_ns`：实际扫描的半开窗口（无可扫描
  内容时为 `null`）；
- `timeline_hash`：扫描输入的内容哈希，可用于审计与一致性比对。

批次不存在或传感器不存在返回 `404`；参数非法（含上下限颠倒、阈值小于 1、
未知策略、多余字段）返回 `422`。

示例（沿用第 5 节的 B42 / S1：v2 在 [100,200) 取值 3.5，超出上限 3.0）：

```bash
$ curl -s -X POST localhost:8080/api/v1/batches/B42/quality-gate \
    -H 'Content-Type: application/json' \
    -d '{"sensor_id":"S1","min_value":0.0,"max_value":3.0,
         "min_duration_ns":100,"missing_policy":"ignore"}'
{
  "batch_id": "B42",
  "sensor_id": "S1",
  "status": "fail",
  "scanned_start_ns": 0,
  "scanned_end_ns": 300,
  "violations": [
    {"start_ns": 100, "end_ns": 200, "duration_ns": 100,
     "min_value": 3.5, "max_value": 3.5}
  ],
  "gaps": [],
  "timeline_hash": "…"
}
```

### `POST /api/v1/batches/{batch_id}/release` — 设备放行评估（多传感器同时合格）

对批次内**一组必需传感器**在指定半开时段 `[window_start_ns, window_end_ns)`
内做只读扫描，找出所有传感器**同时**满足各自标定条件的连续区间
（规则见第 3 节）。全部计算在同一次加锁内的同一批次快照上完成，不写入
任何状态。

请求体：

```json
{
  "window_start_ns": 0,
  "window_end_ns": 300,
  "min_duration_ns": 100,
  "sensors": [
    {"sensor_id": "S1", "min_value": 0.0, "max_value": 4.0, "expected_unit": "C"},
    {"sensor_id": "S2", "min_value": 1.0, "max_value": 2.0, "expected_unit": "bar"}
  ]
}
```

参数说明：

- `window_start_ns` / `window_end_ns`：半开纳秒查询窗口，非负整数且
  `window_end_ns > window_start_ns`；结果区间一律裁剪到该窗口内；
- `sensors`：必需传感器条件列表，**非空且 `sensor_id` 不可重复**；每项给出
  闭区间上下限（有限浮点数，拒绝 NaN/Inf，且 `min_value <= max_value`）与
  期望单位（非空字符串，折叠值单位须与之**完全相等**）；
- `min_duration_ns`：最短连续合格时长（整数，≥ 1）；连续合格段达到该门槛
  才可放行，未达门槛的合格段标为时长不足。

响应字段：

- `status`：`released`（存在至少一个达到时长门槛的连续合格段）/
  `not_released`；
- `released_intervals`：达到门槛的放行区间，按时间排序、相邻已合并，每项含
  `start_ns`/`end_ns`/`duration_ns`（均按半开边界计算）；
- `unreleased_intervals`：窗口内其余区间，同样按时间排序、相邻同因合并，
  与 `released_intervals` 合并后恰好连续覆盖整个查询窗口：
  - 不合格段：`qualified=false`、`too_short=false`，`sensors` 按 `sensor_id`
    排序列出在该段上不合格的传感器（合格的传感器不列出）；
  - 时长不足的合格段：`qualified=true`、`too_short=true`，`sensors` 为空。
- 每个传感器原因对象 `sensors[]` 的 `reason`：
  - `blocked`：该时刻被未落地修订覆盖（补传缺失祖先可能改变折叠赢家），
    即使下方已有 grounded 标定值也判不合格；`missing_ancestors`
    给出去重排序、可直接照单补传的末端缺失祖先；
  - `uncovered`：无任何已落地片段覆盖；
  - `withdrawn`：折叠赢家是撤回墓碑（`value`/`unit` 为 `null`）；
  - `unit_mismatch`：折叠单位不是 `expected_unit`；`unit` 为该段首段折叠
    单位，`observed_units` 为合并段内出现过的去重单位（排序）；
  - `out_of_bounds`：折叠值超出闭区间；`value`/`unit` 为该段首段折叠值，
    `observed_min_value`/`observed_max_value` 为合并段内观测极值；
  - 每种原因都回带该传感器的 `min_value`/`max_value`/`expected_unit`；
  - 成环（invalid）修订不参与扫描，其覆盖点按其余折叠/未覆盖结果判定。
- `input_hash`：对所有必需传感器扫描输入（折叠片段 + blocked 跨度）的
  SHA-256，快照输入不变则哈希不变，与窗口、上下限和时长参数无关。

批次或任一请求传感器不存在返回 `404`；查询窗口无效（空或倒置）、传感器
列表为空或重复、上下限非有限或倒置、时长非正、缺失必填字段或多余字段均
返回 `422`。

示例（B42 中 S1 与 S2 在 [0,300) 均合格，但 [120,180) 上 S1 有一个等待
缺失祖先 `v9` 的 blocked 修订；门槛 100ns）：

```bash
$ curl -s -X POST localhost:8080/api/v1/batches/B42/release \
    -H 'Content-Type: application/json' \
    -d '{"window_start_ns":0,"window_end_ns":300,"min_duration_ns":100,
         "sensors":[
           {"sensor_id":"S1","min_value":0.0,"max_value":4.0,"expected_unit":"C"},
           {"sensor_id":"S2","min_value":1.0,"max_value":2.0,"expected_unit":"bar"}]}'
{
  "batch_id": "B42",
  "window_start_ns": 0,
  "window_end_ns": 300,
  "min_duration_ns": 100,
  "status": "released",
  "released_intervals": [
    {"start_ns": 0, "end_ns": 120, "duration_ns": 120},
    {"start_ns": 180, "end_ns": 300, "duration_ns": 120}
  ],
  "unreleased_intervals": [
    {"start_ns": 120, "end_ns": 180, "duration_ns": 60,
     "qualified": false, "too_short": false,
     "sensors": [
       {"sensor_id": "S1", "reason": "blocked",
        "min_value": 0.0, "max_value": 4.0, "expected_unit": "C",
        "value": null, "unit": null, "observed_units": [],
        "observed_min_value": null, "observed_max_value": null,
        "missing_ancestors": ["v9"]}
     ]}
  ],
  "input_hash": "…"
}
```

当两段合格区间都短于门槛（例如此处门槛改为 130ns）时，`status` 为
`not_released`、`released_intervals` 为空，这两段出现在
`unreleased_intervals` 中并带 `"qualified": true, "too_short": true`。

当两段合格区间都短于门槛（例如此处门槛改为 130ns）时，`status` 为
`not_released`、`released_intervals` 为空，这两段出现在
`unreleased_intervals` 中并带 `"qualified": true, "too_short": true`。

### `POST /api/v1/batches/{batch_id}/majority` — 冗余传感器可信多数投票

对批次内**一组冗余传感器（至少两个、ID 不重复）**在指定半开时段
`[window_start_ns, window_end_ns)` 内做只读扫描，逐基本区间判断它们能否
形成**可信多数**（规则见第 3 节）。全部计算在同一次加锁内的同一批次快照
上完成，不写入任何状态。

请求体：

```json
{
  "window_start_ns": 0,
  "window_end_ns": 300,
  "sensors": [
    {"sensor_id": "S1"},
    {"sensor_id": "S2"},
    {"sensor_id": "S3"}
  ],
  "expected_unit": "C",
  "q": 2,
  "max_value_difference": 0.5
}
```

参数说明：

- `window_start_ns` / `window_end_ns`：半开纳秒查询窗口，非负整数且
  `window_end_ns > window_start_ns`；结果区间一律裁剪到该窗口内；
- `sensors`：冗余传感器列表，**至少 2 个且 `sensor_id` 不可重复**；
- `expected_unit`：期望单位（非空字符串）。仅折叠值单位与之**完全相等**
  的 `calibrated` 传感器可投票；
- `q`：达成可信多数所需票数，必须是**严格过半且不超过传感器总数**，即
  `floor(n/2)+1 <= q <= n`（3 个传感器时 `2 <= q <= 3`，2 个时 `q=2`）；
- `max_value_difference`：相容票值允许的**最大极差**（闭区间边界），有限
  非负浮点数，拒绝 NaN/Inf；相容条件为
  `max(value) - min(value) <= max_value_difference`。

投票与判定（每个基本区间）：

1. **blocked 覆盖优先**：该时刻若被未落地修订覆盖，则遮蔽该传感器的已
   落地值，本时段不可投票，原因记 `blocked`，并给出缺失祖先；
2. 其余传感器按折叠赢家判定：`withdrawn` 墓碑 → `withdrawn`；窗口内无
   已落地覆盖 → `uncovered`；单位不符 → `unit_mismatch`；
3. 只有未遮蔽、`calibrated` 且单位匹配的传感器投出数值票；
4. 在可投票值中求**人数最多的相容集合**；人数相同取成员传感器 ID 排序
   后**字典序最小**的集合；
5. `|集合| >= q` → `qualified`；否则 `|集合| + blocked 传感器数 >= q` →
   `pending_backfill`；否则 `unqualified`。

响应字段：

- `status`：任一区间 `qualified` 即 `qualified`；否则任一区间
  `pending_backfill` 即 `pending_backfill`；否则 `unqualified`；
- `intervals`：**恰好连续覆盖整个查询窗口**、按时间排序的半开区间，
  仅相邻区间的 `verdict` 与**完整证据**（成员列表与每个未投票传感器的
  原因/值/单位/缺失祖先）完全相同时才合并；每项含
  `start_ns`/`end_ns`/`duration_ns`、`verdict`、`members`、
  `non_voting_sensors` 与 `missing_ancestors`；
- `members[]`：获胜相容集合，按 `sensor_id` 排序，每项
  `{sensor_id, value, unit}`。`qualified` 时人数 `>= q`；其余判定时
  仍是当前可投票值的最大相容集合（人数 `< q`）；
- `non_voting_sensors[]`：每个未进入 `members` 的被查询传感器，按
  `sensor_id` 排序：
  - `blocked`：`value`/`unit` 为 `null`，`missing_ancestors` 为末端缺失
    祖先（排序、可直接补传）；
  - `withdrawn` / `uncovered`：无值无单位；
  - `unit_mismatch`：带回该时刻折叠值与实际单位；
  - `value_divergent`：投了票但值在获胜集合之外，带回其值，`unit` 即
    `expected_unit`；
- `missing_ancestors`：该区间内全部 blocked 传感器缺失祖先的并集
  （去重排序）；无 blocked 遮蔽时为空；
- `input_hash`：对所有被查询传感器扫描输入（折叠片段 + blocked 跨度）的
  SHA-256，快照输入不变则哈希不变，与窗口、单位、`q`、阈值无关。

批次或任一请求传感器不存在返回 `404`；`q` 越界（含不足严格过半或超过
传感器数）、传感器少于 2 个或重复、窗口为空/倒置/负、`max_value_difference`
为负或非有限、单位为空、缺失必填字段或多余字段均返回 `422`。查询不创建
批次、不改变任何状态。

示例（B42 中 S1/S2/S3 在 `[0,300)` 取值 10.0/10.2/10.1 °C，S3 在
`[120,180)` 另有等待缺失祖先 `v9` 的 blocked 修订；`q=3`、阈值 0.5）：

```bash
$ curl -s -X POST localhost:8080/api/v1/batches/B42/majority \
    -H 'Content-Type: application/json' \
    -d '{"window_start_ns":0,"window_end_ns":300,
         "sensors":[{"sensor_id":"S1"},{"sensor_id":"S2"},{"sensor_id":"S3"}],
         "expected_unit":"C","q":3,"max_value_difference":0.5}'
{
  "batch_id": "B42",
  "window_start_ns": 0,
  "window_end_ns": 300,
  "expected_unit": "C",
  "q": 3,
  "max_value_difference": 0.5,
  "sensors": ["S1", "S2", "S3"],
  "status": "qualified",
  "intervals": [
    {"start_ns": 0, "end_ns": 120, "duration_ns": 120,
     "verdict": "qualified",
     "members": [
       {"sensor_id": "S1", "value": 10.0, "unit": "C"},
       {"sensor_id": "S2", "value": 10.2, "unit": "C"},
       {"sensor_id": "S3", "value": 10.1, "unit": "C"}],
     "non_voting_sensors": [],
     "missing_ancestors": []},
    {"start_ns": 120, "end_ns": 180, "duration_ns": 60,
     "verdict": "pending_backfill",
     "members": [
       {"sensor_id": "S1", "value": 10.0, "unit": "C"},
       {"sensor_id": "S2", "value": 10.2, "unit": "C"}],
     "non_voting_sensors": [
       {"sensor_id": "S3", "reason": "blocked",
        "value": null, "unit": null, "missing_ancestors": ["v9"]}],
     "missing_ancestors": ["v9"]},
    {"start_ns": 180, "end_ns": 300, "duration_ns": 120,
     "verdict": "qualified", "members": ["…同上三人…"],
     "non_voting_sensors": [],
     "missing_ancestors": []}
  ],
  "input_hash": "…"
}
```

若可投票值彼此分裂（例如 S2=30.0、S3=50.0），且没有 blocked 传感器能凑
够 `q`，区间判为 `unqualified`：`members` 只含最大相容集合（此处仅
`S1`），S2/S3 以 `value_divergent` 列入 `non_voting_sensors` 并各自带回
观测值。

### `POST /api/v1/batches/{batch_id}/seals` — 封存批次（出厂留档证据）

把**已完成**批次固定为一份不可改写的证据快照，用于出厂留档与事后复核。
请求体：

```json
{
  "batch_id": "B42",
  "seal_id": "SEAL-2026-001",
  "timeline_hash": "a7825f6b…fc9d73"
}
```

参数说明：

- `seal_id`：调用方自定义的封存号，在批次内唯一；
- `timeline_hash`：调用方先前从批次结果取得的整批哈希（64 位小写十六
  进制 SHA-256，格式非法返回 `422`）。路径 `batch_id` 必须与请求体一致。

判定（全部在同一事务（跨进程加锁）内的同一批次快照上完成；拒绝时事务
回滚，不留任何记录）：

- 该封存号已存在且请求与原始封存请求**完全相同** → `200`，
  `status="duplicate"`，返回**原快照**（即使实时批次此后已变化）；
- 该封存号已存在但参数不同 → `409`；
- 批次不存在 → `404`；
- 当前批次 `timeline_hash` 与请求不符（哈希已变化）→ `409`；
- 批次仍有 blocked/invalid 修订（`complete=false`）→ `409`；
- 以上任何拒绝都**不留下封存记录**；
- 否则冻结证据并返回 `200`、`status="sealed"`。

响应字段：

- `timeline_hash`：封存时核验的批次哈希（请求回显）；
- `evidence_digest`：覆盖**全部冻结修订 + 折叠结果**的确定性 SHA-256
  证据摘要（修订按 `(sensor_id, revision_id)` 排序，与到达顺序无关）；
- `evidence`：冻结的证据本体——`revisions`（当时全部修订的完整记录，
  含 `parent_id`/`idempotency_key`/区间/值/单位/撤回标记）与 `result`
  （封存时刻的完整批次结果，口径与 `GET /result` 一致）。

### `GET /api/v1/batches/{batch_id}/seals/{seal_id}` — 回查封存证据

返回持久化保存的封存 `evidence` 与 `evidence_digest`，并用**当前**批次状态
重算证据摘要：

- `current_evidence_digest`：按当前全部修订与折叠结果重算的证据摘要
  （批次不存在时为 `null`）；
- `consistent`：当前证据摘要是否仍与封存摘要一致。封存后任何新修订
  （即使是不改变折叠结果的 blocked 修订）都会使实时证据摘要变化、
  `consistent` 变为 `false`，但**冻结的封存内容永不被改写**。

封存号不存在返回 `404`。

示例（B42 已 complete，先用结果哈希封存，随后新修订到达再回查）：

```bash
$ HASH=$(curl -s localhost:8080/api/v1/batches/B42/result | jq -r .timeline_hash)
$ curl -s -X POST localhost:8080/api/v1/batches/B42/seals \
    -H 'Content-Type: application/json' \
    -d "{\"batch_id\":\"B42\",\"seal_id\":\"SEAL-2026-001\",\"timeline_hash\":\"$HASH\"}"
{
  "status": "sealed",
  "batch_id": "B42",
  "seal_id": "SEAL-2026-001",
  "timeline_hash": "a7825f6b…fc9d73",
  "evidence_digest": "e4f303ee…cdde1c",
  "evidence": {
    "batch_id": "B42",
    "revisions": [
      {"sensor_id": "S1", "revision_id": "v1", "parent_id": "ROOT",
       "idempotency_key": "e1", "start_ns": 0, "end_ns": 300,
       "value": 1.0, "unit": "C", "withdrawn": false},
      {"sensor_id": "S1", "revision_id": "v2", "parent_id": "v1",
       "idempotency_key": "e2", "start_ns": 100, "end_ns": 200,
       "value": 3.5, "unit": "C", "withdrawn": false}
    ],
    "result": { … "complete": true, "timeline_hash": "a7825f6b…fc9d73" }
  }
}
# 重放同一请求 -> "status":"duplicate"，返回同一份原快照
# 新修订 v3 到达后回查：
$ curl -s localhost:8080/api/v1/batches/B42/seals/SEAL-2026-001
{
  "batch_id": "B42",
  "seal_id": "SEAL-2026-001",
  "evidence_digest": "e4f303ee…cdde1c",
  "evidence": { …同上，仍只有 v1/v2… },
  "current_evidence_digest": "974f080c…",
  "consistent": false
}
```

### `POST /admin/reset` — 清空全部状态

返回被清除的批次数、事件数与封存数（`seals_removed`）；在持久化后端下
同样**删除数据库中的全部批次、幂等键映射与封存记录**（空库返回全 0，
不报错）。批次数按出现过修订或封存的去重批次计。

### `GET /health` — 存活检查

保留原有字段 `status` / `storage` / `version`；`storage` 如实标识当前
后端：持久化模式为 `"sqlite"`，内存模式为 `"memory"`。

```json
{"status": "ok", "storage": "sqlite", "version": "2.0.0"}
```

---

## 4b. 持久化存储说明（SQLite）

- 数据库包含三张表：`events`（修订，主键 `(batch_id, revision_id)`）、
  `idempotency_keys`（幂等键 → 规范负载/修订，主键
  `(batch_id, idempotency_key)`）、`seals`（封存号 → 冻结证据 JSON 与
  证据摘要，主键 `(batch_id, seal_id)`）。
- 同一修订的“插入修订 + 登记幂等键”、封存的“校验 + 冻结 + 插入封存”
  都在**单个事务**内提交，任何中途失败整体回滚，不会出现半成品状态。
- 数据库运行在 **WAL** 日志模式（`synchronous=NORMAL`）：多个进程可同时
  读取，写入在库级互斥串行；等待锁的时长由 `CAL_SQLITE_BUSY_TIMEOUT_MS`
  控制。备份时请同时复制 `.db`、`.db-wal`、`.db-shm`（或先做 SQLite
  checkpoint）。
- 数据库文件路径的父目录不存在时会自动创建；路径不可写/磁盘错误时，
  启动直接失败，运行期读写错误返回 `503`，绝不当作空批次处理。

## 5. 典型使用流程

```bash
# 0) 提交前先预览：父修订 v1 缺失 → status="submittable"，gaps_added=[v1]，
#    changed_intervals 为空（blocked 不进时间线）；不写入事件、不占用幂等键
curl -s -X POST localhost:8080/api/v1/batches/B42/events:preview \
  -H 'Content-Type: application/json' \
  -d '{"batch_id":"B42","sensor_id":"S1","revision_id":"v2","parent_id":"v1",
       "idempotency_key":"e2","start_ns":100,"end_ns":200,"value":3.5,"unit":"C"}'

# 1) 子版本先到（父 v1 缺失）→ 返回缺口 v1，时间线为空
curl -s -X POST localhost:8080/api/v1/batches/B42/events \
  -H 'Content-Type: application/json' \
  -d '{"batch_id":"B42","sensor_id":"S1","revision_id":"v2","parent_id":"v1",
       "idempotency_key":"e2","start_ns":100,"end_ns":200,"value":3.5,"unit":"C"}'

# 2) 同一请求重放（提交或预览均可）→ "status":"duplicate"，结果不变

# 3) 正式补传前再预览父版本 v1：gaps_removed=[v1]、三段 added、
#    after.complete=true；预览不改变任何状态，确认后再真正提交
curl -s -X POST localhost:8080/api/v1/batches/B42/events:preview \
  -H 'Content-Type: application/json' \
  -d '{"batch_id":"B42","sensor_id":"S1","revision_id":"v1","parent_id":"ROOT",
       "idempotency_key":"e1","start_ns":0,"end_ns":300,"value":1.0,"unit":"C"}'

# 3b) 按缺口清单补传父版本 v1（根）→ complete=true，时间线变为
#    [0,100)=1.0(v1) | [100,200)=3.5(v2) | [200,300)=1.0(v1)
curl -s -X POST localhost:8080/api/v1/batches/B42/events \
  -H 'Content-Type: application/json' \
  -d '{"batch_id":"B42","sensor_id":"S1","revision_id":"v1","parent_id":"ROOT",
       "idempotency_key":"e1","start_ns":0,"end_ns":300,"value":1.0,"unit":"C"}'

# 4) 查询 / 采样点查询 / 重置
curl -s localhost:8080/api/v1/batches/B42/result
curl -s 'localhost:8080/api/v1/batches/B42/sensors/S1/sample?timestamp_ns=150'
#   -> state="calibrated", value=3.5, revision_id="v2",
#      parent_chain=["v2","v1","ROOT"]

# 5) 质量门禁：S1 允许值闭区间 [0,3]，超界持续 ≥100ns 才计违规
curl -s -X POST localhost:8080/api/v1/batches/B42/quality-gate \
  -H 'Content-Type: application/json' \
  -d '{"sensor_id":"S1","min_value":0.0,"max_value":3.0,
       "min_duration_ns":100,"missing_policy":"ignore"}'
#   -> status="fail", violations=[{"start_ns":100,"end_ns":200,
#      "duration_ns":100,"min_value":3.5,"max_value":3.5}], gaps=[]

# 6) 设备放行：S1 与 S2 在 [0,300) 同时合格且连续达到 100ns 的区间才可放行；
#    被 blocked 修订覆盖的时段按传感器列出原因（可补传的缺失祖先）
curl -s -X POST localhost:8080/api/v1/batches/B42/release \
  -H 'Content-Type: application/json' \
  -d '{"window_start_ns":0,"window_end_ns":300,"min_duration_ns":100,
       "sensors":[
         {"sensor_id":"S1","min_value":0.0,"max_value":4.0,"expected_unit":"C"},
         {"sensor_id":"S2","min_value":1.0,"max_value":2.0,"expected_unit":"bar"}]}'
#   -> status="released"，released_intervals 为达门槛的连续同时合格段，
#      其余区间（blocked/withdrawn/uncovered/unit_mismatch/out_of_bounds
#      或时长不足的合格段）在 unreleased_intervals 中按传感器给出原因

# 7) 可信多数投票：3 个冗余传感器严格过半门槛 q=2（此处 q=3 要求全票），
#    相容票值极差 <= 0.5；blocked 遮蔽时段判 pending_backfill 并列缺失祖先
curl -s -X POST localhost:8080/api/v1/batches/B42/majority \
  -H 'Content-Type: application/json' \
  -d '{"window_start_ns":0,"window_end_ns":300,
       "sensors":[{"sensor_id":"S1"},{"sensor_id":"S2"},{"sensor_id":"S3"}],
       "expected_unit":"C","q":2,"max_value_difference":0.5}'
#   -> status=qualified/pending_backfill/unqualified；intervals 恰好覆盖
#      [0,300)，每段给 verdict、members、non_voting_sensors、missing_ancestors

# 8) 出厂留档：批次 complete=true 后用当前 timeline_hash 封存；
#    重放同一请求返回原快照，后续修订不改写封存，回查可核对一致性
HASH=$(curl -s localhost:8080/api/v1/batches/B42/result | jq -r .timeline_hash)
curl -s -X POST localhost:8080/api/v1/batches/B42/seals \
  -H 'Content-Type: application/json' \
  -d "{\"batch_id\":\"B42\",\"seal_id\":\"SEAL-2026-001\",\"timeline_hash\":\"$HASH\"}"
#   -> status="sealed"，evidence 冻结当时全部修订与折叠结果，
#      evidence_digest 为覆盖两者的确定性 SHA-256 摘要
curl -s localhost:8080/api/v1/batches/B42/seals/SEAL-2026-001
#   -> 返回冻结证据与摘要；consistent 表示实时批次证据是否仍与封存一致

# 9) 重启后（或请求落到另一个 worker 进程）数据仍在：
#    原请求重放仍是 duplicate 并返回同一份证据；
#    同封存号换参数 -> 409；回查比较持久封存与当前批次。
curl -s -X POST localhost:8080/api/v1/batches/B42/seals \
  -H 'Content-Type: application/json' \
  -d "{\"batch_id\":\"B42\",\"seal_id\":\"SEAL-2026-001\",\"timeline_hash\":\"$HASH\"}"
#   -> {"status":"duplicate", "evidence_digest":"e4f303ee…", "evidence":{ …原快照… }}

# 10) 清空（持久化数据也一并删除）；返回准确计数
curl -s -X POST localhost:8080/admin/reset
#   -> {"status":"reset","batches_removed":N,"events_removed":M,"seals_removed":K}
```

### 多进程 / 重启持久化示例（默认 SQLite 后端）

```bash
# 单进程写入一个批次并封存
CAL_SQLITE_PATH=./data/demo.db uvicorn app.main:app --port 8080 &
curl -s -X POST localhost:8080/api/v1/batches/B1/events -H 'Content-Type: application/json' \
  -d '{"batch_id":"B1","sensor_id":"S1","revision_id":"v1","parent_id":"ROOT",
       "idempotency_key":"k1","start_ns":0,"end_ns":100,"value":1.0,"unit":"C"}'
kill %1

# 重启（任意个数进程）指向同一个数据库文件，状态完整可见
CAL_SQLITE_PATH=./data/demo.db uvicorn app.main:app --port 8080 --workers 2 &
curl -s localhost:8080/api/v1/batches/B1/result | jq .revisions_total   # -> 1
# 同一请求在任一 worker 上重放都是 duplicate；新键重放同修订同内容登记新键
# /health 的 storage 字段为 "sqlite"
curl -s localhost:8080/health
#   -> {"status":"ok","storage":"sqlite","version":"2.0.0"}
```

撤回某一段（墓碑，区间内被擦除）：

```json
{
  "batch_id": "B42", "sensor_id": "S1", "revision_id": "w1",
  "parent_id": "v2", "idempotency_key": "w1",
  "start_ns": 120, "end_ns": 180, "withdrawn": true
}
```

---

## 6. 项目结构

```
.
├── app/
│   ├── main.py       # FastAPI 路由：ingest / preview / result / sample / compare / quality-gate / release / majority / seals / reset / health（每个判定+求值包在单个 store 事务内）
│   ├── engine.py     # 因果分类、区间切分折叠、预览影响计算、采样点查询、跨批次差异对比、质量门禁扫描、设备放行扫描、冗余传感器可信多数投票、批次封存证据与摘要、缺口与哈希（纯函数引擎）
│   ├── storage.py    # 两种同构后端：线程安全的内存 EventStore 与持久化/多进程共享的 SqliteEventStore（WAL + BEGIN IMMEDIATE 跨进程事务），幂等判定、无副作用预览判定、不可改写封存记录与存储故障 StorageError
│   ├── models.py     # Pydantic 请求/响应模型与校验
│   └── config.py     # CAL_ 前缀环境变量配置（CAL_STORAGE / CAL_SQLITE_PATH / CAL_SQLITE_BUSY_TIMEOUT_MS）
├── tests/            # pytest（API 端到端 + 引擎单测 + SQLite 持久化/重启/跨进程/存储故障，共 207 个用例）
├── Dockerfile
├── docker-compose.yml  # 双 worker + 命名卷 calibration-data 共享 SQLite 数据库
├── requirements.txt
├── requirements-dev.txt
└── .env.example
```
