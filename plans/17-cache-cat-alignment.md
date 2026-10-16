# 17 性能对齐 cache-cat：三层改造（读路径 / 写管线 / 协议层）

## 目标

按 plans/16 步骤 7 的三方对测归因，向 cache-cat 的实现对齐三层瓶颈，
目标是在同机对测中达到其性能量级。全程保持核心最小、不留 legacy：
被替代的路径直接删除，不保留兼容开关。

## 层次与状态

### 第 1 层：读路径（已交付，2026-10-15）

cache-cat 的 GET 走 `lease_read()` = openraft `get_read_linearizer(LeaseRead)`
（租约有效即本地应答，零日志、零多数派往返）。我们原实现每条读走
`ensure_linearizable(ReadIndex)`（blank 追加 + 多数派确认，串行 ~30ms/读，
实测读上限 ~30 reads/s 与并发无关——混合负载的决定性短板）。

交付内容（两项叠加，缺一不可）：

1. **LeaseRead 策略 + 屏障前置**：读请求先建立租约屏障再读状态机；
   follower 经 ForwardToLeader 重定向（不变）；PING/ECHO/INFO 仍在
   服务器层本地应答（不进 raft）。租约语义与护栏：
   - 租约窗口 = `election_timeout_max`（300ms），follower 心跳确认续期；
   - 分区时租约窗口内只允许应答分区前已提交状态，到期必须 fail-closed
     （护栏测试 `tcp_minority_partition_separates_read_write_and_redirect_errors`
     已改为断言该契约：窗口内只允许 pre-partition 值，到期后必须报错）；
   - 时钟前提（漂移可忽略）写入 docs/architecture.md 读取路径一节。
2. **单条路径直备（去全状态 clone）**：`execute_client_request_with_id`
   原来每次调用 `state().await` 深拷贝整个状态机（40 万 key 实测单次
   ~90-130ms，且在互斥锁内串行）——预载数据后读吞吐塌到两位数即此所致。
   新增 `prepare_request_live` / `prepare_transaction_live` /
   `prepare_time_advance_live`（直备契约与合批路径一致：prepare 纯校验
   + Operation 生成，唯一前置变更是单调逻辑时钟 max）。事务路径同样
   去掉外层 clone（事务内部工作副本语义保留）。删除无调用方的
   `execute_raft_request` 与 `prepare_request_at`。
   附带发现并修复：leader 时间 worker 原每 5ms 全状态 clone 一次
   （plans/13 中"未知 ~13ms/轮"的来源）。

实测（3 节点，512c×p16，40 万 key，value 16B，全部零错误完整完成）：

| 负载 | 改造前（同日口径） | 改造后 | cache-cat 参照（同表） |
| --- | ---: | ---: | ---: |
| 50/50 | 崩溃（30s 超时）/ 55 | **19,654** | 153,068 |
| 95/5 | 30 | **82,551** | 197,387 |
| 5/95 | 500 | **13,373** | 152,440 |
| 纯写参考 | 16,814 | **18,578** | - |

读路径单点：三节点 GET p1 = 32,178 ops/s（p50 66µs）；单节点 40 万 key
深并发读 = 340,859 ops/s（p50 785µs）。改造前同点分别为 3-35 ops/s 与
18 ops/s。

### 第 1 层（续）：读并发化（已交付，2026-10-15）

- 状态机锁 `Mutex` → `RwLock`（cache-cat 同款读并发）：读直备走读守卫
  并发执行；写路径（含逻辑时钟推进）、apply、快照转换走写守卫。
  日志存储的锁不动（独立小临界区）。
- 读请求不再推进逻辑时钟（写路径与时间 worker 已维护新鲜度），过期判定
  依赖已应用状态的 logical time（时间 worker 5ms 节拍，与批量读路径一致）。

### 第 3 层：响应批量写出（已交付，2026-10-15）

- pipeline 段内响应编码进单个 `BytesMut`，段末一次写出（每帧以其产生
  时的 RESP 版本编码，HELLO 等版本切换语义不变）；32MiB 软上限防止
  巨型响应段无界缓冲。每段 128 响应从 128 次 syscall 降为 1 次。

实测（3 节点，512c×p16，40 万 key，全部零错误完整完成；机器 load 7+，
写侧数字波动 ±40%，读侧为保守对照）：

| 负载 | 层 1 交付时 | 层 1b+3 后 | cache-cat 参照 |
| --- | ---: | ---: | ---: |
| GET-only（纯读） | - | **167,846** | （95/5 口径 197,387） |
| 95/5 | 82,551 | 79,171（同噪声带） | 197,387 |
| 50/50 | 19,654 | 12,373-19,654（写侧噪声带） | 153,068 |
| 5/95 | 13,373 | 11,853 | 152,440 |
| 纯写 | 18,578 | 13,032-18,578（噪声带） | - |

结论：读路径量级已对齐（纯读 168k vs cache-cat 同表 197k，差 ~15%）；
50/50 与 5/95 的剩余差距全部归因写侧（第 2 层）。95/5 混合档读侧不再是
瓶颈（写 5% 的提交轮主导波动）。

### 第 2 层：增量快照（已交付，2026-10-15）

**格式**（向后兼容，旧快照文件照常加载）：

- base 文件（ARSS v1）不变；新增 delta 段文件 `snapshot.deltas`（ARSD v1
  帧：last_log_id + membership + operations，追加写 + fsync）。
- build 路由：有 base 且未达紧缩阈值 → **增量构建**（取 pending_deltas
  追加一条 delta 记录 + fsync 后才推进覆盖点）；首次/紧缩到期 → 全量
  重建（原路径，同时清理 delta 文件）。紧缩阈值：64MiB 或 512 条。
- 快照 blob = **ARSE 信封**：验证过的 base 载荷字节 + 原始 delta 帧的
  纯拼接 + 整体校验和——增量构建零状态编解码（初版误用 StoredSnapshot
  包装字节导致 install 失败无限重发，已修复并规范化 install 落盘为干净
  全量载荷）。`decode_snapshot_blob` 同时接受信封与旧格式。
- apply 持续把操作累积进 pending_deltas（全量构建期间走 snapshot_queue
  原机制）；恢复 = openraft 启动 install（信封解码合并）+ 短日志回放；
  follower 追赶/learner 加入走同一信封传输路径。
- 状态机锁为 RwLock：build 增量段与读直备互不阻塞。

实测（单节点 40 万 key 热态，policy 4096）：

| 方案 | 稳态写吞吐 | 恢复时间（~2.3M 条日志） |
| --- | ---: | ---: |
| 全量快照（改造前） | 14,456 | 秒级（快照小） |
| 无快照（对照） | 24,455-48,235 | **>16 分钟，不可用** |
| **增量快照** | **34,370-45,223** | **1.7s**（base+delta 合并 + 短回放） |

三节点（512c×p16，40 万 key）：50/50 = **44,345**（全量快照时代
12,373-19,654）；纯写 = **33,368**（13,032-18,578）。零错误。

新增测试：`file_raft_incremental_snapshots_survive_restart`（base+delta
重启恢复，早期 key 仅可经快照恢复）、
`incremental_snapshots_compact_and_keep_state`（515 次构建触发紧缩、
覆盖计数重置、状态不丢）、`delta_records_scan_stops_at_torn_tail`
（撕裂尾帧安全截停）。全部 171 项测试通过，clippy 零告警。

第 2 层剩余（记录，非本轮）：恢复回放逐条 apply ~250µs/条的批量优化；
committer 固定窗口与 openraft 逐条 apply 的进一步压榨；读侧
GetLinearizer 单核 oneshot 的最后 ~15% 差距归因。

**步骤 0 归因实验（此前未做，本轮补齐）**——单节点、40 万 key 热态、
纯写 32c×512，`sample` 采样 leader：

| 配置 | 稳态吞吐 | CPU 归因 |
| --- | ---: | --- |
| policy 4096（有快照） | 14,456 | **~95% 在快照流水线**（bincode 序列化 258 + 持久化 88 + sync_all 65 vs raft 写路径 ~10） |
| policy 1e9（真·无快照，修复后） | 首段 48,235 → 平台 24,455（日志 2M 条） | apply 41 + sync_all 21（快照税消除，剩逐条 apply + WAL fsync） |

**附带发现并修复两个快照 policy bug**：

1. **单节点路径完全忽略 --snapshot-policy**：`new_file_raft_with_metrics`
   硬编码 `..Default::default()`（openraft 默认 LogsSinceLast(10000)），
   CLI 参数只在集群路径生效。实测单节点 policy 1e9 下 33 万条就打了
   32MB 全量快照。已修复：显式参数贯通 `new_file_raft_with_metrics` →
   `open_single_node(_with_metrics)` → server；测试调用点传 10_000
   保持原隐式行为。**此前所有单节点"无快照/快照衰减"实验的归因均被
   此 bug 污染**（衰减实为默认 10000 的全量快照压制，非日志增长）。
2. 日志存储/集群路径的 policy 贯通复核无误。

**两个极端都不可生存（最终归因）**：

- 有快照（4096）：95% CPU 在全量序列化，稳态腰斩；
- 无快照：写侧 48k→25k 尚可，但**恢复 = 全量回放**——613MB/
  ~400 万条日志重启恢复 >16 分钟仍未完成（apply ~250µs/条），不可接受。

结论：**系统必须保留周期性快照以约束恢复时间，因此唯一出路是让快照
本身变便宜 = 增量快照**（base 持久化一次 + 追加 delta 段 + 紧缩），
plans/06 遗留项正式升级为第 2 层前置。恢复回放速度（apply 逐条
~250µs）是第二个待优化项（批量回放/免 Frame 构建）。

**过渡期调参依据（增量快照交付前）**：快照成本随状态大小线性增长，
policy 应随工作集规模上调（4096 默认是为 10 万级 key 设计的）；具体
默认值待增量快照方案落地时一并修订，避免两头不讨好的临时值。
（增量快照已交付，见上文；默认 policy 4096 现在代价可控，维持不变。）

### 第 2 层（续）：归因实验留档（2026-10-15）

**步骤 0 归因实验（此前未做，本轮补齐）**——单节点、40 万 key 热态、
纯写 32c×512，`sample` 采样 leader：

| 配置 | 稳态吞吐 | CPU 归因 |
| --- | ---: | --- |
| policy 4096（改造前全量快照） | 14,456 | ~95% 快照流水线 |
| policy 1e9（真·无快照） | 24,455-48,235 | apply + WAL fsync |
| policy 4096（增量快照，交付后） | 34,370-45,223 | 快照税消除 |

### 迭代循环（2026-10-16 起）：每轮 = 双集群同机对测 → 归因 → 修复 → 复测

**迭代 1**：对测发现 50/50 仅 4.8-12.2k（cache-cat 102,323）。归因链：
单轮提交延迟 p1 = 96.8ms（三节点）/24.7ms（单节点），而裸 nc 单次 SET
只花 10-70ms（含进程启动）→ 服务端往返本就正常，等待在 TCP 栈。
**根因：服务端 accept 的连接从未设 TCP_NODELAY**——每个小 RESP 响应都
被 Nagle + 延迟 ACK 拖 40-90ms，自项目伊始就在暗税所有小操作与全部
benchmark 数字（redis-benchmark 口径同样受害）。修复：客户端监听与
raft RPC 监听两处 accept 后 set_nodelay(true)。
附带：WAL fsync 实测 2.2ms/次（max 15.8ms，INFO raft_log_sync_data_*
指标，此前 0.05ms 探针是新鲜小文件的假象）；committed sidecar 每次
提交 rewrite+rename ~1.3ms → 去抖为 200ms 一次（sidecar 是恢复提示
而非正确性记录，日志回放兜底）。

迭代 1 结果（三节点 512c×p16，40 万 key，零错误）：

| 档位 | 迭代前 | 迭代 1 后 | cache-cat | 剩余差距归因 |
| --- | ---: | ---: | ---: | --- |
| 95/5 | 79-82k | **96,886** | 197,387 | 读 prepare 单核串行 + apply |
| 50/50 | 12-25k | **25,673** | 102,323 | 写侧（同 5/95） |
| 5/95 | 13.5k | **13,563** | 152,440 | apply 逐条 ~40-100µs：1024 条/轮 × 每轮 40-100ms = 吞吐上限；BTreeMap×2（values+versions）+ 内存计费 + Frame 构造 |
| SET p1 延迟 | 96.8ms | 30.4ms | - | 每跳 ~10ms（fsync 2.2ms + core 开销） |

**迭代 2**：apply 逐条瘦身第一步——WATCH 版本表 `versions` 从
BTreeMap 换 HashMap（apply 每条写少一次 40 万节点 BTreeMap 行走；WATCH
只按 key 点查，无序遍历需求，快照相等性为语义比较不受布局影响）。

迭代 2 结果（三节点 512c×p16，40 万 key，零错误）：

| 档位 | 迭代 1 后 | 迭代 2 后 | cache-cat | 达成率 |
| --- | ---: | ---: | ---: | ---: |
| 95/5 | 96,886 | **191,488** | 197,387 | **97%，实质持平** |
| 50/50 | 25,673 | **39,189** | 102,323 | 38% |
| 5/95 | 13,563 | **29,599** | 152,440 | 19% |

**迭代 3**（2026-10-16）：

1. apply 微基准落地（`apply_throughput_microbenchmark_400k_hot_state`）：
   40 万 key 热态逐条 apply = **4.9µs/条（202,633 ops/s）**——状态机
   不是瓶颈，apply 瘦身假设证伪（数据驱动否决）。
2. 快照开销再压：envelope 直接嵌入 base 文件原始载荷字节（免每次构建
   的全量反序列化提取）；默认 snapshot policy 4096 → 65536（delta 便宜
   后构建频率才是成本）。采样确认 build_snapshot 从 ~36% 降到 ~1%。
3. 5/95 = **43,682**（+48%）；深管线（512c×p128，65k 在途）无进一步
   提升 → **存在 ~40k/s 的结构性串行点，与在途深度无关**。
4. 精确符号采样：replication + append_entries 机制占最大 CPU 份额——
   上限在我们喂给 openraft 的方式（coalescer 1024 条分块轮次）vs
   cache-cat 的单流无界 client_write_many。

**迭代 4 方向**：管线形状对齐 cache-cat——实验去掉/放大
CLIENT_BATCH_MAX_ITEMS 分块、coalescer 窗口消除、单流
client_write_many 直通；对照 raft-direct 口径 142k 验证共识层上限；
follower 侧（复制接收）单独采样。

**迭代 2 方向（按预期收益）**：
1. apply 逐条瘦身：client_results 去重表是否有界/换 HashMap；versions
   BTreeMap → HashMap（WATCH 才需要）；内存计费按批聚合。目标是把
   1024 条/轮的 apply 从 40-100ms 压到 ~10ms（对齐 cache-cat 的
   HashMap 直写模型）。
2. 5/95 与 50/50 打平后，95/5 的读 prepare 串行（单核 oneshot）。
3. SET p1 剩余 30ms：每跳 ~10ms 的 core/fsync 开销归因。

## 完成标准

- 同机三方对测（plans/16 步骤 7 口径）混合负载进入 cache-cat 同量级
  （≥50%）且全部零错误完整完成；
- 180+ 项功能测试全绿；线性一致/分区/护栏测试不回退；
- 架构文档记录租约读全部前提（时钟、续期、分区、Leader 变更）；
- 每步独立提交，实测数字回写本文件。
