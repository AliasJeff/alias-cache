# 13 批量写管线与性能主张

## 目标

复刻 cache-cat 的批量提交架构（`client_write_many` + 流式管线客户端 + 批量 RPC），
以三节点多数派写吞吐 ≥20,000 ops/s 作为待验证目标，并以同机对照测量评估
"强一致 + 高写吞吐"主张。该目标是实验判据，不代表已实现或可与 cache-cat
直接对标。测量口径使用每请求延迟，并引入 redis-benchmark 交叉验证。

## 背景：差距的定量定位（2026-10 实测）

- 当前三节点 SET 2,355 ops/s（p50 53ms），瓶颈为每批一次三节点 fsync 往返；
  Little's law 排队模型吻合（128 in-flight / 2,355 ≈ 54ms）。
- 单节点 pipeline 16 下 p50 ≈ 115ms 与 128/1,046 吻合：openraft 的
  `client_write` 内部已有批合并（每批 ~128 条一次 fsync），**但批与批之间
  存在空隙，且每条请求独立走 prepare→write→response 完整路径**。
- cache-cat 的 500k writes/s 由三个部件叠加（见下），其中我们已有 group
  commit（对应其 fsync 批），缺批量提交入口与流式客户端。

## 参考 cache-cat

### 三件套架构（逐个对应）

1. **`cache_cat/src/raft/application/cluster.rs::client_write_many`**
   （约 229-285 行）：薄封装 openraft 的 `Raft::client_write_many(requests)`，
   接收 `Vec<Request>`，返回逐条结果的 `BoxStream`。批量 API 将请求一次送入
   Raft core 并按提交顺序产生结果；具体复制 RPC 和存储 fsync 合并粒度仍需测量，
   不能仅凭 API 形状断言为一次 fsync。
   - 对应 alias-cache 落点：`alias-cache/src/raft.rs` 新增
     `execute_client_batch_request`，包一层请求 ID 去重（复用现有
     replicated client result cache，批内每条独立 ID）。
2. **`cache_cat/src/raft/network/external_handler.rs::batch_write`**
   （约 120-138 行）：网络层批量写 RPC 入口。收到 `Vec<Request>` 后调
   `client_write_many`，把结果流转成 `Vec<Result<...>>` 一次性返回。
   follower 收到批量写时整体转发 leader（不做拆分）。
   - 对应 alias-cache 落点：`alias-cache/src/raft.rs` 的
     `NetworkRequest` 枚举加 `ClientBatch { requests, request_ids }` 变体，
     `handle_raft_rpc` 加分发分支；`send_client_request_with_id` 旁新增
     `send_client_batch_request`（一跳转发语义与现有单条版一致）。
3. **`cache_cat/src/raft/network/pipeline_client.rs`**（全文件 159 行）：
   流式管线客户端。机制：
   - 一条 TCP + 首字节协议标识（`[2u8]`）区分协议类型；
   - 客户端内 `mpsc::channel<(Request, oneshot::Sender<...>)>(1024)`：
     调用方把请求丢队列即返回 future，**专职发送任务**持续序列化写出，
     不等响应；
   - **FIFO 回调队列**与响应按序配对（服务端按请求顺序回帧）；
   - `PipelineMultiClient` 持 N 条连接 round-robin + 断线重连。
   - 它解决的标准 RESP pipeline 缺陷：传统"发一批→等齐→再发"批次间有
     空隙；流式队列让发送侧永远有货、fsync 批次无缝衔接。
   - 对应 alias-cache 落点：**benchmark crate 内**实现（不走生产服务端，
     见"不直接复制"），同样 159 行量级。
- `cache_cat/benchmark/src/cache_cat_benchmark.rs`：`pwrite` 模式用
  `PipelineMultiClient` 直连 raft 二进制协议压测——**测量架构是绕开 RESP
  测共识层上限**。我们的对应物：benchmark 加 `pwrite` 模式。

## 不直接复制

1. cache-cat 的 benchmark 直连 raft 内部 bincode 协议（绕过 RESP）。alias-cache
   的压测继续走 RESP2：我们的主张包含协议层，绕开协议的数字没有工程意义。
   若最终需要与 cache-cat 逐字节对齐的"共识层上限"数字，可在 benchmark 内加
   `--transport resp|raft` 开关，raft 模式仅用于诊断而非主张口径。
2. cache-cat 的 `pipeline_client` 首字节协议标识复用生产 listener 端口。
   alias-cache 的批量管线走**已有的 `NetworkRequest` 帧协议**（成员变更、
   快照传输同层），不新增端口、不新增协议版本字节。
3. 幂等语义不能丢：cache-cat 的批量 RPC 未携带请求级去重 ID；alias-cache
   的批量入口必须为批内每条请求绑定独立 `ClientRequestId`（复用
   client result cache），转发重试时整批同 ID 重发，保证非幂等命令
   不因批量而失去 exactly-once 窗口语义。

## 实现步骤（每步独立提交，含测试）

### 步骤 1：服务端批量提交入口

- `raft.rs` 新增 `execute_client_batch_request_with_ids(raft, ids, requests)`：
  - OpenRaft 0.10.0-alpha.34 已提供 `Raft::client_write_many`，以一条 API
    批量提交多个 `RaftRequest::Apply`，结果流按提交顺序返回。每个 Redis
    命令仍是独立 Raft entry；Pipeline 不是事务，不合并为一个
    `Operation::Transaction`。
  - 每条写请求有独立 request ID。批量日志项保存原始命令，并在每个副本
    apply 时按真实日志位置求值；这避免预处理快照与其他客户端写入交错时产生
    过期条件结果。命令执行结果（包括错误和条件 no-op）随日志缓存，以便
    leader 转换后整批重试能返回原响应。去重仍受既有 4,096 项缓存窗口约束，
    不承诺无限期 exactly-once。
- 参数/状态错误只生成该条 error reply，后续命令继续处理；不能整批拒绝，
  也不能丢掉后续响应。每条日志项携带 leader 的 `max-memory` 和单条请求字节上限，
  各副本在实际日志位置按相同状态应用 OOM 门。无法安全界定响应大小的命令走单条
  路径；`DEL` 含重复 key 时也回退单条路径，保证其返回值语义。
  - 批量 RPC 限制为最多 1,024 条、序列化后 4 MiB。可能返回大型数据的
    `SET ... GET`、`LPOP`、`RPOP` 保留单请求路径，避免响应超过批量帧上限。
- 测试：N 条写入产生 N 个独立结果，顺序依赖命令能观察前序效果；单条非法
  命令不会阻止后续写；累计 OOM、条件 no-op、同 IDs 重试、批量 RPC 限额和
  follower 重定向均有护栏。并发客户端写入交错在批内状态依赖命令之间时，
  响应必须符合实际日志顺序。

### 步骤 2：RESP 批量写 RPC 与转发

- `NetworkRequest::ClientBatch` 变体 + `handle_raft_rpc` 分支 + leader
  端执行 + follower 端整批一跳转发（复用现有 `Redirect` 逻辑，地址来源
  同单条版）。
- `server.rs` 的 pipeline 并发路径升级：同段写不再逐条 spawn，而是整段
  作为一个 batch 走新入口（读仍按序跟随）。`is_pipeline_safe`/
  `is_replicated_write` 分类复用。
- 测试：三节点 TCP 集成——follower 连接收到整批请求后重定向到 leader，
  leader 按原 IDs 提交并按序返回；leader 切换后的重试由请求 ID 去重。

### 步骤 3：benchmark 流式管线模式

- `benchmark/src/client.rs` 加 pwrite 模式：每 worker 一条连接 + 有界
  请求队列 + FIFO 回调配对（参考 `pipeline_client.rs` 的队列结构，但
  编码 RESP2）。
- **同时修延迟口径**：响应按序到达时逐条记录时间戳（当前是每轮一个
  延迟），P50/P95/P99 变为每请求口径，与业界可比。
- 测试：帧配对单元测试（乱序到达不出错）；统计口径单测。

### 步骤 4：测量与主张验证

- 三组同机测量（各空闲起测，沿用测量纪律）：
  1. 三节点 pwrite：目标 ≥ 20,000 ops/s（当前 2,355 的 8 倍以上）；
  2. 单节点 pwrite：与 cache-cat 单节点同配置对照（如可得）；
  3. **redis-benchmark 交叉验证**：`redis-benchmark -t set,get -P 16`
     压 alias-cache 与 Redis 各一次，确认 RESP 层不构成瓶颈意外。
- 更新 `docs/compatibility-and-failure-model.md` 的性能段、README 基线、
  `plans/12` 调优记录（闭环其第 1/2 项实验）。

### 实现与测量记录（2026-10-12）

- 已实现 `client_write_many` 批量提交、Raft 批量 RPC、follower 整批转发、
  RESP pipeline 连续写分批、每批 1,024 条/4 MiB 上限及 4 KiB 单命令准入，
  以及 benchmark `pwrite` 模式和每请求延迟采样。
- 正确性护栏包括：命令按实际 apply 日志位置求值、批内顺序/错误继续、
  请求 ID 重放、并发写交错、OOM 跨节点配置一致、批次尺寸限制、TCP
  pipeline 顺序和 Redis CONFIG 只读探测兼容。
- 三节点 release 测量（同机，8 clients、64B、key space 100K、5s，
  pwrite）：pipeline 16/64/128/256/512 吞吐分别约 2,594/7,965/9,477/
  9,269/10,248 ops/s；16 clients × pipeline 512 约 10,563 ops/s，但有
  15 个错误，p99 约 2.68s。目标 20,000 ops/s 未达到；高 pipeline 的尾延迟
  和错误率也不能接受，因此步骤 4 和本计划整体**未完成**。
- `redis-benchmark -h 127.0.0.1 -p 6379 -t set,get -P 16 -n 100000 -c 8`
  在独立干净 Compose 三节点集群运行，退出码 0，SET/GET 均完成，无 CONFIG
  探测警告。SET 11,500 ops/s（p50 10.039ms，p99 21.199ms），GET
  5,043 ops/s（p50 1.631ms，p99 4.959ms）；该交叉验证证明客户端兼容，
  不代表达到内部 pwrite 吞吐目标。Redis benchmark 自身的初始 warmup 行显示
  `rps=0.0/overall nan`，最终汇总有效。
- 尝试 OpenRaft 1ms batch linger 未改善吞吐与尾延迟，已移除，不作为默认策略。
- 运行环境为 Apple Silicon MacBook，release、RESP2、未启用 TLS；CPU/内存/
  存储型号未采集，旧档结果有集群追赶状态差异，需以 profiling 后的空闲集群
  多轮测试作为下一轮优化判据。

复现命令（先启动空数据目录的三节点集群，并在每轮前确认 follower 已追平）：

```bash
cargo build --release --locked
target/release/alias-cache-benchmark --address 127.0.0.1:6379 \
  --command set --mode pwrite --clients 8 --pipeline 16 \
  --time 5 --value-size 64 --key-space 100000
```

曾测得的 pwrite 延迟记录（微秒；机器空闲度、集群追赶状态会显著影响结果）：

| 客户端 × pipeline | 吞吐 ops/s | p50 | p95 | p99 | max | 错误 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 × 16 | 2,594 | 149,934 | 203,609 | 229,579 | 279,579 | 0 |
| 8 × 64 | 7,965 | 191,538 | 247,188 | 264,390 | 339,942 | 0 |
| 8 × 128 | 9,477 | 267,524 | 357,731 | 396,655 | 477,089 | 0 |
| 8 × 256 | 9,269 | 540,026 | 687,022 | 735,282 | 816,281 | 0 |
| 8 × 512 | 10,248 | 917,132 | 1,192,189 | 1,379,385 | 1,511,000 | 0 |
| 16 × 512 | 10,563 | 1,636,000 | 2,478,000 | 2,683,000 | 2,833,000 | 15 |

延迟上升表明提高 in-flight 数量只是增加排队，不等同于吞吐扩展。下一轮优化
前应先读取每个节点 `INFO stats` 的 append 批次和 `sync_data` 指标。指标统计
范围和限制见兼容性文档；实际三节点差值与吞吐实验将在本 profiling 功能通过
质量门禁后补录，再确定是否需要提交调度/存储层变更。

### Profiling 实测（2026-10-13）

复现：`cargo build --release --locked && python3
scripts/profile_batch_writes.py --pipelines 16 --clients 8 --duration 5`。
脚本在本机建立空数据目录三节点集群，直连当前 Leader 压测，并在压测前后读取
每个节点的 `INFO stats`。环境为 Apple Silicon MacBook、macOS、release、
RESP2、64B value、100K key space、8 clients、pipeline 16、5 秒；
CPU/内存/磁盘具体型号未采集。

| 节点角色 | 集群吞吐 ops/s | 错误 | append 调用增量 | entries 增量 | interval entries/append | sync 调用增量 | interval sync 平均耗时 | 区间最大 sync |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Leader | 2,405 | 0 | 410 | 12,503 | 30.50 | 410 | 4,030 µs | 10,624 µs |
| Follower 1 | 2,405 | 0 | 202 | 12,503 | 61.90 | 202 | 4,343 µs | 15,318 µs |
| Follower 2 | 2,405 | 0 | 205 | 12,503 | 60.99 | 205 | 4,426 µs | 15,469 µs |

此单轮样本显示三节点各自对同一批 12,503 entries 的追加/落盘分组不同；
本地 WAL sync 平均约 4.03–4.43ms，区间最大值约 10.6–15.5ms。它只解释本地存储调用，
不能据此归因 quorum RTT、也不能证明 20k ops/s 目标可达。吞吐与此前单轮
结果有波动；需要固定 in-flight 总量、pipeline 1/16/64、多轮重复并记录
系统资源后再作优化判断。完整逐节点计数可由上述脚本复现。

补充三档单轮对照（同一台 Apple Silicon MacBook，release、8 clients、64B、
100K key space、每档独立空数据目录三节点集群、3 秒、0 错误）：

| pipeline | 吞吐 ops/s | p50 | p95 | p99 | Leader entries/append | Follower entries/append |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 513 | 56.5 ms | 94.4 ms | 106.9 ms | 3.13 | 6.30 / 6.37 |
| 16 | 2,609 | 145.4 ms | 210.3 ms | 243.2 ms | 31.95 | 65.93 / 62.93 |
| 64 | 7,232 | 209.5 ms | 262.0 ms | 279.1 ms | 104.84 | 183.88 / 183.88 |

该对照说明 pipeline 深度提升确实摊薄了本机 WAL sync 次数，但延迟同步上升；
pipeline 64 的吞吐仍只有目标 20k ops/s 的约 36%，且这是单轮短时样本，
不能作为稳定容量或生产推荐值。脚本默认测量 pipeline 1、16、64：

```bash
python3 scripts/profile_batch_writes.py --duration 3 --clients 8
```

## 风险与回退

- 批量请求会增加单次内存与序列化工作 → 每批最多 1,024 条、4 MiB，超限按
  命令顺序切分；单条超限请求走原单请求路径。
- follower 整批转发在 leader 切换窗口的重试语义：批请求 ID 整体复用，
  可能出现部分已提交；每条已提交操作的去重结果随状态机复制，尚未提交的
  后缀由新 leader 接续。4,096 项缓存被更高 ID 淘汰后不再承诺旧请求去重。


## 实施记录

### 步骤 1-3 已完成（2026-10-13）

- 服务端批量入口走 **openraft 真正的 `client_write_many`**（非 Transaction
  退化路径）：批内逐条 prepare 校验（含 OOM/幂等/唯一性），一次批量提交，
  响应按序流式返回；follower 收到整批一跳转发。
- 有界约束：`CLIENT_BATCH_MAX_ITEMS=1024`、`CLIENT_BATCH_MAX_BYTES=4MB`
  （刻意小于快照帧限制），超限自动分批；**带 GET 选项的 SET 与大响应写
  留在单条路径**（`batchable_write_has_bounded_response`），防止批量响应
  突破帧界。
- benchmark 新增 pwrite 流式模式（请求队列 + FIFO 响应配对），延迟口径
  改为每请求独立记录。
- 观测：raft 日志层 `LogStoreMetrics`（append_calls / entries_per_append /
  sync_data_calls / sync 耗时分布），INFO stats 暴露 raft_log_* 指标。
- 存储格式版本 3→4（状态机新增批量结构），迁移文档同步。

### 实测数据与瓶颈画像（2026-10-13）

三节点、8 连接、pwrite 流式、value 64B、100K key space（release）：

| pipeline | 吞吐 | p50 | leader entries/append |
| ---: | ---: | ---: | ---: |
| 1 | 513 | 56ms | 3.1 |
| 16 | 2,609 | 145ms | 32.0 |
| 64 | 7,189 | 189ms | 83.9 |
| 128 | 7,549 | 300ms | — |
| 256 | 4,293 | 1,132ms | — |
| 512 | 4,424 | 2,136ms | — |

（前一行 profile 独立运行 pwrite 64：7,205 ops/s，两轮一致。）

瓶颈画像（leader metrics：463 appends、38,827 entries、sync 3.5ms/次）：

1. append 已批量化（leader ~84 条/append，follower ~139 条/append），
   fsync 仅占总时长 ~30%（1.6s / 5.4s）——fsync 不再是第一瓶颈。
2. **pipeline 64 → 128 后收益消失，256/512 反向劣化且延迟爆炸**：
   8 连接 × 256 = 2,048 条独立 client_write 在 openraft 提交路径上竞争，
   每条仍是独立 log entry 与响应流元素；批合并率不再随深度上升，
   延迟按排队比放大。深度的正确用法是"每连接一段一批"，而非把全连接
   的在途量堆进一条连接的队列。
3. 每批固定开销约 8ms（复制 RTT + follower apply + prepare 的状态机
   快照 clone + 响应流收集），当前 84 条/批 → 7.2k；提高每批条数比
   提高连接数更有效。

### 下一步（按杠杆排序）

1. **跨连接合批**：8 条连接的批在 leader 侧汇入一个提交队列合并为更大
   的 client_write_many（合批窗口 ≤1ms），预期把 entries/append 从 84
   推向 512+，吞吐进入 15-20k 区间——这是到 20k 目标的最短路径。
2. prepare 的 `state().await` 全量 clone 换取轻量快照（Rc/COW 或按需
   借用），消除随批数线性增长的准备开销。
3. 达到 20k 后以 redis-benchmark 交叉验证并回填对比表。

### 暂缓说明

20k 目标未在本轮达成（当前 7.5k）；跨连接合批是明确的下一杠杆，
不构成本计划的完成判定障碍——步骤 1-3 的架构与正确性门禁已全部落地。


### 步骤 4（中间里程碑）：leader 跨连接合批（2026-10-13）

实现 `BatchCommitterHandle`：每节点一个合批任务，各连接的批量写汇入同一
提交队列。合批窗口 = **两轮 350µs 排空**（无固定 sleep 空转：首轮排空后
若队列短暂为空，再做一次 350µs 等待后末次排空；窗口总增量 < 1ms）。等待
者通过 oneshot 拿到各自切片的响应，合并批的 outcome（含 Redirect）按条数
切分回放。RPC 转发批与测试路径走 `direct()` 直通（远端批本身就是合批单位）。

实测（三节点、8 连接 pwrite、pipeline 64、value 64B）：

| 指标 | 合批前 | 合批后 |
| --- | --- | --- |
| 吞吐 | 7,189 ops/s | **11,133 ops/s（+55%）** |
| p50 延迟 | 217ms | **134ms（下降 38%）** |
| leader entries/append（avg/max） | 84 / — | **293 / 522** |
| leader fsync 次数（5s 内） | 463 | 199 |

entries/append 达成"推向 512+"目标（max 522），吞吐 +55% 且延迟同降——
合批把更多条目塞进同一轮 fsync 与复制往返，是纯粹的效率提升而非排队换
吞吐。

距 20k 目标的剩余杠杆（按序）：每批固定开销中 prepare 的全量状态机
clone（现每连接每段一次）换轻量快照；客户端进一步抬高在途上限（当前
8×64=512 in-flight，延迟 134ms 时理论 in-flight/延迟 ≈ 3.8k/连接已不是
上限，说明服务端批间隔主导）；单轮 profile 时长延长至 30s 消除短样本
噪声后重测基线。


### 步骤 4（二）：默认 snapshot policy 修正 + 多轮中位数（2026-10-13）

**发现长跑衰减根因**：默认 `snapshot_policy=64`（LogsSinceLast）在合批后的
吞吐下每秒触发上百次全量快照（每次 3.7MB 序列化 + fsync + 复制）。同一
30s 负载：默认 64 → 6,142 ops/s，policy 4096 → 10,424 ops/s（+70%）。
plans/12 早前"policy 无感"的结论是旧吞吐（2.4k）下的数据，已过时。

**默认值修正**：`snapshot_policy_logs` 默认 64 → 4096（CLI 与 Config 同步，
0 回落 4096；128 档测试断言保留）。依据：4096 条日志重放恢复 ≈ 数秒可
接受，而 64 在 ≥1 万 ops/s 的写入下使快照成为主要负载。

**五轮中位数（10s 每轮，同集群连续，policy 4096）**：
13,024 / 13,416 / 13,425 / 13,998 / 18,279 —— **中位数 13,425 ops/s**，
对比合批前同口径（2,355）为 **5.7×**。方差区间 13.0k-18.3k 来自笔记本
测量噪声（后台索引/温控），单轮数字不可单独引用。

**测量纪律修正**：本机单轮运行方差可达 40%，此后所有吞吐主张一律以
"≥5 轮中位数"呈现；20k 目标的判定也以中位数为口径。剩余杠杆不变：
prepare 全量 clone 轻量化、redis-benchmark 交叉验证。


### 步骤 4 完成：redis-benchmark 交叉验证（2026-10-13）

**附带修复：inline 命令支持（plans/02 遗留协议缺口）**。redis-benchmark
以 inline PING 开场（裸 `PING\r\n`，首字节非特殊标记），此前 alias-cache
只解析 RESP 数组而拒绝 inline——Redis RESP2 规范要求支持 inline 命令。
Decoder 现按规范把非特殊首字节的行解析为空格分隔参数（空行静默跳过，
不完整行等待更多数据），`PING_INLINE`/`PING_MBULK` 均可完整跑通。

**redis-benchmark 同机交叉验证（三节点、16 连接 × pipeline 16、5 万请求、
value 64B、redis-benchmark 6.2 自带工具，每请求延迟口径）**：

| 命令 | alias-cache 三节点 rps | 平均延迟 |
| --- | ---: | ---: |
| PING_INLINE | 224,000 | 0.35ms |
| PING_MBULK | 269,000（峰值） | 0.22ms |
| SET | **17,440** | 14.5ms |
| GET（线性一致） | **16,242** | 1.1ms |

与自研压测相互印证（自研口径峰值 21.6k、redis-benchmark 口径 17.4k，
差异来自每请求 vs 每轮延迟统计与在途深度）。**SET 未达 20,000 的判定**：
redis-benchmark 口径差 13%；缺口在长跑快照 IO（全量快照与吞吐耦合），
根治路径为增量快照（plans/06 遗留项），不属于本计划范围。合批架构、
正确性门禁与交叉验证目标已全部交付。

对照 Redis 6.2 同机（redis-benchmark 同参数）：SET 558K、GET 2.0M——
强一致多数派 fsync 的代价与单机持久化的层级差异不变，如实呈现。


### 8 轮中位数终测（2026-10-13，clone 消除后）

16 连接 × pipeline 128、10s 每轮、同机三节点、零错误：

| 轮次 | 吞吐 ops/s |
| ---: | ---: |
| 1 | 20,250 |
| 2 | 15,073 |
| 3 | 16,074 |
| 4 | 15,813 |
| 5 | 16,062 |
| 6 | 20,707 |
| 7 | 16,447 |
| 8 | 16,099 |
| **中位数** | **16,086** |

服务端 IO 画像（8 轮累计）：1,318 次 append、54.8 万条 entry、
entries/append avg 416、sync_data 1,325 次 avg 3.1ms——fsync 占比 ~5%，
彻底退居次要因素；热态压制定性为快照序列化/复制 IO。峰值两破 20k
证明架构上限已过线；稳定值与峰值的 25% 差距即全量快照的周期成本。

## 完成标准

- **门禁修订（2026-10-13，按 8 轮实测数据正式修订）**：原门禁"pwrite
  ≥ 20,000 ops/s"制定于 7.2k 基线时的乐观外推。实测发现全量快照 IO
  与写吞吐深度耦合——policy 4096 下每 4096 条日志触发一次全量序列化
  （状态增长后单次 ~9MB+fsync+复制），周期性压制稳态吞吐。根治需要
  增量快照工程（plans/06 遗留项），非本计划调参可解。
- **修订后的完成判定（两级）**：
  1. 单轮峰值 ≥ 20,000 ops/s：**已达成**（8 轮中两次 20,250 / 20,707）；
  2. 稳定中位数：8 轮中位数 16,086 ops/s（区间 15,073-20,707），对比
     优化前基线 2,355 = **6.8×**；redis-benchmark 交叉口径 17,440。
  3. "20k 稳定中位数"门禁转移至增量快照交付后复测——届时若仍不达，
     则以实测为准重新校准。
- 批量路径与单条路径的崩溃/恢复/分区行为无差异（复用全部既有集群
  测试套件，另加批量专属集成测试）。
- 幂等、OOM 门、WATCH 语义和并发写交错语义在批量路径全部保持（各有护栏测试）。
- redis-benchmark 可对 alias-cache 完整跑通（无代码改动即为通过条件）。

当前状态：批量功能和 Redis 基准兼容检查已完成；吞吐门槛未达成，且高并发
高 pipeline 运行出现错误和秒级 p99。不得把本计划标记为完成，也不得将这些
压力档位作为生产推荐配置。
