# 13 批量写管线与性能主张

## 目标

复刻 cache-cat 的批量提交架构（`client_write_many` + 流式管线客户端 + 批量 RPC），
把三节点多数派写吞吐从当前 2,355 ops/s 提升一个数量级以上，并以同机对照测量
验证"强一致 + 写性能对标 cache-cat"的性能主张。测量口径同时对齐业界标准
（每请求延迟）并引入 redis-benchmark 交叉验证。

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
   接收 `Vec<Request>`，返回逐条结果的 `BoxStream`。一批请求一次复制、
   一次 fsync，结果是流式的（提交后逐条 apply 逐条回）。
   - 对应 alias-cache 落点：`alias-cache/src/raft.rs` 新增
     `execute_client_batch_request`，包一层请求 ID 去重（复用现有
     `Operation::Client` 幂等机制，批内每条独立 ID）。
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
   `Operation::Client` 缓存），转发重试时整批同 ID 重发，保证非幂等命令
   不因批量而失去 exactly-once 窗口语义。

## 实现步骤（每步独立提交，含测试）

### 步骤 1：服务端批量提交入口

- `raft.rs` 新增 `execute_client_batch_request_with_ids(raft, ids, requests)`：
  - 一次 `raft.client_write_many(...)`？——否。openraft 0.10-alpha.34 的
    `client_write_many` 签名与结果流需先核实（cache-cat 用的是其自有
    TypeConfig 下的 API）；若该版本 API 不可用，退化方案为：构造单条
    `RaftRequest::Apply(Operation::Transaction { operations, ... })`——
    事务已是"多命令一次提交"的原生批量，`watch_expectations` 为空即纯批量。
    **先验证退化方案的吞吐**（可能已达标），再决定是否需要真
    `client_write_many`。
  - 批内 prepare：在 leader 状态机快照上逐条 `prepare_request`（与
    MULTI/EXEC 相同路径），全部成功才提交，任一失败整批拒绝（与事务
    语义一致，简单且可预期）。
- 测试：单节点批量写 N 条 = 一个日志条目、N 条响应按序返回；批内含
  非法命令时整批拒绝且零残留；幂等：同 IDs 重发整批，响应来自缓存
  且状态不重复应用。

### 步骤 2：RESP 批量写 RPC 与转发

- `NetworkRequest::ClientBatch` 变体 + `handle_raft_rpc` 分支 + leader
  端执行 + follower 端整批一跳转发（复用现有 `Redirect` 逻辑，地址来源
  同单条版）。
- `server.rs` 的 pipeline 并发路径升级：同段写不再逐条 spawn，而是整段
  作为一个 batch 走新入口（读仍按序跟随）。`is_pipeline_safe`/
  `is_replicated_write` 分类复用。
- 测试：三节点 TCP 集成——follower 连接收 16 条写，观察单日志条目提交、
  响应按序；leader 切换期间整批转发正确；请求 ID 去重覆盖批语义。

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

## 风险与回退

- `client_write_many` 在 0.10-alpha.34 的可用性未知 → 步骤 1 优先走
  Transaction 批量化退化路径，性能不达标再评估 API。
- 批量提交放大单条日志体积（bincode 全批操作）→ 复用
  `NETWORK_MAX_SNAPSHOT_SIZE` 之上的批量上限常量（如 4 MiB），超限分批。
- follower 整批转发在 leader 切换窗口的重试语义：批请求 ID 整体复用，
  重试可能遇到"部分已提交"——幂等缓存返回整批首次结果，与单条语义
  相同，无新增窗口。

## 完成标准

- 三节点多数派写 pwrite ≥ 20,000 ops/s（同机对照，测量纪律在案）。
- 批量路径与单条路径的崩溃/恢复/分区行为无差异（复用全部既有集群
  测试套件，另加批量专属集成测试）。
- 幂等、OOM 门、WATCH 语义在批量路径全部保持（各一条护栏测试）。
- redis-benchmark 可对 alias-cache 完整跑通（无代码改动即为通过条件）。
