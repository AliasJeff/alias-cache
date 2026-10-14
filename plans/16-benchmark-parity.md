# 16 写吞吐对等实测：分层消除瓶颈，对齐 cache-cat 测量口径

## 目标

1. 以 cache-cat 的测量口径（批量管线客户端 + 批量提交 RPC）建立**等价对照
   实测**：构建 cache-cat 本体，用双方各自的批量压测在同机对测，用实测
   数字替换"500k 宣传"这个不可验证的标量。
2. 分层消除 alias-cache 批量路径上已定位的软件瓶颈（状态机 clone、快照
   IO、管线深度），使批量写吞吐进入同一数量级。
3. 全程区分两个口径并分别报告：**RESP 产品口径**（redis-benchmark /
   自研 RESP 压测，用户真实可见性能）与 **raft 直连口径**（共识管线上
   限，仅作诊断）。

## 背景数据（2026-10 实测，全部分析依据）

> **2026-10-14 更正**：下表多连接 RESP 数字（16,086 / 11,133 /
> 17,440 / 16,242）受合批器响应划分 bug 污染（详见 plans/13 修复节），
> 仅作历史参照；fsync 占比 ~5% 的归因结论不受影响。raft-direct 与
> 单连接数字不受影响。

当前（合批 + clone 消除 + policy 4096 后）三节点 pwrite：

| 配置 | 吞吐 | 备注 |
| --- | ---: | --- |
| 16c × pipeline 128，8 轮 | 中位数 16,086，峰值 20,707 | fsync 占比 ~5% |
| 8c × 64 | 11,133 | 合批器前 7,189 |
| redis-benchmark P16 | SET 17,440 / GET 16,242 | 每请求延迟口径 |

已定位的瓶颈（按预计贡献排序）：

1. **批量路径状态机全量 clone**：`execute_client_batch_request_with_ids`
   内 `state_machine.state().await` 每批 clone 一次（10 万 key 实测
   3.15ms/次），且 `with_state_machine` 的互斥锁在 clone + prepare 期间
   被持有，所有连接的批在此串行化。
2. **全量快照周期性压制**：policy 4096 下热态每秒 2+ 次全量序列化
   （状态增长后 9MB+）+ fsync + 复制，30s 长跑衰减 -30%。
3. **管线深度未扩展**：clone 消除与合批修复后，pipeline 256/512/1024
   尚未重测（此前劣化数据测于竞争 bug 时代，已失效）。
4. **测量口径缺口**：无 raft 直连压测模式，无法与 cache-cat 的 pwrite
   对等比较。

## 参考 cache-cat

- `benchmark/src/cache_cat_benchmark.rs`：`pwrite` op 的驱动方式——
  `PipelineMultiClient` 直连内部协议；我们只参考其驱动结构，传输仍走
  RESP 与 raft RPC 两个口径。
- `raft/network/pipeline_client.rs`（159 行）：流式客户端的队列/回调/
  重连结构——我们的 benchmark pwrite 模式已实现等价物（有界队列 +
  FIFO 配对），本轮补齐** raft RPC 传输档**。
- `raft/network/external_handler.rs::batch_write`：批量 RPC 的服务端
  形态——我们已有 `NetworkRequest::ClientBatch` + committer，等价。
- `raft/application/cluster.rs::client_write_many`：openraft 批量 API
  的薄封装——我们已用同一 API（非退化路径），等价。
- `benchmark/sh/run_with_redis.sh`：对 Redis 用官方客户端对测的公平性
  原则——我们对 cache-cat 本体也用**它自己的 benchmark**压测，反之
  我们用 redis-benchmark 双向交叉，不用单方数字下结论。

## 不直接复制

- cache-cat 的 benchmark 直连 bincode 内部协议且**绕开 RESP**——我们把
  raft 直连档仅作诊断口径，产品主张仍以 RESP 口径为准。
- 它的 500k 宣传无测量配置附录；我们的每个数字必须带完整配置（节点数、
  连接数、pipeline、时长、policy、快照状态）。
- 它的 write_clock 在 RPC handler 逐条生成；我们保持 write_time 在
  prepare 时统一注入（逻辑时钟语义不同，不复刻）。

## 实现步骤（每步含验证判据，独立提交）

### 步骤 0：profiling 基线与快照变量隔离

- macOS `sample <leader-pid>` 对 16c×128 pwrite 运行中的 leader 采样
  30s，输出火焰归因：`state().await` clone、bincode 序列化、sync_data、
  快照序列化各占 CPU 比例——把"预计贡献排序"变成实测排序。
- 快照隔离实验：同负载下 policy 4096 vs 65536 各 30s，量化快照的确切
  压制幅度（此前只有单点对照）。
- 判据：得到一张归因表，后续每步优化对应表中一项。

### 步骤 1：批量路径消除状态机 clone

- `MemStateMachine` 新增在互斥锁内直接借用真状态执行 prepare 的入口
  （当前 `with_state_machine` 闭包内先 `state().await` clone 再
  prepare——改为闭包内直接 `&mut` 真状态；失败的批丢弃副本的语义改由
  openraft 的 apply 侧兜底：prepare 失败本就不进日志，真状态不被污染，
  因为 prepare 是纯校验+操作生成）。
- 若 prepare 需要跨批的工作副本语义（批内命令互见），在闭包内临时
  clone 也比跨互斥锁 clone 好——按 profile 数据决定最终形态。
- 判据：`sample` 归因表中 clone 占比显著下降；16c×128 中位数提升
  可测；全量测试与一致性护栏不回退。

### 步骤 2：管线深度扩展重测

- clone 消除后重测 pipeline 128/256/512/1024（此前 512 劣化数据测于
  竞争 bug 时代，作废）。
- 判据：entries/append 随深度继续上升；找到新甜点深度并写入默认
  benchmark 参数。

### 步骤 3：benchmark 增加 raft 直连传输档（诊断口径）

- `alias-cache-benchmark --transport raft`：直连 leader 的 raft RPC
  listener 发 `NetworkRequest::ClientBatch`（bincode 编码，复用现有
  批量 RPC——服务端零改动）。
- 该档仅报告"共识管线上限"，明确标注不含 RESP/命令层，不与产品口径
  混排。
- 判据：raft 直连口径数字与 RESP 口径的差值 = 协议层成本，单独成表。

### 步骤 4：快照 IO 实验（视步骤 0 归因决定深度）

- policy 矩阵在持续负载下重跑（4096/65536/262144，30s+），绘制吞吐-
  时间曲线定位每次快照的压制窗口。
- 若确认压制显著：`MemStateMachine::build_snapshot` 的序列化移入
  `spawn_blocking`（快照生成不阻塞 apply 路径），openraft 侧确认
  快照构建与日志追加可并行。
- 判据：快照窗口内的吞吐跌幅收窄；曲线进入下一台阶则继续，否则
  记录"此路径已到 openraft 结构约束"。

### 步骤 5：cache-cat 同机对等实测

- 构建 `~/projects/cache-cat`（其 README 提供构建方式），单节点与
  三节点两档。
- 对测矩阵：cache-cat pwrite vs alias-cache raft 直连 vs alias-cache
  RESP；同连接数/pipeline/时长/value。
- 产出分层归因报告写入 plans/13：双方上限、口径差、协议层差、以及
  "500k" 在受控条件下的真实复现值（或不可复现的如实记录）。

## 风险

- openraft alpha.34 的 `client_write_many` 在深管线下的内部行为可能
  有隐藏串行点——若 profile 指向 openraft 内部，该层为上游约束，如实
  记录并在报告中区分"我们的代码 vs 依赖内部"。
- 快照异步化若与 openraft 的快照契约冲突（快照必须覆盖 last_applied），
  回退为"policy 放宽 + 增量快照立项"。
- 笔记本测量噪声：所有主张以 ≥5 轮中位数为口径，单轮仅作参考。

## 完成记录

- 步骤 3 已交付（2026-10-13）：`alias-cache-benchmark --mode raft-direct`
  直连 leader 的 raft RPC listener 发送 `ClientBatch` bincode 帧（会话
  循环连接 + 4 字节 BE 长度前缀），与内部复制同协议。同时服务器
  `handle_raft_rpc` 改为会话循环（一连接多请求），follower 复制不再
  每 RPC 重连。
- 对等实测：alias-cache 单节点 raft-direct 32c×512 = **142,316
  writes/s**，同机 cache-cat pwrite = 154,000——差距 8%，数量级对齐。
  协议层成本量化：RESP 口径 27.7k vs raft-direct 55.2k（16c×128）。
  （**2026-10-14 更正**：27.7k 为多连接 RESP 口径，受合批器响应划分
  bug 污染，作废；142,316 raft-direct 数字不受影响，仍然有效。）
- 服务端附带改进：raft RPC 会话循环（ follower 复制不再每 RPC 重连）。
- 合批器响应划分 bug 修复（2026-10-14）：合并 chunk 的响应按提交者
  offset 切片回发（此前整 chunk 原样发给了每个提交者，多连接 RESP
  数字全部受污染，详见 plans/13 修复节）。benchmark 侧配套：
  `first_error` 采样（首个错误回复原文随结果打印，本 bug 即由它定位）、
  `count_replies` 错误计数修复、混合负载 `--write-percent`。
- 修复后首轮净测（2026-10-14，负载 avg 9-10 噪声环境，全零错误）：
  单节点 RESP 8c×128 = 22,275；三节点 RESP 8c×128 = 12,897；三节点
  raft-direct 8c×128 = 17,441（协议层成本 ~26%，同形状）；静默环境
  重测前不替换任何门禁数字。

### 步骤 6（新增，2026-10-14）：读路径扩展性与协议层并行化

- 现状实测：GET 逐条走 `ensure_linearizable(ReadIndex)`，pipeline 段内
  读串行；50% 混合负载 16c×128 在 5s 超时下吞吐塌缩（2c×32 仅
  123 ops/s，p50 1.23s），机制为每读一次心跳轮（50ms heartbeat）+
  多数派确认，写路径可合批摊销而读路径不能。
- 候选优化（按侵入度排序）：
  1. 段内连续读簇并行化：同一 pipeline 段内相邻 GET 共享一次
     ReadIndex barrier（读写边界仍严格排序），N 连续读从 N 次 barrier
     降为 1 次；
  2. 跨连接读 barrier 合批（对齐写侧 committer 结构）；
  3. 租约读（架构文档前置条件已具备，需时钟漂移护栏测试）。
- 判据：混合负载（50% 写）16c×128 在 5s 超时内零错误完成；GET p1
  往返与 SET p1 同数量级；全部线性一致护栏测试不回退。
- 静默环境前置：所有读路径基线（GET p1、混合矩阵）需在系统负载
  < 2 的环境下取 ≥5 轮中位数后再评估收益。

## 完成标准

- cache-cat 本体与 alias-cache 在同机、同负载下的对等实测数字写入
  plans/13，含完整配置与双方口径说明。
- 每一步优化有 profile 前后对比证据；redis-benchmark 口径不回退。
- 分层归因报告完成：每层（协议/提交/复制/fsync/快照）的成本占比有
  实测数字，剩余差距明确归因到层。
- 若最终仍与 500k 有数量级差距：报告中明确差距层级与原因（如 cache-cat
  数字本身口径不可复现），不以模糊措辞掩盖。
