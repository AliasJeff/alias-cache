# 12 Benchmark、兼容矩阵与发布

## 目标

把实现整理成可复现、可解释、可维护的个人项目，并用基线数据说明 alias-cache 的适用场景和代价。

## Benchmark 场景

- 单节点纯读；
- 单节点写入；
- 三节点多数派写入；
- 三节点线性一致读；
- 读写比例 95:5、80:20、50:50；
- Value 大小 16B、128B、1KB、16KB；
- Pipeline 长度 1、10、100；
- TLS 开启和关闭；
- fsync 策略不同；
- 快照期间吞吐和延迟；
- Leader 切换期间错误率、恢复时间和数据完整性；
- Follower 落后、快照安装和追赶时间；
- 内存占用、日志增长和快照大小。

## 参考 cache-cat

- `benchmark/Cargo.toml`、`benchmark/src/args.rs`：参考 benchmark 参数和场景配置。
- `benchmark/src/common.rs`：参考公共客户端、地址和结果收集工具。
- `benchmark/src/cache_cat_benchmark.rs`、`redis_benchmark.rs`：参考同一 workload 对比多个服务的结构。
- `benchmark/sh/start_single.sh`、`start_cluster.sh`、`run.sh`、`run_with_redis.sh`：参考启动单节点、集群和对比运行脚本。
- `benchmark/res/ycsb_throughput.png`：参考结果可视化；alias-cache 必须同时报告延迟分位数、错误率、资源占用和故障恢复指标。
- `cache_cat/tests` 下的 Python 测试：参考最终发布前的客户端兼容回归集合。

## 不能直接复制

cache-cat 的历史 benchmark 数字和硬件假设不能作为 alias-cache 的基线。所有结果必须在同一机器、同一 workload 和明确的持久化配置下重新测量。

## Redis/Valkey 对比规则

对比必须记录 CPU、内存、磁盘、操作系统、版本、节点数量、客户端数量、Key/Value 大小、Pipeline、读写比例、持久化、fsync、TLS、P50、P95、P99、最大延迟、吞吐、错误率、内存和磁盘写入。

不要用单一峰值证明“超过 Redis”。报告应分别说明读性能、已提交写入的可靠性、故障切换行为、一致性保证、写延迟和运维成本。

## 文档任务

- 快速开始；
- 单节点运行；
- 三节点运行；
- Leader 故障演示；
- 配置参考；
- 支持命令兼容矩阵；
- TTL 和一致性说明；
- 快照、备份和恢复说明；
- 存储格式和升级限制；
- 资源限制；
- 已知差异和非目标；
- 故障注入测试说明。

### 进度记录（2026-10）

- CLI 配置化已落地：`alias-cache-cli` 支持 `--node-id`、`--client-addr`、
  `--raft-addr`、`--data-dir`、`--sentinel-master-name`、`--bootstrap` 和可重复的
  `--peer NODE_ID=RAFT_ADDR[/CLIENT_ADDR]`。peer 解析与拓扑校验在库内实现并测试
  （`config.rs::PeerSpec::parse`、`Config::with_peers`；`config::tests`、
  `alias-cache-cli` 单元测试）。
- README 已补快速开始（单节点 + 三节点 + Leader 故障行为说明），并用真实
  `redis-cli`/RESP 会话手工验证：follower 读写透明转发、杀 Leader 后自动选主
  并继续写入。命令级截图/输出仍待发布阶段补充。

## 发布任务

- Linux、macOS 和 Windows 构建；
- Docker 镜像；
- 版本化二进制、日志和快照格式；
- 变更日志；
- CI 测试、Clippy、格式检查和构建矩阵；
- 依赖许可证和供应链检查；✅ `scripts/check-licenses.sh` 基于 Cargo.lock
  对全部依赖做 SPDX 白名单校验（MIT/Apache-2.0/BSD/ISC/Unicode/Zlib 等），
  未知许可证使 CI 失败；已接入 check job。
- 发布前运行多节点恢复、分区和快照测试。

## 完成标准

- 新用户可以启动单节点和三节点集群。
- 新用户可以复现 Leader 故障切换。
- benchmark 命令和环境记录可重复。
- 每个重要兼容承诺都有测试证据。
- README 明确系统优势、代价和不适用场景。
- 发布包在干净环境中可以启动、恢复和升级检查。

### Benchmark 基线（2026-10-10）

工具：`alias-cache-benchmark`（本仓库自带，RESP2 pipeline 客户端 + 延迟分位数）。
环境：Apple Silicon MacBook（darwin 25.2.0），release 构建，`--clients 8 --pipeline 16
--time 5 --value-size 64 --key-space 100000`，数据目录为空目录，raft election 150-300ms。

| 场景 | 吞吐 | p50 | p99 | 错误率 |
| --- | --- | --- | --- | --- |
| 单节点 PING（连接级，不进 Raft） | 1,030,014 ops/s | 112µs | 265µs | 0% |
| 单节点 GET（ReadIndex 线性一致读） | 335,606 ops/s | 343µs | 706µs | 0% |
| 单节点 SET（每写 fsync 提交） | 589 ops/s | 219ms | 248ms | 0% |
| 三节点 GET（Leader 线性一致读） | 4,378 ops/s | 27.5ms | 50ms | 0% |
| 三节点 SET（多数派提交 + 3×fsync） | 461 ops/s | 274ms | 345ms | 0% |

解读：

- 读路径（ReadIndex barrier + 内存读）在单节点上保持亚毫秒 p99；
  PING 不经过状态机，代表纯协议/网络上限。
- 写路径吞吐受每次提交一次 WAL fsync 串行化约束：延迟≈in-flight/吞吐
  （Little's law），p50 与 p99 趋同证实排队为瓶颈。已知优化路径：group
  commit、openraft 批量 append、生产 snapshot policy。
- 三节点 GET 的 leadership 确认（AppendEntries round）在复制压力下偶发
  50ms 超时；此前观测到约 0.04% 的 barrier 超时错误，本组干净运行无错误。
  宁可返回错误也不放宽一致性，是刻意的取舍。
- 复现命令：见 README 快速开始 + `alias-cache-benchmark --help`。

### Benchmark 工具进度（2026-10）

- `benchmark/` crate 已入库：可配置命令（set/get/ping）、并发、pipeline、
  时长、value 大小、key space、延迟采样率；输出吞吐与 p50/p95/p99/max。
- 帧解析有单元测试（完整/截断/错误帧计数）。
- 首次实跑即暴露服务端 decoder 的 pipeline 限制误用连接级累计的缺陷
  （长连接超过 1024 条命令后拒绝服务），已修复并加回归测试。

### 写路径优化：group commit + pipeline 并发提交（2026-10-10）

benchmark 暴露三节点 SET 仅 461 ops/s（读 4378）后，定位并修复两层串行化：

1. `FileLogStore::append` 曾对每条 entry 单独 fsync。openraft 的
   `append(entries, callback)` 契约要求回调前整批落盘——现在批量写、单次
   `sync_data`、再回调，即 group commit。`save_committed` 从"每提交点追加
   记录并 fsync"改为独立 `committed.meta` sidecar 的 write+rename 原子发布、
   不 fsync：commit 指针只是恢复加速，openraft 文档认可其回退无害（重启后
   等 cluster commit 覆盖才重新服务读）。`save_vote` 保持 fsync（选举安全）。
2. 服务端 pipeline 处理曾逐条 await——16 深度的 pipeline 变成 16 次串行
   Raft 往返。现在同段内的连续写并发提交（Raft 日志全序串行 apply，各自
   线性化），读严格按序执行且在前面全部写 apply 之后；响应按请求顺序写回。
   读-写分段边界由 `state_machine::is_replicated_write` 分类驱动，并有
   全命令分类与真实 mutation 一致性的护栏测试。

实测（三节点、8 客户端 × pipeline 16、release、Apple Silicon）：

| 场景 | 优化前 | 优化后 | 提升 |
| --- | --- | --- | --- |
| SET（多数派提交） | 461 ops/s | 2,352 ops/s | 5.1× |
| GET（线性一致读） | 4,378 ops/s | 17,651 ops/s | 4.0× |
| 单节点 GET | 335,606 ops/s | 435,165 ops/s | 1.3× |

测量纪律：GET 与 SET 必须各自在空闲集群上测——5 秒 SET 后 follower 仍在
追赶日志尾部，此时 barrier 的 leadership 确认延迟会数倍劣化读吞吐（此前
一组 1971 ops/s 的"退化"实为该顺序效应）。约 0.03% 的 GET 在选举边界收到
barrier 超时错误：宁可报错也不放宽一致性，是刻意取舍。

SET 的剩余瓶颈是每批一次的三节点 fsync 往返（p50 ≈ 53ms，符合
in-flight/吞吐 的排队模型）；进一步方向是 openraft 批量 append 的
更积极合并与 follower 端 fsync 策略。

### 写吞吐调优计划与判据（2026-10-11）

当前对照基线：三节点 SET 2,352 ops/s（p50 53ms），每批一次三节点 fsync
往返为已确认瓶颈（延迟符合 in-flight/吞吐 的 Little's law 排队模型）。

下一步实验设计与判据：

1. openraft 批量 append 合并粒度：在 benchmark 中固定 in-flight 总量
   （8 连接 × pipeline 深度 1/16/64），统计每批实际合并的 entry 数与
   fsync 次数/秒；判据为"pipeline 深度提升时 fsync 次数是否随之摊薄"，
   若合并不足（每批 entry 数 ≈ 连接数而非 pipeline 深度），则需评估
   openraft 的 client_write 合并行为或服务端提交队列。
2. follower 端 fsync 策略：测量 leader 与 follower 各自 fsync 在一次
   提交延迟中的占比（INFO stats 的 raft_log_bytes 增速 + 延迟分布），
   评估 follower 侧 `sync_data` 频率放松对安全性的影响（openraft 契约
   仍要求回调前落盘，放松空间有限，预期收益主要在 leader）。
3. 生产 snapshot policy 实测：以 `--snapshot-policy` 64/256/1024 三档
   分别跑 60s 写入，对比快照期间吞吐跌幅与日志重放恢复时间，选定
   默认值的依据记录回本节。

### 与 Redis 的同机对比基线（2026-10-11）

环境：同一台 Apple Silicon MacBook，release 构建，同一压测工具
（8 客户端 × pipeline 16 × 5s，value 64B，key space 100K）。
Redis 6.2.14 以两组配置运行：内存态（--save "" --appendonly no）与
最接近的持久化档（AOF appendfsync always，单机 fsync）。
alias-cache 以三节点集群（每批多数派 fsync + ReadIndex 线性一致读）运行。

| 场景 | Redis 内存态 | Redis AOF always（单机持久化） | alias-cache 三节点（强一致） |
| --- | --- | --- | --- |
| PING | 3,149,997 ops/s | — | 1,441,066 ops/s |
| SET | 1,523,299 ops/s | 558,973 ops/s | 2,355 ops/s |
| GET | 2,072,083 ops/s | 2,014,019 ops/s | 17,651 ops/s（空闲集群） |

诚实解读：

- alias-cache 的定位不是替代 Redis，而是给出**可解释的强一致代价**：
  每笔写入经三节点多数派 fsync 提交，延迟由复制往返与磁盘 fsync 主导；
  Redis 单机即使用 AOF always 也只 fsync 一台机器，且其副本为异步复制
  （failover 有丢数据窗口），两者保证不同层级，数字不可直接互换。
- 协议/网络能力（PING）alias-cache 达 Redis 的 46%——RESP 解析与
  连接层不是主要差距。
- 线性一致读（ReadIndex barrier）与 Redis 普通读的本质差异：alias-cache
  的读保证读到已提交的最新状态（跨故障切换不回退），Redis 单机读本身
  线性，但其 Sentinel/集群副本读存在复制滞后。
- 写吞吐的进一步方向见上文 group commit 分析（批量合并粒度、follower
  fsync 策略）；强一致系统的正确宣传语是"可解释的代价"，而非"追平 Redis"。
- 复现：README 快速开始 + `alias-cache-benchmark --help`；Redis 配置如上。
