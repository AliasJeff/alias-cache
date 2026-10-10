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
- 依赖许可证和供应链检查；
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
