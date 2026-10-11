# alias-cache

一个使用 Rust 实现的 Redis 兼容、基于 Raft 的强一致分布式内存 KV 存储。

alias-cache 的重点不是复刻全部 Redis 命令，而是用一个足够完整、可验证的工程展示：

- Redis RESP2/RESP3 协议实现
- Raft 共识、Leader 选举和成员变更
- 内存状态机与持久化日志
- 快照、恢复和落后节点追赶
- TTL 在复制状态机中的确定性语义
- 故障切换、网络分区和重启恢复
- 安全、可观测性、基准测试和可重复构建

项目会逐步扩大功能范围。每次迭代都必须先保证一致性语义、恢复语义和测试质量，再增加 Redis 命令或外围能力。

## 项目定位

alias-cache 适合作为：

- 分布式系统学习项目
- Rust 后端和存储方向的个人项目
- Redis 协议、Raft、WAL、快照和故障测试的综合实践

它不是 Redis 的替代品，也不承诺第一阶段兼容全部 Redis 行为。所有兼容性声明都必须由测试和文档支持。

## 目标能力

最终版本计划支持：

- Redis RESP2 和 RESP3
- 字符串、Hash、List、Set、Sorted Set 等常用数据结构
- TTL、事务、Pipeline
- Raft 三节点及以上集群
- Leader 转发、线性一致读和安全写入
- WAL、快照、崩溃恢复和落后节点快照安装
- 基础认证、TLS、管理接口和运行指标
- Sentinel 风格的主节点发现兼容层
- 故障注入、数据一致性测试和公开 benchmark

## 非目标

以下内容不作为早期目标：

- 一开始实现全部 Redis 命令
- 一开始追求极限吞吐量
- 自研完整 Raft 算法替代成熟实现
- 为了减少代码量而牺牲恢复测试
- 在没有一致性测试的情况下宣传“绝不丢数据”

## 文档入口

- [项目章程](docs/00-project-charter.md)
- [架构设计](docs/architecture.md)
- [协议与一致性模型](docs/protocol-and-consistency.md)
- [测试与验证标准](docs/testing-and-validation.md)
- [开发规范](AGENT.md)
- [开发计划](plans/README.md)

## 快速开始

```bash
cargo build --release
```

单节点模式（自动初始化为单节点集群）：

```bash
./target/release/alias-cache-cli --node-id 1
redis-cli -p 6379 SET hello world
redis-cli -p 6379 GET hello
```

三节点强一致集群（节点 1 种子成员，其余节点以 voter 加入）：

```bash
./target/release/alias-cache-cli --node-id 1 --client-addr 127.0.0.1:6379 \
  --raft-addr 127.0.0.1:16379 --data-dir data/node-1 --bootstrap \
  --peer 1=127.0.0.1:16379/127.0.0.1:6379 \
  --peer 2=127.0.0.1:16380/127.0.0.1:6380 \
  --peer 3=127.0.0.1:16381/127.0.0.1:6381 &

./target/release/alias-cache-cli --node-id 2 --client-addr 127.0.0.1:6380 \
  --raft-addr 127.0.0.1:16380 --data-dir data/node-2 \
  --peer 1=127.0.0.1:16379/127.0.0.1:6379 \
  --peer 2=127.0.0.1:16380/127.0.0.1:6380 \
  --peer 3=127.0.0.1:16381/127.0.0.1:6381 &

./target/release/alias-cache-cli --node-id 3 --client-addr 127.0.0.1:6381 \
  --raft-addr 127.0.0.1:16381 --data-dir data/node-3 \
  --peer 1=127.0.0.1:16379/127.0.0.1:6379 \
  --peer 2=127.0.0.1:16380/127.0.0.1:6380 \
  --peer 3=127.0.0.1:16381/127.0.0.1:6381 &
```

行为说明：

- 写入任意节点都会被透明转发给 Leader 并经多数派提交。
- 读取任意节点都走 ReadIndex 线性一致读，follower 读会转发到 Leader。
- 杀掉 Leader 进程后，剩余节点会自动选出新 Leader 并继续接受写入。
- `SENTINEL GET-MASTER-ADDR-BY-NAME alias-cache` 可发现当前 Leader 地址。

## 当前状态

已实现并由测试覆盖（当前 `cargo test --workspace --locked`：核心 crate 148 个测试、
CLI 9 个、benchmark 2 个测试全部通过）：

- RESP2/RESP3：`HELLO 2/3` 协商、Pipeline、MULTI/EXEC/DISCARD 事务
- 安全：`--password`/`ALIAS_CACHE_PASSWORD` 密码认证（AUTH/HELLO AUTH）、
  客户端 TLS、Raft 复制 mTLS
- 确定性状态机：String 类型全套 SET 选项、TTL 家族、INCRBY（i64 溢出保护）
- Hash/List/Set/Sorted Set：含 HINCRBY、ZINCRBY（f64 score 位模式确定性存储）
- SCAN/HSCAN/SSCAN/ZSCAN 确定性游标遍历
- WATCH/UNWATCH 乐观锁：版本随复制状态机，EXEC 时日志内 CAS 校验，
  检查与写入原子提交、无竞态窗口，被 watch 的 key 变更则事务整体 abort
- OpenRaft 三节点集群：选举、复制、ReadIndex 线性一致读、Leader 转发
- Learner 加入/提升/移除、网络分区、旧 Leader 冲突恢复
- 分块快照传输、快照安装/持久化、落后节点追赶、重启恢复
- 复制逻辑时钟 TTL：跨节点重放、故障切换、重启后语义一致
- 可观测：`INFO server|replication|cluster`（term/leader/commit/复制滞后/membership）
- 只读 Sentinel 风格主节点发现
- CLI 配置化（单节点/集群拓扑/种子引导）、Dockerfile + docker-compose 三节点编排
- 自带 `alias-cache-benchmark` 压测工具与基线数据（见 plans/12）

可重复的三节点故障切换与恢复 smoke test：

```bash
python3 scripts/smoke_cluster.py
```

脚本会创建临时数据目录，启动三节点集群，写入并读取数据，终止当前
Leader，验证新 Leader 继续写入，再重启原节点并验证其追赶已提交状态。
需要 Python 3；脚本只使用标准库，不依赖 Redis 客户端。
CI 还会通过 `python3 scripts/smoke_compose.py` 对 Docker Compose 部署执行相同的
Leader 故障切换和重启恢复检查。

Docker Compose 启动三个容器后，宿主机可分别通过 `127.0.0.1:6379`、
`6380`、`6381` 连接各节点；任一入口都可在其他节点故障后继续使用：

```bash
docker compose up --build --detach
redis-cli -p 6380 SET docker-smoke replicated
redis-cli -p 6381 GET docker-smoke
docker compose down
```

单节点基线（Apple Silicon，release，8 客户端 × pipeline 16）：
PING 103 万 ops/s；线性一致 GET 43.5 万 ops/s（p99 428µs）。

三节点集群（同负载）：线性一致 GET 1.77 万 ops/s（p99 13ms）、
多数派提交 SET 2,352 ops/s——写入经 group commit 与 pipeline 并发提交
（相比初版 461 ops/s 提升 5 倍），剩余瓶颈为每批一次的三节点 fsync 往返。

CI 已配置 Linux/macOS/Windows 三平台构建矩阵并上传构建制品。

与 Redis 6.2 的同机对比（同工具同负载，诚实口径）见 plans/12：
PING 1.44M ops/s（Redis 3.15M 的 46%）；三节点强一致 SET 2,355 ops/s
对比 Redis 单机 AOF-always 559K ops/s——这是"每笔写入三节点多数派 fsync"
的可解释代价，两者保证不同层级（Redis 副本为异步复制，failover 有丢失
窗口）。测试与发布：SET 全选项兼容矩阵、EXPIRE/PEXPIRE 协议边界测试、三处存储
格式的版本拒绝门禁与迁移策略文档（docs/storage-format-migration.md）、
CHANGELOG、依赖 SPDX 许可证白名单门禁。写吞吐的下一步在 plans/12 调优
记录中给出方向与判据：openraft 批量 append 合并粒度实验（不同 pipeline
深度下的每批 entry 数 vs fsync 次数）与 follower 端 fsync 策略评估；当前
2,352 ops/s 的三节点 SET 为对照基线。

客户端连接和 Raft 节点间链路均支持 TLS：同时指定 `--tls-cert-file` 和
`--tls-key-file` 即可启用客户端 TLS listener；额外指定
`--tls-ca-file --tls-auth-clients` 可要求客户端证书并执行 mTLS 校验。
集群节点间再指定 `--tls-replication`，Raft RPC 和分块快照会使用独立的双向 TLS
握手。未指定该参数时，Raft 链路保持明文，便于开发环境和协议测试。

## 许可证

Apache-2.0。
