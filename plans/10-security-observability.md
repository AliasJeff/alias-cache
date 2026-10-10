# 10 安全、资源限制与可观测性

## 目标

建立服务的安全默认值、资源边界、健康检查和故障诊断能力。

## 当前实现进度

- 已实现按连接隔离的密码认证。
- 支持 `AUTH <password>` 和 `AUTH default <password>`。
- 支持 `HELLO 2/3 AUTH default <password>` 在协商协议的同时认证。
- 支持客户端入口 TLS，以及基于 CA 的可选 mTLS 客户端证书校验；启动时加载并校验
  PEM 证书链和 PKCS#8/RSA/EC 私钥。
- 客户端 TLS 与 Raft 节点间 TLS 分开配置；`--tls-replication` 启用节点间双向
  TLS，普通 RPC 和分块快照共用同一加密传输抽象。
- 配置密码时，未认证连接只能执行认证、HELLO 和 QUIT；普通命令返回 `NOAUTH`。
- 密码按原始字节比较，不写入启动日志；空密码配置会在启动校验阶段拒绝。
- 已覆盖未认证请求、错误密码、认证后读写和 HELLO 内联认证测试。

## 实现内容

- AUTH 和密码配置。
- TLS 客户端连接。
- 节点间 TLS/mTLS。
- 可选双向 TLS 和证书校验。
- 连接数、请求数、Pipeline、事务、Key、Value、日志和快照限制。
- 内存使用估算和最大内存保护。
- 连接、命令、Raft、WAL、快照、TTL 和错误指标。
- Leader、term、commit/apply、quorum、日志大小和快照状态指标。
- 健康检查、存活检查和可写就绪检查。
- 敏感配置不写入日志。
- 配置校验和安全默认值。
- 优雅关闭：停止接收、等待可完成请求、持久化必要元数据、关闭 Raft 和网络。

## 参考 cache-cat

- `cache_cat/src/cfg/config.rs`、`cache_cat/src/node/parsed_config.rs`：参考 TLS、端口、路径和配置校验。
- `cache_cat/src/raft/network/tls.rs`：参考客户端/节点间 TLS 初始化和证书错误分类。
- `cache_cat/src/error.rs`：参考认证、TLS、配置和可重试错误的统一表达。
- `cache_cat/src/protocol/connection/auth.rs`：参考 AUTH 命令和未认证请求边界。
- `cache_cat/src/protocol/server/memory/*.rs`：参考内存统计、诊断和管理命令的组织方式。
- `cache_cat/src/protocol/server/shutdown.rs`、`server/time.rs`：参考服务级命令和优雅关闭入口。
- `cache_cat/src/main.rs`、`node/raft_builder.rs`：参考后台任务、信号处理和节点状态日志。

## 不能直接复制

cache-cat 的指标和管理命令不能自动视为 alias-cache 的公开 API。先定义稳定指标名称、权限和资源成本，再暴露管理接口。

## 测试任务

- 未认证请求。
- 密码错误、认证后权限和连接重置。
- TLS 证书、私钥、协议版本和双向认证错误。
- 超过连接、请求、Value、事务和内存限制。
- 磁盘空间不足和快照空间不足。
- Leader 无 quorum 时 ready 状态变化。
- 指标在高并发下不阻塞命令路径。
- 优雅关闭和启动恢复。

## 完成标准

- 默认配置不会无意暴露未认证服务。
- TLS 配置错误在启动阶段失败。
- 所有限制触发后服务不会崩溃或无限增长。
- 关键 Raft、存储和协议状态可观测。
- 健康检查能区分进程存活、节点健康和安全写入。

## 完成记录

- 2026-10（首批）：AUTH 密码认证、客户端 TLS、Raft 复制 mTLS、INFO 可观测
  命令、优雅关闭已交付（各自 feature 提交）。
- 2026-10（续）：资源限制已落地——
  - `max-memory`（字节，0 不限）：状态机维护增量 `memory_bytes` 计数器
    （`insert_entry`/`remove_entry` 精确差量，恢复时 `recompute_memory` 重建，
    serde skip 不进快照格式）。prepare 层 OOM 门：写入的新载荷全量计入
    当前用量，超过上限返回 Redis 文本
    `OOM command not allowed when used memory > 'maxmemory'`；替换写按全额
    保守计费（宁可早拒不超发）；读与删除永不拦截；计数器恢复后重建，
    PartialEq 忽略派生计数字段。
  - `max-connections`（0 不限）：accept 循环维护活跃连接计数，超限时新
    连接收到 `-ERR max number of clients reached` 后立即关闭，已有连接
    不受影响。
  - CLI：`--max-memory`、`--max-connections`，缺省 0（不限）。
  - 测试：状态机 OOM 门（超限拒绝/拒绝不落盘/缩小放行/删除释放/关闭
    限制恢复/快照往返计数一致）、CLI 解析、连接上限端到端（占位、拒绝
    回复、释放后可再连）。
- 2026-10（续）：健康检查四级已实现——`INFO health` 输出 alive /
  raft_recovered / readable / writable，全部取自单一 openraft metrics 快照。
  分级语义与探针组合（liveness=TCP+PING，readiness=readable）记录于兼容性
  文档；writable 为节点自报信号，quorum 丢失时由真实写失败兜底。
  测试：单节点 INFO 全段落断言（含四级全真）。
