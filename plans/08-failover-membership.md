# 08 故障切换、路由与成员管理

## 目标

把基础 Raft 集群扩展为可操作的生命周期，并明确客户端在 Leader 变化期间的行为。

## 实现内容

### 08.1 Leader 路由与错误语义

1. 以 `Raft::current_leader()` 和 `membership_config` 作为 Leader 发现来源，不在客户端连接层维护独立的选主状态。
2. 非 Leader 的写请求返回明确的 `ForwardToLeader`/`TRYAGAIN` 类错误；响应中只携带已知地址，不能伪造成功。
3. 服务端转发最多执行一跳；转发请求必须携带请求 ID，不能因为连接重试而重复执行非幂等操作。
4. 区分以下状态：
   - 请求尚未提交：客户端可以安全重试；
   - 已提交但响应丢失：结果未知，必须通过请求 ID 或读路径确认；
   - Leader 已改变：重新发现 Leader 后再决定是否重试；
   - 无 quorum：写请求不得返回成功。

进度记录（2026-10）：

- follower 上的读请求此前直接把 openraft `ForwardToLeader` 内部错误透传给客户端；
  现已映射为与写路径相同的 `ClientRequestOutcome::Redirect`，由服务层一跳转发到
  Leader 执行 ReadIndex 读（`alias-cache/src/raft.rs::execute_client_request_with_id`）。
  测试：`server::tests::follower_connection_serves_reads_through_leader_redirect`
  在真实三节点 TCP 集群上经 follower 连接读到 Leader 写入的值。
- 服务器多节点启动路径修复：非种子 voter 不再等待自己成为 Leader
  （`raft.rs::wait_for_any_leader`），且启动时即启用选举运行时
  （`server.rs::Server::open_raft`）。测试：
  `server::tests::cluster_servers_form_and_fail_over_without_seed`
  杀掉种子节点后由剩余 voter 选出新 Leader 并成功写入。

### 08.2 learner 加入与追赶

1. 新节点先启动本地 `Raft`、日志存储和 RPC listener，但不调用 `initialize`，也不把自己放入初始 voter 集合。
2. Leader 通过 `add_raft_learner()` 调用 OpenRaft `add_learner(node_id, node, false)`，先提交 learner membership，再启动复制。
3. 用 Leader metrics 的 `replication[node_id]` 与 `last_log_index` 判断追赶状态；不能用固定 sleep 或“收到过一条日志”作为追平条件。
4. 追赶期间允许继续写入；提升前必须再次读取最新 metrics，确保 learner 已覆盖当前提交边界。
5. 日志足够新时走 AppendEntries；落后到日志保留边界之外时由快照阶段接管，详见 `06-snapshot.md`。

### 08.3 learner 提升与 voter 移除

1. 追平后调用 `change_membership(new_voters, false)`，将 learner 加入 voter 集合。
2. 成员变更必须等待 membership entry 提交完成，并在所有相关节点的 metrics 中观察到新配置。
3. 移除 voter 前计算剩余集合，禁止移除最后一个 voter；生产实现还必须校验移除后仍满足配置要求的 quorum。
4. 默认先移除 voter，再决定是否保留为 learner；只有显式 drain/remove 流程才彻底移除节点元数据。
5. 自我移除需要拆成“先退出 voter、再移除节点”的两个受 Raft 控制的变更，不能直接删本地状态。

### 08.4 健康、分区与发现

1. 暴露 `voter_ids`、`learner_ids`、Leader、term、commit/apply index、replication lag 和 membership config。
2. 健康分为进程存活、Raft 已恢复、当前节点可读、当前节点可写四级；无 quorum 时可读与可写状态必须分开。
3. 网络分区恢复后验证旧 Leader 的冲突日志被 Raft 覆盖，禁止使用本地日志长度判断数据正确性。
4. Sentinel 风格命令只读取 Raft membership/Leader 状态，不参与选主、不修改 membership、不复制 Sentinel 的故障检测模型。

## 参考 cache-cat

### 当前实现对应关系

- `alias-cache/src/raft.rs::new_tcp_raft`、`new_tcp_raft_joiner`、`new_tcp_raft_inner`：
  对应 `cache_cat/src/node/raft_node.rs`、`raft_builder.rs` 的节点构造与 bootstrap 边界。重点保持“bootstrap 节点初始化初始 voters，joiner 只启动 Raft 不初始化 membership”。
- `alias-cache/src/raft.rs::serve_raft_rpc`、`TcpNetworkFactory`、`TcpNetwork`：
  对应 `cache_cat/src/raft/network/raft_network.rs`、`network/rpc.rs`、`network/model.rs`。后续扩展 Snapshot RPC 时仍保持协议请求、响应和错误分类分层。
- `alias-cache/src/raft.rs::TcpNetworkControl`、`new_tcp_raft_with_control`、
  `new_tcp_raft_joiner_with_control`：
  是在 `cache_cat/src/raft/network/client.rs` 和 `network/raft_network.rs` 基础上增加的
  可测试链路故障注入层。普通构造函数使用空 control，不改变生产网络行为；测试构造函数共享
  control 后可以按 NodeId 阻断双向链路，验证真实的 Raft 多数派行为。
- `alias-cache/src/raft.rs::execute_client_request_with_id`、
  `send_client_request_with_id`、`NetworkRequest::Client`：
  对应 `cache_cat/src/raft/network/client.rs`、`network/external_handler.rs`。当前只实现一跳 Leader 转发；重试策略必须继续放在服务层，不下沉到状态机。
- `alias-cache/src/state_machine.rs::Operation::Client`、
  `StateMachine::client_results`：
  在 cache-cat 的 RPC request-id 帧和响应槽位关联基础上增加复制层去重。请求 ID 随业务
  operation 进入 Raft 日志，已应用响应随状态机 snapshot 保存；旧 Leader redirect 后由服务层
  使用同一个 ID 重发现并重试。当前缓存最多保留 4,096 个已完成请求，超过窗口后旧 ID 不再
  提供无限期 exactly-once 保证，客户端仍应在超时后通过查询或业务幂等键确认结果。
- `alias-cache/src/raft.rs::add_raft_learner`：
  对应 `cache_cat/src/raft/application/cluster.rs::add_learner`。当前使用非阻塞 OpenRaft API，显式调用 `wait_for_raft_replication` 观察追赶完成，避免无限阻塞。
- `alias-cache/src/raft.rs::promote_raft_learner`、`remove_raft_voter`、`drain_raft_voter`：
  对应 `cache_cat/src/raft/application/cluster.rs::change_membership`、`remove_self`。当前覆盖新增 voter、最后 voter 保护、voter drain 和 self-drain；drain 使用 `retain=true` 保留 learner 元数据，真正彻底移除仍由显式 remove 流程完成。
- `alias-cache/src/raft.rs::update_raft_node`：
  对应 `cache_cat/src/raft/application/cluster.rs::remove_self` 的“先从 membership 移出”和
  `cache_cat/src/raft/network/external_handler.rs::add_node` 的“重新以节点元数据加入”边界。
  OpenRaft 对直接 `SetNodes` 更新地址存在 split-brain 风险，当前只允许 Learner 通过
  `RemoveNodes` + `add_learner` 迁移地址；Voter 必须先 drain，再以新地址重新加入。
- `alias-cache/src/raft.rs::wait_for_raft_replication`：
  对应 OpenRaft metrics 与 cache-cat cluster facade 的节点状态查询；它以 `replication[node_id] >= last_log_index` 作为追平条件，不复制 cache-cat 的隐式等待实现。
- `alias-cache/src/raft.rs::tests::tcp_cluster_adds_promotes_and_removes_learner`：
  对应 `cache_cat/tests/test_cluster.py` 的集群编排，覆盖 1-3 初始 voters、4 号 learner、追赶、提升和移除。
- `alias-cache/src/raft.rs::tests::tcp_old_leader_conflict_is_overwritten_after_partition_recovery`：
  对应 `cache_cat/src/raft/network/raft_network.rs` 的 AppendEntries 复制路径和
  `cache_cat/tests/test_cluster.py` 的故障恢复编排；额外验证旧 Leader 的未提交尾部被新
  Leader 的冲突日志覆盖，最终比较完整状态机而不是比较本地日志长度。
- `cache_cat/src/raft/types/entry/membership.rs`：
  只作为 membership entry/JoinRequest 的建模参考；alias-cache 的 membership 记录由 OpenRaft 管理，不把成员变更伪装成业务 `RaftRequest::Apply`。
- `cache_cat/src/protocol/sentinel/core.rs`、`masters.rs`、`slaves.rs`、`sentinels.rs`、`get_master_addr.rs`：
  仅在 RESP3/发现阶段实现只读兼容命令时参考。
- `alias-cache/src/sentinel.rs`、`alias-cache/src/config.rs::SentinelContext`：
  对应 cache-cat 的 Sentinel 只读命令模块，但将 master name、客户端监听地址和 Raft
  membership 明确分离。`Node.addr` 仍只用于 Raft RPC；`cluster_client_addrs` 才用于
  Sentinel 返回的客户端连接地址。当前实现不执行 Sentinel 选主、监控、FAILOVER 或配置写入。
- `cache-cat/docs/cluster_access_strategy_en.md`、`cluster_access_strategy_zh.md`：
  用于确认 Sentinel 适配层边界；不能把 Sentinel 选主、故障检测或 quorum 逻辑引入 alias-cache。

## 重点借鉴

先完成明确的 Leader redirect/error 和成员安全检查，再实现 Sentinel 兼容。Sentinel 命令只读取 Raft 状态，不能改变 Raft 状态。

## 测试任务

### 已完成门禁

- [x] TCP 三节点复制测试不依赖固定 sleep。
- [x] 4 号节点以空 membership 启动，作为 learner 加入。
- [x] learner 追赶后提升为 voter。
- [x] 移除 voter 后观察新的 membership config。
- [x] 单节点禁止移除最后 voter。
- [x] learner 追赶期间持续提交 16 个写入，并分别等待 leader 日志追平、learner 应用最新日志后校验状态机数据。
- [x] 日志 purge 后加入 learner，验证通过 snapshot 追赶并应用快照后的新日志。
- [x] drain voter 后保留为 learner；self-drain 先提交成员变更、再停止本地节点，剩余 voter 仍能选主并写入。
- [x] drain 后再次调用 voter remove 会稳定拒绝，避免把 learner 当作 voter 操作。
- [x] 成员变更覆盖重复提交、目标节点重启、旧 Leader 下线后由新 Leader 继续提升，以及 Learner 地址迁移后关闭旧 listener 仍能复制。
- [x] 少数派分区分别断言多数派线性读写、少数派读的 quorum 错误、少数派写的无 Leader 错误、follower redirect 和多数派 Leader 变更。
- [x] 分区恢复后旧 Leader 先产生未提交冲突尾部；多数派提交新值后解除分区，三台节点完整 `StateMachine` 逐字段一致且不保留旧值。

### 后续执行步骤

1. [x] 为成员变更增加 leader 丢失、重复提交、目标节点重启和地址更新测试。
2. [x] 增加少数派分区测试：可读、可写、Leader 变更和错误分类必须分别断言。
3. [x] 增加旧 Leader 恢复测试，确认冲突日志被覆盖且状态机结果与新 Leader 一致。
4. [x] 增加客户端连接旧 Leader 后的 redirect/重发现测试，并验证非幂等命令不会重复执行。
   `tcp_minority_partition_separates_read_write_and_redirect_errors` 在分区恢复后连接旧
   Leader，断言 `MOVED` 地址、同一请求 ID 在新 Leader 执行，以及改 payload 的重复请求不
   覆盖第一次结果；`state_machine::tests` 覆盖响应缓存和序列化恢复。
5. [x] 实现只读 Sentinel 风格发现命令，并建立兼容性矩阵。
   `SENTINEL MASTERS`、`GET-MASTER-ADDR-BY-NAME`、`SLAVES`、`SENTINELS` 均只读取
   Raft metrics/membership；`sentinel::tests` 覆盖单节点地址发现和三节点扁平字段结构，
   `server::tests::single_node_server_serves_read_only_sentinel_discovery` 覆盖真实 RESP
   TCP 入口。FAILOVER、MONITOR、SET 等写操作仍明确返回不支持错误。
   - 已完成节点元数据拆分：`Node.addr` 只用于 Raft 复制，
     `cluster_client_addrs` 用于客户端发现。
   - 已完成 master name 配置、空名称校验、名称不匹配和单节点默认行为。
   - 已完成第一批只读命令：`SENTINEL MASTERS`、
     `GET-MASTER-ADDR-BY-NAME`、`SLAVES`、`SENTINELS`。
   - 当前限制：健康 flags 仍是基于 Raft metrics 的简化表示，不实现 Sentinel 自身的
     监控、选主、FAILOVER 或配置写入。
   - 已完成真实 RESP TCP 入口测试；`scripts/smoke_cluster.py` 和
     `scripts/smoke_compose.py` 在 Leader 被终止后通过 Sentinel 地址发现新 Leader，
     并验证新 Leader 写入及旧节点重启追赶。

### Sentinel 兼容性矩阵（设计基线）

| 命令 | 参数 | 返回形状 | 数据来源 | 写入 Raft | 当前状态 |
| --- | --- | --- | --- | --- | --- |
| `SENTINEL MASTERS` | 无 | master 信息数组 | 当前 Leader、membership、节点存活状态 | 否 | 已实现 |
| `SENTINEL GET-MASTER-ADDR-BY-NAME` | master name | `[host, port]` 或 null | 当前 Leader 的客户端地址 | 否 | 已实现 |
| `SENTINEL SLAVES` | master name | replica 信息数组 | 非 Leader voter/learner 与复制状态 | 否 | 已实现 |
| `SENTINEL SENTINELS` | master name | Sentinel 信息数组 | 配置节点与健康状态 | 否 | 已实现 |
| `SENTINEL FAILOVER` | master name | 错误 | 非目标能力 | 禁止 | 明确不支持 |
| `SENTINEL MONITOR` | 参数可变 | 错误 | 非目标能力 | 禁止 | 明确不支持 |
| `SENTINEL SET` | 参数可变 | 错误 | 非目标能力 | 禁止 | 明确不支持 |

## 完成标准

- 成员变更全部经过 Raft 提交，业务状态机不直接修改 membership。
- learner 只有在复制追平最新日志后才能提升为 voter。
- 不会出现两个都能安全提交写入的 Leader，少数派不能返回写成功。
- Leader 切换期间错误类型稳定，已提交但响应丢失的请求不会被盲目重放。
- 新节点加入、追赶、提升、drain、移除均有可观察状态。
- 客户端可以通过明确接口发现当前 Leader，并能区分 redirect、无 Leader 和无 quorum。

## 非目标

- 不实现 Redis Sentinel 的选主和故障判断。
- 不实现 Redis Cluster 分片和 slot 重平衡。
