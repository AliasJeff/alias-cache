# 06 并发快照、安装与落后节点追赶

## 目标

实现类似 cache-cat 的并发快照协议，控制日志增长，并验证严重落后节点可以通过快照恢复。

## 实现内容

- 当前阶段使用 OpenRaft 的快照元数据和 `MemStateMachine` builder；业务快照文件格式仍需在状态机持久化阶段独立版本化。
- 定义快照 magic、格式版本、元数据、校验和和文件布局。
- 实现 `Start`、`Tail`、`End` 状态。
- 为可变值维护版本号或等价 CAS 条件。
- 快照遍历期间记录有序增量操作。
- 在同一元数据锁下完成 Tail 切换、队列排空和 last applied log id 记录。
- 将多 Key 操作作为一个快照增量单元，保存所有依赖版本。
- 写临时文件、flush、fsync、目录 fsync 和原子 rename。
- 远程发送快照时先落地临时文件，再校验和安装。
- 安装期间暂停物理过期 worker，恢复后重新建立调度。
- 快照安装成功后清理被覆盖日志，失败时保留旧状态。
- 根据 Raft 日志保留阈值决定何时发送日志或快照。

## 参考 cache-cat

### 当前实现对应关系

- `alias-cache/src/raft.rs::MemStateMachine::build_snapshot`、`install_snapshot`、`get_current_snapshot`：
  对应 `cache_cat/src/raft/store/statemachine.rs` 的 snapshot builder、安装和 applied metadata；
  业务载荷现在使用 `RaftSnapshotPayload { version, state, pending_operations }` 独立版本化，`StateMachine` 中的
  逻辑时钟、TTL 元数据和请求结果缓存随状态一起保存，membership 仍由 OpenRaft
  `SnapshotMeta` 保存。
- `alias-cache/src/raft.rs::SnapshotState`、`MemState::snapshot_queue`、
  `RaftStateMachine::apply`、`RaftSnapshotBuilder::build_snapshot`、
  `MemStateMachine::build_snapshot_from_base`：
  对应 `cache_cat/src/raft/store/snapshot/snapshot_handler.rs` 的
  `Start/Tail/End` 生命周期和增量队列思路。Start 时固定业务状态基线并收集普通
  Raft 操作；Tail 时切换到不再收集新操作的状态并记录 `last_applied`；
  安装时先恢复基线，再按日志顺序重放队列。这里保留 OpenRaft 的 membership metadata，
  不把成员变更伪装成业务增量。
- `alias-cache/src/raft.rs::TcpNetwork::full_snapshot`、`handle_raft_rpc::NetworkRequest::Snapshot`：
  对应 `cache_cat/src/raft/network/raft_network.rs`、`network/external_handler.rs` 的远程 snapshot 发送和安装入口。
  旧 `Snapshot` 分支保留兼容；当前生产发送路径使用 `SnapshotStart` 加 chunk frame，
  后续仍需增加断点续传。
- `alias-cache/src/raft.rs::send_snapshot_rpc`、`read_snapshot_chunks`、
  `write_raw_frame`、`NetworkRequest::SnapshotStart`：
  当前已把远程快照从单个内存请求帧改为控制帧加固定大小 chunk 帧，
  接收端校验总长度、单 chunk 长度、总大小上限和滚动 checksum，并先写入唯一
  临时文件、`sync_all` 后再读入 OpenRaft 所需的内存快照；失败路径会清理临时文件。
  仍尚未达到 cache-cat 式断点续传能力。
- `alias-cache/src/raft.rs::trigger_raft_snapshot`、`purge_raft_log`：
  对应 `cache_cat/src/raft/application/cluster.rs::trigger_snapshot` 与 OpenRaft 的 purge trigger；业务层只触发 Raft，不直接删除日志文件。
- `alias-cache/src/raft.rs::new_tcp_raft_inner`：
  使用 `SnapshotPolicy::LogsSinceLast(64)` 作为当前开发阶段阈值，便于验证自动 snapshot；生产阈值必须由配置和资源预算决定。
- `alias-cache/src/raft.rs::tests::tcp_network_installs_snapshot_on_lagging_node`：
  对应 `cache_cat/tests/test_cluster.py` 的故障编排，以及 `cache_cat/src/raft/network/external_handler.rs` 的 snapshot RPC 路径，验证落后节点重启后恢复快照并继续复制。
- `cache_cat/src/raft/store/snapshot/snapshot_handler.rs`：
  参考 `Start/Tail/End`、元数据文件、临时文件、fsync 和原子发布；不能直接复制其文件布局。
- `cache_cat/src/raft/types/core/mocha/snapshot.rs`、`cas.rs`、`request_handler.rs`：
  下一阶段实现 TTL、版本号和多 Key 原子快照时参考；当前 `StateMachine` 尚未具备 cache-cat 的值级 CAS 增量队列。
- `cache-cat/docs/snapshot_en.md`、`snapshot_zh.md`：
  参考快照期间写入、Tail 切换、逻辑时钟和增量重放的设计约束，不作为 alias-cache 的格式规范。

## 不能直接复制

快照文件格式必须由 alias-cache 自己版本化。不能直接依赖 cache-cat 的 bincode 布局，也不能把 CAS 队列恢复当作普通命令重放。

## 重点

快照不是简单地把当前 HashMap 写到文件。必须证明快照内容、增量队列和 `last_applied_log_id` 匹配。

## 测试任务

### 已完成门禁

- [x] 状态机快照 builder 与 install snapshot 单元测试。
- [x] TCP snapshot RPC 编解码和 OpenRaft full snapshot 路径。
- [x] 落后节点重启后安装快照并继续复制。
- [x] snapshot 生成后 purge 日志前缀，后续写入仍可复制。
- [x] 快照文件和独立 SnapshotStore 的损坏校验测试。
- [x] learner 在日志 purge 后加入，使用 snapshot 追赶并继续应用快照后的日志。
- [x] 快照数据与 `last_log_id`、membership 元数据成对保存，覆盖快照生成后继续写入的恢复回归。
- [x] 快照增量操作恢复时推进 `StateMachine` 逻辑时钟，覆盖带 TTL 的写入不会因安装快照而时间倒退。
- [x] Raft snapshot 持久化、重启恢复和损坏拒绝；安装失败不会替换旧状态或旧快照文件。

### 后续执行步骤

1. [x] 将快照状态从裸 `StateMachine` 序列化扩展为带版本、逻辑时钟和 TTL 元数据的业务格式。
   `RaftSnapshotPayload` 在安装时拒绝未知版本；`memory_state_machine_applies_operations_and_snapshots`
   和 `raft_snapshot_rejects_unsupported_payload_version` 覆盖正常恢复及格式边界。
2. [x] 引入 `Start/Tail/End` 快照状态和有序增量队列，验证快照期间持续写入不会丢失。
   - 代码入口：`MemState::snapshot_state`、`MemState::snapshot_queue`、
     `RaftStateMachine::apply`、`MemStateMachine::build_snapshot`、
     `build_snapshot_from_base`、`install_snapshot`。
   - 参考：`cache_cat/src/raft/store/snapshot/snapshot_handler.rs` 的快照阶段切换，
     `cache_cat/src/raft/types/core/mocha/snapshot.rs` 的增量/版本一致性约束。
   - 当前实现边界：增量队列按完整 `Operation` 保存，事务和多 Key 操作不会被拆分；
     尚未引入 cache-cat 的值级 CAS、磁盘临时文件和异步分块序列化，这些属于后续步骤。
3. [进行中] 将 TCP snapshot RPC 从单帧内存传输改成受限分块/临时文件流，覆盖中断、重复安装和超大快照。
   - [进行中] 已完成 `SnapshotStart` 控制帧、`256 KiB` chunk、`512 MiB` 总大小上限、
     临时文件、`sync_all`、滚动 checksum、chunk 顺序重组、长度越界、checksum 错误、
     发送取消和重复安装测试。
   - [x] 大于单帧上限的真实 TCP 集成测试：`raft_snapshot_tcp_transport_handles_payload_larger_than_one_frame`。
   - [进行中] 断开连接后的传输重试和临时文件复用；当前已覆盖连接断开后的重新传输，
     尚未实现跨连接断点续传。
4. [x] 增加快照安装崩溃恢复测试，确认旧快照和旧状态不会被半成品替换。
   - 代码入口：`MemStateMachine::persistent`、`persist_raft_snapshot`、
     `load_persisted_raft_snapshot`、`install_snapshot`。
   - 测试：`raft_snapshot_persists_and_recovers_after_restart`、
     `raft_snapshot_persistence_rejects_corruption`、
     `failed_raft_snapshot_install_preserves_previous_state`。
5. [ ] 增加快照后重启、旧日志尾部冲突和状态机逐字段比较测试。
6. [ ] 根据日志大小、快照大小、磁盘空间和恢复耗时重新设定生产 snapshot policy。

## 完成标准

- 当前阶段：落后节点能安装 OpenRaft 快照并继续追赶，日志 purge 后重启不依赖已删除前缀。
- 未完成：值级 CAS 增量、断点续传、流式大快照的真实超单帧集成测试，以及生产 snapshot policy 调优。
- 最终完成标准仍为：
  - 快照状态机不会暴露中间状态；
  - 增量队列 CAS 重放后结果正确；
  - 多 Key 操作不因快照拆分而破坏原子性；
  - 快照失败不会替换最后一个有效快照；
  - 落后节点能安装快照并继续追赶；
  - 日志清理后重启不会出现旧尾部或状态机位置倒退。
