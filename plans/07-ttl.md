# 07 TTL、逻辑时钟与过期清理

## 目标

实现 Redis TTL 语义，并保证主副本、重放、快照和 Leader 切换中的判断一致。

## 实现内容

- 当前阶段已实现 `EXPIRE`、`PEXPIRE`、`TTL`、`PTTL`、`PERSIST`，以及 `SET EX/EXAT/PX/PXAT/NX/XX/KEEPTTL/GET` 的基础语义。
- 当前阶段使用 `SetWithExpiration`、`Expire`、`Persist` 和 `AdvanceTime` 作为可序列化 Raft 操作，过期时间和逻辑时间随日志复制。
- 实现 `EXPIRE`、`PEXPIRE`、`TTL`、`PTTL`、`PERSIST`。
- 后续按兼容矩阵实现 `SET EX/NX/PX/KEEPTTL`。
- 定义逻辑时间、读时钟、写时钟和本地物理时钟。
- 写日志携带确定的 write time。
- 使用绝对逻辑过期时间保存 TTL。
- 使用 `now >= expire_at` 作为统一过期条件。
- 实现惰性读取过期和后台物理清理。
- 使用时间轮或等价结构调度物理清理。
- 清理 worker 只能删除当前版本仍匹配的值。
- 定义空闲期间的逻辑时间推进策略。
- 定义快照、恢复和远程安装时的过期 worker 协调。
- 定义溢出、负数、零 TTL、秒毫秒转换和舍入规则。

## 参考 cache-cat

### 当前实现对应关系

- `alias-cache/src/state_machine.rs::Operation::SetWithExpiration`、`Expire`、`Persist`、`AdvanceTime`：
  对应 `cache_cat/src/raft/types/entry/base_operation.rs`、`read_operation.rs` 的可重放时间/过期操作。当前采用更保守的方式：时间推进也必须进入 Raft 日志。
- `alias-cache/src/state_machine.rs::StateMachine::prepare_request_at`：
  对应 `cache_cat/src/raft/types/core/mocha/core.rs` 的统一逻辑时间入口；调用层注入时间，状态机不读取本地墙上时钟。
- `alias-cache/src/raft.rs::execute_client_request`、`advance_raft_time`、`run_leader_time_worker`：
  对应 `cache_cat/src/node/raft_builder.rs` 的 Leader 时间推进任务；当前由服务生命周期启动可取消的 Leader-only 周期 worker。
- `alias-cache/src/state_machine.rs::snapshot`、`Entry::expire_at`：
  对应 `cache_cat/src/mocha/mod.rs` 的绝对过期时间和惰性可见性判断；当前使用扫描式候选枚举，不引入时间轮，清理仍通过 Raft 操作复制。
- `alias-cache/src/raft.rs::tests::tcp_cluster_replicates_ttl_expiration`：
  对应 `cache_cat/src/mocha/test.rs` 和集群测试，验证同一 TTL 操作在三节点重放后由同一逻辑时间判断过期。
- `cache_cat/src/protocol/key/expire.rs`、`pexpire.rs`、`ttl.rs`、`pttl.rs`、`persist.rs`：
  参考 Redis 返回值、过期条件和秒级舍入；当前已覆盖 EXPIRE/PEXPIRE/TTL/PTTL/PERSIST 的基础路径。
- `cache_cat/src/protocol/string/set.rs`：
  参考 `SET` 的 EX/EXAT/PX/PXAT、NX/XX、KEEPTTL、GET 选项解析和互斥规则；当前仍需补齐全部选项排列组合的错误响应矩阵。
- `cache_cat/src/mocha/test.rs`、`docs/distributed_delete_en.md`、`distributed_delete_zh.md`：
  参考时钟单调性、过期边界、读写竞态和故障切换推导；不直接复制其 lease-read 假设。

## 重点借鉴

cache-cat 的逻辑时钟设计依赖明确的时钟偏移假设；alias-cache 若使用更保守的 ReadIndex 读，也必须保留“复制写入不能依赖本地墙上时钟”的核心原则。

## 必须测试的 Redis 语义

### 已完成

- [x] PTTL 不存在 Key 返回 `-2`。
- [x] PTTL 存在但无 TTL 返回 `-1`。
- [x] `now >= expire_at` 的惰性过期判断。
- [x] 已过期 Key 的 GET、EXISTS、DEL、DBSIZE 和 TYPE 可见性。
- [x] PERSIST 对无 TTL、已过期和不存在 Key 的行为。
- [x] TTL 操作在三节点 Raft 重放后保持一致。

### 后续执行步骤

1. [x] 增加 `EXPIRE`、`TTL`，明确剩余毫秒到秒的四舍五入和边界值。
   - 代码入口：`alias-cache/src/state_machine.rs::execute_expire`、`execute_ttl`。
   - 参考：`cache_cat/src/protocol/key/expire.rs`、`ttl.rs`。
   - 当前规则：`now >= expire_at` 视为过期，剩余毫秒按最近秒数四舍五入。
2. [x] 完成 `SET EX/EXAT/PX/PXAT/NX/XX/KEEPTTL/GET`，定义覆盖写的 TTL 继承规则。
   - 代码入口：`StateMachine::execute_set`、`Operation::SetWithExpiration`。
   - 参考：`cache_cat/src/protocol/string/set.rs` 和 `cache_cat/src/raft/types/entry/base_operation.rs`。
   - 当前规则：普通覆盖清除 TTL，`KEEPTTL` 只继承仍有效的 TTL；`NX`/`XX` 条件失败不写入 Raft；`GET` 返回覆盖前的旧值；`EXAT/PXAT` 使用当前复制逻辑时间解释为绝对毫秒截止时间。
3. [x] 修正非正 `EXPIRE`/`PEXPIRE` 的边界语义。
   - 代码入口：`execute_expire`、`execute_pexpire`、`Operation::ExpireSeconds`、`Operation::Expire`。
   - 当前规则：存在且未过期的 Key 使用 `expire_at = logical_time`，返回 `1`，随后惰性读取不可见；不存在或已过期返回 `0`。
   - 参考：`cache_cat/src/protocol/key/expire.rs`、`pexpire.rs`。
4. [x] 将 TTL 元数据和逻辑时间纳入本地 snapshot、WAL 和 Raft snapshot 恢复测试。
   - 本地快照：`alias-cache/src/snapshot.rs::preserves_ttl_metadata_and_logical_time`。
   - WAL：`alias-cache/src/wal.rs::replays_ttl_metadata_and_logical_time`。
   - Raft：`alias-cache/src/raft.rs::memory_state_machine_applies_operations_and_snapshots`。
   - 格式边界：WAL 与本地 snapshot 版本提升至 `2`，旧版本明确拒绝，避免新旧 `Operation`/`StateMachine` 载荷静默误读。
5. [x] 将 `AdvanceTime` 接入 Leader 周期任务，避免业务必须显式触发时间推进。
   - 目标模块：`alias-cache/src/raft.rs`、服务启动/停止生命周期模块。
   - 参考：`cache_cat/src/node/raft_builder.rs` 的 Leader-only 时间推进任务。
   - 实现：`run_leader_time_worker` 每个周期检查 `current_leader()`；只由当前 Leader 提交 `AdvanceTime`，监听服务 shutdown。
   - 测试：`raft::tests::leader_time_worker_advances_only_on_leader_and_stops`。
6. [x] 实现版本保护的物理清理 worker，过期任务不能删除新版本值。
   - 实现：新增 `Operation::DeleteIfExpired { key, expire_at }`，应用时要求当前条目过期时间完全匹配且逻辑时间已到期。
   - 目标模块：`state_machine.rs::expired_entries`、`raft.rs::run_expiration_worker`。
   - 参考：`cache_cat/src/mocha/mod.rs` 的过期删除检查，以及 `docs/distributed_delete_zh.md` 的分布式删除推导。
   - 实现：旧清理任务遇到同 Key 新写入、TTL 更新或 `PERSIST` 时 no-op；清理操作经过 Raft 复制。
   - 测试：`state_machine::tests::stale_expiration_cleanup_cannot_delete_replaced_or_persisted_value`、`raft::tests::expiration_worker_removes_expired_key_after_replicated_time_advance`。
7. [进行中] 增加 Pipeline、事务、Leader 切换和重启场景下的 TTL 测试。
   - [x] Pipeline：`server::tests::foundation_server_preserves_ttl_pipeline_responses` 验证 SET/ PTTL/EXISTS 的响应顺序。
   - [x] 事务：`server::tests::single_connection_executes_ttl_transaction_atomically` 验证事务内 SET/PEXPIRE/PERSIST 作为单次提交执行；事务内读命令暂不支持。
   - [x] Leader 切换：`raft::tests::committed_expired_key_does_not_reappear_after_leader_failover` 验证已提交过期 Key 不因切主重新出现。
   - [ ] 重启：已有普通写入重启测试，仍需增加 TTL 写入、逻辑时间和过期清理后的文件 Raft 重启场景。
8. [ ] 补充完整兼容矩阵和持久化迁移策略。
   - 待覆盖命令：`SET` 选项的全部顺序组合、完整错误响应，以及 `EXPIRE`/`PEXPIRE` 的协议边界。
   - 待定义格式：应用状态版本、WAL 版本、Raft snapshot 载荷版本分别演进；不兼容版本必须启动失败并提供可诊断错误。
   - 参考：`cache_cat/src/raft/types/entry/base_operation.rs`、`read_operation.rs`，以及 `cache_cat/src/raft/types/core/mocha/core.rs`。

## 一致性测试

- 相同日志在不同初始物理时间的副本上得到相同业务结果。
- 主节点和副本逻辑时钟单调不减。
- Leader 切换后已判定过期的 Key 不重新出现。
- 快照恢复不会延长或缩短 TTL。
- 物理清理与读写并发不会删除新版本值。
- 空闲节点通过时间推进后仍能清理确定过期数据。

## 完成标准

- 当前阶段：TTL 过期时间和逻辑时间通过 Raft operation 复制，三节点重放结果一致；基础 Redis TTL 命令和 SET 选项已覆盖。
- 已完成：非正 TTL 边界、TTL 元数据恢复测试，以及 WAL/本地 snapshot 的不兼容版本拒绝。
- 未完成：事务内读命令和 WATCH、完整 SET/EXPIRE 条件矩阵、Raft snapshot 载荷独立版本和旧数据迁移工具。
- 最终完成标准仍为：
  - TTL 命令兼容矩阵中的边界全部通过；
  - TTL 逻辑在日志重放和快照恢复后保持一致；
  - 本地墙上时钟只用于调度，不决定复制结果；
  - 过期 worker 暂停或失败不会破坏业务正确性。

### 完成记录（2026-10 收尾）

- SET 全选项排列兼容矩阵测试已落地（`set_option_matrix_and_expire_boundaries`）：
  表驱动覆盖 NX/XX/KEEPTTL/GET/EX/EXAT/PX/PXAT 的全部拒绝组合（互斥、双
  TTL、双 GET、非正/非整数/溢出 TTL）与合法组合（NX 新旧 key、XX、绝对/
  相对过期、KEEPTTL 继承、GET 返回旧值、三选项连用），并断言每个失败 SET
  在状态机上零残留；EXPIRE/PEXPIRE 协议边界（缺失 key、已过期 key 返回 0、
  非正 TTL 立即过期、非整数参数协议错误、秒→毫秒精确转换）一并覆盖。
- 迁移策略文档已交付：`docs/storage-format-migration.md` 定义 WAL/本地
  快照/Raft 快照载荷三个独立版本号的演进规则（只拒绝不猜测、随结构变更
  递增、派生数据恢复后重算）与 0.x 升级路径（备份 + SCAN 导出 + 重建 +
  回灌），三个版本拒绝测试作为门禁存在。
