# 11 数据结构、Pipeline 与事务扩展

## 目标

在核心复制、恢复、TTL 和路由稳定后，逐步增加常用 Redis 能力。每种结构都必须沿用同一状态机、日志和快照模型。

## 推荐顺序

1. Hash；
2. List；
3. Set；
4. Sorted Set；
5. Pipeline 完整语义；
6. MULTI/EXEC/DISCARD；
7. `WATCH` 或明确记录不支持；
8. Lua；
9. Pub/Sub 和阻塞命令；
10. Stream。

## 每种数据结构的设计任务

- 内部表示、类型标签和内存估算。
- 单 Key 读写和多 Key 操作分类。
- 迭代顺序和返回结果稳定性。
- 类型检查和错误文本。
- TTL 继承、覆盖和删除规则。
- 序列化格式和版本。
- 日志重放。
- 快照全量数据和增量 CAS。
- RESP2/RESP3 响应。
- 最大元素数和单 Key 大小限制。
- 大对象操作的超时和资源保护。

## 参考 cache-cat

- `cache_cat/src/raft/types/core/cache/string.rs`、`hash.rs`、`list.rs`、`set.rs`、`zset.rs`：参考各数据结构的内部表示和公共 cache facade。
- `cache_cat/src/raft/types/core/structure/sorted_set.rs`：参考 Sorted Set 的底层结构和排序语义。
- `cache_cat/src/protocol/hash/*.rs`、`list/*.rs`、`set/*.rs`、`zset/*.rs`、`string/*.rs`：按数据结构参考命令参数、类型错误、返回值和测试组织。
- `cache_cat/src/protocol/multi_compute_tests.rs`：重点参考跨 Key 计算、快照增量和 TTL 继承测试。
- `cache_cat/src/protocol/transaction/*.rs`、`transaction_tests.rs`：参考 MULTI/EXEC/DISCARD 的连接状态、命令入队和批量执行。
- `cache_cat/src/protocol/lua/*.rs`、`lua_env.rs`：仅作为 Lua 确定性、事务集成和资源限制的研究材料。
- `cache_cat/src/raft/types/core/structure/stream/*.rs`、`protocol/stream/*.rs`：仅在阻塞和 Stream 阶段参考数据结构、ID、恢复和唤醒逻辑。

## 实现顺序建议

每种数据结构都按“内部值类型 -> 单 Key 命令 -> 多 Key 命令 -> 日志重放 -> 快照 -> TTL -> RESP3 -> 多节点测试”的顺序完成，不要一次性从协议层堆叠全部命令。

## Pipeline

- 请求仍按输入顺序执行和返回。
- 每个写请求独立进入 Raft，除非明确属于事务批次。
- 定义 Leader 切换期间已经写入请求的状态。
- 限制 Pipeline 长度和累计响应大小。
- 测试半包、粘包、错误命令和混合读写。

## 事务

- `MULTI` 只改变连接状态，不修改复制状态。
- 入队阶段检查命令形状和参数。
- `EXEC` 将有序子操作编码成一个可复制操作。
- 子操作按固定顺序执行，外部不可观察中间状态。
- 运行时类型错误作为事务响应元素返回，不能使副本分歧。
- 事务中的 TTL 使用同一 write time。
- 明确 `WATCH` 是否支持以及其一致性实现。
- 限制事务命令数、嵌套和响应大小。

### 当前实现进度

- 已实现 Hash 状态类型和确定性 `BTreeMap` 表示：
  `HSET`、`HMSET`、`HSETNX`、`HDEL`、`HGET`、`HMGET`、`HGETALL`、
  `HKEYS`、`HVALS`、`HLEN`、`HEXISTS`。
- Hash 操作进入统一 `Operation`、Raft apply、事务和快照序列化路径；
  更新字段继承 key TTL，删除最后字段移除 key，String/Hash 类型错误返回稳定
  `WRONGTYPE`。
- `StateMachine` 测试覆盖重复 field、确定性枚举、重放序列化、TTL 保留、
  HSETNX 和错误类型。
- 已实现 List 状态类型和确定性 `VecDeque` 表示：
  `LPUSH`、`RPUSH`、`LPOP`、`RPOP`、`LINDEX`、`LRANGE`、`LLEN`、
  `LREM`、`LSET`、`LTRIM`。
- List 操作进入统一 `Operation`、Raft apply、事务和快照序列化路径；
  支持负索引、count 形式弹出、TTL 继承、空 key 清理、序列化重放和稳定
  `WRONGTYPE`。
- 已实现 Set 状态类型和确定性 `BTreeSet` 表示：
  `SADD`、`SREM`、`SMEMBERS`、`SISMEMBER`、`SCARD`、`SDIFF`、`SINTER`、
  `SUNION`。
- Set 写操作进入统一 `Operation`、Raft apply、事务和快照序列化路径；
  集合计算结果按字节序稳定返回，更新继承 TTL，删除最后 member 自动删除 key。
- `SPOP`、`SRANDMEMBER` 等随机命令暂缓到确定性随机种子和响应上限设计完成后实现。
- 已实现连接级 `MULTI`、`EXEC`、`DISCARD` 状态。
- 已实现 `Operation::Transaction`，多个写子命令作为一个 Raft 日志操作提交。
- 事务内子命令共享同一个逻辑写入时间，TTL 操作按入队顺序执行。
- 当前事务只接受 replicated write；读命令在 `EXEC` 时返回稳定错误，不进入 Raft。
- 已实现事务请求的 Leader 转发 RPC，避免 follower 将事务拆成多个写入。
- 当前限制：未实现 `WATCH`、事务内读命令和运行时错误元素；事务中命令形状错误会使 `EXEC` 整体失败。
- 参考实现：
  - `alias-cache/src/server.rs::handle_transaction_command`：连接状态和命令入队。
  - `alias-cache/src/state_machine.rs::prepare_transaction`：确定性子操作准备。
  - `alias-cache/src/raft.rs::execute_transaction_request`：单次 Raft 提交及 Leader 转发。
  - `cache_cat/src/protocol/transaction/*.rs`、`transaction_tests.rs`：连接状态、入队和响应组织。

## Lua、阻塞和 Stream 前置条件

只有满足以下条件才进入实现：

- 脚本执行确定性；
- 禁止或替换时间、随机数和外部 I/O；
- CPU、内存、递归和返回值有上限；
- 阻塞等待不会占用 Raft apply 线程；
- Push、唤醒和取消语义已定义；
- 快照和恢复可以重现相同结果。

## 完成标准

- 每个结构都有协议、状态机、重放和快照测试。
- 多节点、重启、Leader 切换和 TTL 场景通过。
- 事务在提交、恢复和快照安装后保持原子性。
- 不支持的高级功能返回稳定错误，不伪造兼容性。

## 完成记录

- 2026-10：String、Hash、List、Set、Sorted Set 五种结构已实现，均沿用统一的
  prepare/apply 路径：prepare 在当前状态上校验参数并生成可序列化 `Operation`，
  apply 重放且结果与 prepare 响应一致；写入继承 key TTL，删空即删 key。
- 确定性取舍：Set 用 `BTreeSet` 按字节序返回；Sorted Set 的 score 用 `i64`
  而非 double，避免浮点跨副本分歧；集合内乱序输入的返回顺序全部确定。
- 测试：`state_machine::tests` 覆盖每种结构的命令语义、重放、TTL 继承、
  WRONGTYPE 拒绝和 INCRBY 的溢出/非整数边界；兼容矩阵见
  `docs/compatibility-and-failure-model.md` 的各结构约束段。
- 待完成：数据结构命令的多节点 TCP 集成回归（当前多节点测试集中在 String/TTL）、
  `WATCH`、事务内读命令、HINCRBY/ZINCRBY、SCAN 系列。

- 2026-10（续）：`HINCRBY`/`ZINCRBY`、`SCAN`/`HSCAN`/`SSCAN`/`ZSCAN` 和
  `WATCH`/`UNWATCH` 已实现。WATCH 采用日志内 CAS：key 版本随状态机保存，
  事务携带 WATCH 时的版本快照，apply 时校验、不一致则整体 abort（null
  回复、子命令不执行）——检查与写入原子提交，无竞态窗口。
  状态机新增 `versions` map 与 `key_versions` 读取接口；WAL、本地快照和
  Raft 快照载荷版本升至 3，旧版本数据启动时明确拒绝。
  测试：状态机级 abort/replay 测试、RESP 连接级 WATCH→并发修改→EXEC
  null 回复→UNWATCH 后正常提交的全流程测试，及每命令分类护栏更新。
