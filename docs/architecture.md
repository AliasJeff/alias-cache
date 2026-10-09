# 架构设计

## 1. 分层

```text
Client
  |
RESP Listener
  |
Protocol Parser / Session
  |
Command Planner
  |
Consistency Layer
  |
Raft Core
  |
Replicated Log / WAL
  |
State Machine
  |
Snapshot / Recovery
```

### Protocol Layer

负责 RESP2/RESP3 编解码、连接状态、认证、Pipeline 和请求大小限制。

它不直接修改数据，也不决定 Raft 提交语义。

### Command Layer

负责参数解析、命令分类和 Redis 语义。命令执行必须生成可复制、可重放的操作。

### Consistency Layer

负责判断：

- 请求是否必须发送给 Leader。
- 读请求使用哪种一致性策略。
- 写请求何时可以返回客户端。
- 非 Leader 请求如何返回或转发。

### Raft Layer

负责 Leader 选举、日志复制、成员变化、提交位置和节点状态。

优先使用成熟 Raft 实现，项目重点放在存储适配、状态机和恢复语义，而不是重复实现共识算法。

### State Machine

只应用已经提交的日志。状态机必须满足：

- 相同日志前缀产生相同状态。
- 操作可序列化和反序列化。
- 操作可重放。
- 本地时间不会改变复制结果。

### Storage Layer

包括：

- Raft WAL
- 状态机快照
- 快照元数据
- 快照临时文件和原子发布
- 日志清理和恢复校验

核心模块之间的稳定契约见：

- [核心设计契约](core-design-contracts.md)
- [兼容性与故障模型](compatibility-and-failure-model.md)

## 2. 写入路径

```text
RESP request
  -> parse and validate
  -> reject if not Leader or forward
  -> assign replicated logical time
  -> append Raft log
  -> replicate to quorum
  -> apply committed entry
  -> return state-machine result
```

客户端收到成功响应的前提是项目定义的提交条件已经满足。不能在日志提交前修改本地业务状态，也不能先返回成功再异步复制。

## 3. 读取路径

第一版使用 ReadIndex 或等价的多数派读屏障实现线性一致读：

```text
RESP request
  -> confirm current Leader
  -> establish read barrier
  -> read committed state
  -> encode response
```

租约读只能作为后续优化，必须记录时钟漂移、租约失效、网络分区和 Leader 变更行为。

## 4. 快照路径

快照必须定义以下阶段：

1. 记录快照起点。
2. 生成状态机一致视图。
3. 记录快照期间需要保留的增量。
4. 写入临时文件。
5. flush 和 fsync。
6. 原子发布快照。
7. 清理已被快照覆盖的日志。

安装快照时：

1. 接收并校验临时文件。
2. 先写入本地安全位置。
3. 校验格式、版本和元数据。
4. 以明确的读锁或状态切换替换状态机。
5. 重放快照后的增量。
6. 发布为当前快照。
7. 恢复过期任务和正常服务。

任何步骤失败，都必须有可测试的恢复结果。

## 5. 时间模型

业务 TTL 使用复制日志中携带的逻辑时间和逻辑时间域中的绝对过期时间。具体约束见核心设计契约。

本地墙上时钟只用于：

- 网络超时
- Raft 选举计时
- 后台清理调度

不能使用本地墙上时钟直接决定复制命令的结果。

## 6. WAL、恢复和快照顺序

Raft 日志存储、状态机恢复和快照发布必须共同定义崩溃语义。至少需要区分：

1. 日志追加但未 fsync；
2. 日志 fsync 但尚未提交；
3. 日志提交但尚未 apply；
4. 状态机 apply 但响应尚未发送；
5. 快照临时文件完成但尚未发布；
6. 快照发布但旧日志尚未清理。

恢复时不能把未提交日志当作对外可见状态，也不能在损坏或版本未知时静默启动空数据库。

## 7. 关键不变量

- 只有已提交日志才能进入对外可见的复制状态。
- Follower 不得接受未经 Leader 提交的外部写入。
- 相同日志和相同初始状态必须得到相同结果。
- 快照元数据中的最后日志位置必须与状态机内容匹配。
- 任何成功响应都必须能在恢复路径中解释。
- 协议解析失败只能影响当前请求或连接，不能导致服务进程崩溃。
- Leader 失去多数派时不能返回成功写入。
- 快照失败时最后一个有效快照仍然可用。
- 物理过期清理不能删除逻辑上已经被新版本替换的值。
