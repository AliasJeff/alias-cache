# 协议与一致性模型

## 1. RESP2

第一阶段实现 RESP2：

- Simple String
- Error
- Integer
- Bulk String
- Array

要求支持二进制安全的 Bulk String、分片读取、半包、粘包、流水线和最大帧限制。

## 2. RESP3

第二阶段增加 RESP3：

- Null
- Boolean
- Double
- Map
- Set
- Push
- Big Number
- Attribute

通过 `HELLO 3` 协商协议版本。命令执行层不应依赖具体编码格式，响应值先使用内部类型表达，再由 RESP 编码器转换。

## 3. 命令分类

每个命令都必须属于以下一种或多种类别：

- Local read：不修改状态且不需要复制。
- Consistent read：需要 Leader 或读屏障。
- Replicated write：必须进入 Raft 日志。
- Multi-key operation：需要原子访问多个 key。
- Transaction command：影响会话事务状态。
- Blocking command：需要等待后续状态变化。
- Pub/Sub command：改变连接订阅状态或发送推送。

分类必须影响执行路径和测试，而不是只作为注释存在。

## 4. 一致性语义

### 写入

写入成功表示该操作已经满足项目定义的 Raft 提交条件，并已经在 Leader 状态机中应用或具有可解释的应用结果。

### 读取

第一阶段只承诺通过 ReadIndex 或等价多数派读屏障确认后的 Leader 线性一致读。Follower 直接读不作为默认能力。

### 事务

事务中的命令必须在一个可重放的 Raft 操作中保持顺序和原子性。不能让每个子命令独立提交后再拼接响应。

### TTL

TTL 的过期时间必须作为日志数据或确定性派生结果复制。副本不能因为本地时钟不同而返回不同的复制结果。

完整的状态机、TTL、Raft/WAL 和快照契约见 [核心设计契约](core-design-contracts.md)。

## 5. 兼容性边界

每个命令文档需要说明：

- 支持的参数形式
- 错误响应
- RESP2 响应
- RESP3 响应
- 缺失 key 行为
- 类型错误行为
- TTL 继承规则
- 事务和 Pipeline 行为
- 是否支持二进制 key/value

不支持的命令必须返回稳定、明确的错误，而不是静默执行近似行为。

命令支持范围、版本基线、故障期间客户端行为和明确非目标见 [兼容性与故障模型](compatibility-and-failure-model.md)。
