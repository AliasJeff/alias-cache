# 兼容性与故障模型

## 1. Redis 兼容边界

alias-cache 的兼容目标是“对支持的命令提供可测试的 Redis 协议和语义”，不是一次性复制全部 Redis。

每个命令进入稳定支持列表前必须记录：

- 目标 Redis 版本；
- 参数形式和默认值；
- RESP2 返回；
- RESP3 返回；
- 二进制安全要求；
- 缺失 Key 行为；
- 类型错误；
- TTL 继承；
- Pipeline 行为；
- MULTI/EXEC 行为；
- 是否需要 ReadIndex；
- 是否进入 Raft；
- 快照和重放规则；
- 已知差异。

第一版建议支持：

- 连接：`PING`、`ECHO`、`QUIT`、`AUTH`、`SELECT`、`HELLO`；
- 字符串：`GET`、`SET`、`DEL`、`EXISTS`、`MGET`、`MSET`、`INCRBY`；
- Hash：`HSET`、`HMSET`、`HSETNX`、`HDEL`、`HGET`、`HMGET`、`HGETALL`、
  `HKEYS`、`HVALS`、`HLEN`、`HEXISTS`；
- Key/TTL：`TYPE`、`DBSIZE`、`EXPIRE`、`PEXPIRE`、`TTL`、`PTTL`、`PERSIST`；
- 事务：`MULTI`、`EXEC`、`DISCARD`；
- Pipeline：复用普通请求顺序，不引入额外语义。

第一版明确不承诺：

- Redis Cluster 分片；
- 完整 Sentinel 故障判断模型；
- Lua；
- Pub/Sub；
- 阻塞命令；
- Stream；
- LRU/LFU 淘汰；
- Redis RDB/AOF 文件兼容。

Hash 当前约束：

- Hash field/value 使用二进制安全的 bulk string；
- `HGETALL` 返回 RESP3 Map，RESP2 客户端按数组降级；
- `HKEYS`、`HVALS` 和 `HMGET` 保持确定性顺序或请求顺序；
- Hash 写入继承 key 的 TTL，删除最后一个 field 后删除整个 key；
- `HINCRBY`、`HSCAN` 和 Hash 过期策略扩展尚未纳入稳定兼容范围。

Sentinel 风格命令只能作为后续客户端发现适配层，不能成为 Raft 状态来源。

## 2. 一致性和可用性声明

alias-cache 的默认语义：

- 已成功响应的写入已经满足 Raft 提交条件；
- Leader 线性一致读不会读取未提交状态；
- 少数节点故障时，只要多数派仍然可通信，集群可以继续提交；
- 少数派分区不能安全提交写入；
- Follower 不默认提供强一致读；
- Leader 失去多数派时，写请求必须失败或等待，不能返回成功；
- 已提交日志在可恢复存储正常的前提下，节点重启后必须可恢复。

以下情况不属于系统承诺：

- 磁盘介质已经丢失且没有备份；
- 集群所有节点同时损坏；
- 客户端在响应丢失后要求系统判断请求是否已经执行；
- 不受限制的内存增长；
- 与 Redis 未支持命令完全一致。

## 3. 客户端路由

非 Leader 请求必须返回可区分的错误类型：

- `NOLEADER`：当前没有可确认的 Leader；
- `TRYAGAIN`：Leader 正在切换或请求需要重试；
- `MOVED` 或等价错误：已知 Leader 地址；
- `READONLY`：当前节点不接受该类请求。

服务端转发如果启用，必须：

- 携带原始请求 ID；
- 限制最大转发跳数；
- 处理 Leader 变化；
- 不因超时自动重复不可幂等的写请求；
- 记录转发延迟和最终结果。

## 4. 故障模型

测试和文档统一使用以下故障类型：

### 节点故障

- Leader 立即退出；
- Follower 立即退出；
- 节点在日志追加后退出；
- 节点在 fsync 前退出；
- 节点在提交后、响应前退出；
- 节点在快照发布前退出。

### 网络故障

- 单向丢包；
- 双向丢包；
- 固定延迟；
- 随机延迟；
- 重复消息；
- 网络分区和分区恢复；
- Leader RPC 连接建立失败。

### 存储故障

- 日志尾部部分写入；
- 日志中间损坏；
- 日志截断失败；
- 快照校验失败；
- 快照传输中断；
- 磁盘空间不足；
- fsync 返回错误；
- 旧快照和新快照同时存在。

每个故障测试都必须说明：

- 故障发生点；
- 允许的客户端错误；
- 不允许出现的状态；
- 恢复条件；
- 恢复后需要验证的日志、状态机和客户端结果。

## 5. 成功响应和重复请求

客户端可能在服务端提交成功后、响应到达前断开连接。第一版可以不提供 Redis 风格的请求去重，但必须：

- 在文档中明确“超时后写入状态未知”；
- 不在服务端盲目自动重试不可幂等操作；
- 为后续 client request id、幂等 token 或事务序列号预留扩展位置；
- 对 `SET`、`DEL` 等幂等命令和 `INCR` 等非幂等命令分别测试。

## 6. 发布兼容矩阵

兼容矩阵至少要按以下维度记录：

- Redis 目标版本；
- 命令和子命令；
- RESP2/RESP3 返回类型；
- 单 Key/多 Key/事务属性；
- 是否进入 Raft；
- 是否需要 Leader；
- TTL 继承规则；
- 资源限制；
- 快照/重放支持；
- 已知差异和测试文件。

未进入矩阵的命令不属于稳定兼容范围。
