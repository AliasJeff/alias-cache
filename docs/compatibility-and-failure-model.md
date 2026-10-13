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
- 只读配置探测：`CONFIG GET save|appendonly|*`；`save` 和 `appendonly` 分别
  返回空字符串和 `no`，表示没有 Redis RDB/AOF 配置。alias-cache 的写入
  持久化通过独立的 Raft WAL 完成，不表示 Redis AOF/RDB 文件兼容；所有
  `CONFIG` 修改命令均拒绝，快照频率由 `--snapshot-policy` 控制；
- 字符串：`GET`、`SET`、`DEL`、`EXISTS`、`MGET`、`MSET`、`INCRBY`；
- Hash：`HSET`、`HMSET`、`HSETNX`、`HINCRBY`、`HDEL`、`HGET`、`HMGET`、
  `HGETALL`、`HKEYS`、`HVALS`、`HLEN`、`HEXISTS`；
- List：`LPUSH`、`RPUSH`、`LPOP`、`RPOP`、`LINDEX`、`LRANGE`、`LLEN`、
  `LREM`、`LSET`、`LTRIM`；
- Set：`SADD`、`SREM`、`SMEMBERS`、`SISMEMBER`、`SCARD`、`SDIFF`、
  `SINTER`、`SUNION`；
- Sorted Set：`ZADD`、`ZINCRBY`、`ZREM`、`ZSCORE`、`ZCARD`、`ZRANGE`、
  `ZREVRANGE`、`ZRANK`、`ZREVRANK`；
- Key/TTL：`TYPE`、`DBSIZE`、`EXPIRE`、`PEXPIRE`、`TTL`、`PTTL`、`PERSIST`；
- 遍历：`SCAN`、`HSCAN`、`SSCAN`、`ZSCAN`（游标 + `COUNT`）；
- 可观测：`INFO [health|stats|server|replication|cluster]`；health/server/
  replication/cluster 取自单一 openraft metrics 快照；stats 为运行指标
  （连接计数与上限、内存用量与上限、raft 日志/快照文件字节大小、命令
  计数、WAL append 批次大小、sync_data 次数及耗时），其中计数器为进程内
  原子量、文件大小按路径即时读取，不进复制状态机；WAL 指标仅统计
  `FileLogStore::append` 的成功追加批次和成功返回的 `sync_data` 调用，不包括
  vote、日志重写/清理、目录同步或失败的 sync；平均批次大小为成功 append
  entry 数除以成功非空 append 调用数，在线读取各原子计数器并非同一时刻的事务快照；
  fsync 耗时是本机调用耗时而非跨节点提交延迟；未知 section 返回空
  body；RESP3 为 VerbatimString，RESP2 降级
  bulk string（保留 `txt:` 格式标签）；`replication_lag_node_<id>` 仅在
  Leader 上报告（-1 表示未知）；
- 健康检查四级：`INFO health` 输出 `alive`（进程可响应）、`raft_recovered`
  （openraft 运行态正常）、`readable`（存在 Leader，读可经转发线性化）、
  `writable`（本节点即 Leader）。`writable` 是节点自报的快速信号：quorum
  丢失时写操作会实际失败，写可用性的最终判定以真实写入为准；K8s 探针组合：
  liveness=TCP+PING，readiness=INFO health 的 readable；
- 事务：`MULTI`、`EXEC`、`DISCARD`、`WATCH`、`UNWATCH`；
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

事务与 WATCH 当前约束：

- 事务仍只接受 replicated write 子命令；`WATCH` 在 MULTI 之外对任意 key
  建立乐观锁，`UNWATCH`/`DISCARD` 清除全部 watch；
- watch 的版本检查随事务进入 Raft 日志，在 apply 时校验：任一被 watch 的
  key 的版本与 WATCH 时不同（包括从不存在到存在），整个事务 abort 并返回
  null 回复，队列内子命令全部不执行——检查与写入在所有副本上原子生效，
  没有检查-提交竞态窗口；
- key 版本是每次成功修改自增的计数器，key 删除后回到"不存在"态；
- 事务内的读命令、事务嵌套仍不受支持，返回稳定错误。

资源限制当前约束：

- `--max-memory`（字节）：按增量计数器估算的活跃数据载荷（不含树结构
  开销）超限时拒绝新写入，回复 `OOM command not allowed when used memory >
  'maxmemory'`；替换写按新数据全额保守计费；读与删除不受限；
- `--max-connections`：超限的新连接收到 `-ERR max number of clients
  reached` 后立即关闭；
- 两项缺省 0（不限）；限制在重启后由配置恢复，不进复制状态机；
- `--snapshot-policy`：追加多少条日志后生成 raft 快照，缺省 64，0 回落默认。

Bitmap 当前约束：

- `SETBIT`/`GETBIT`/`BITCOUNT`/`BITPOS`/`BITOP` 操作 `Value::String`（位
  数组即字节串），MSB-first；SETBIT 越界以 0 字节扩展，offset 上限 2^32；
- BITOP 支持 AND/OR/XOR（≥2 源）与 NOT（恰 1 源），源短于最长者按 0
  填充，源与目标必须不同；BITOP 写入不继承 TTL（Redis 同）；
- 全零结果删除 key；GETBIT 越界返回 0；BITPOS 0 在全 1 缓冲返回首个
  越界位（Redis quirk）；String 类型的 key 可直接位寻址；
- `BITFIELD`、`BITCOUNT` 的 BIT 位模式、`BITOP` 的耗时保护尚未纳入
  稳定兼容范围。

Bloom Filter 当前约束：

- `BF.RESERVE key error_rate capacity`（0 < error_rate < 1，capacity > 0；
  重复保留返回 `ERR item exists`）、`BF.ADD`（未保留的 key 隐式按默认
  容量 1024/1% 创建）、`BF.EXISTS`、`BF.INFO`；
- **确定性 hash**：双重 FNV-1a 64 派生 `h_i = (h1 + i·h2) mod m`——
  无随机盐，同输入在所有副本产生相同位索引，这是复制一致性的关键；
- ADD 返回 1（本实现不区分 may-exist 与新建）；容量写满后继续写入
  不扩容（错误率超出声明值，RedisBloom noScale 行为）；TYPE 返回
  `MBbloom--`；
- `BF.MADD`/`BF.MEXISTS`/`BF.SCANDUMP`/`BF.LOADCHUNK` 尚未纳入稳定
  兼容范围。

Stream 当前约束（非阻塞骨干）：

- `XADD key [NOMKSTREAM 裁剪] id field value...`：自动 ID（`*`）由复制
  逻辑时钟毫秒 + 严格递增序号生成——**同日志重放产生相同 ID 序列**（
  跨副本确定性的核心）；显式 ID 必须严格大于流顶部 ID；NOMKSTREAM 标记
  当前不生效（首版裁剪）；
- `XLEN`、`XRANGE - + [COUNT n]`（含端点、升序、每条目 `[id, [f, v...]]`
  嵌套形状）、`XDEL`（缺失 ID 计 0）、`XTRIM key MAXLEN n`（从最旧端裁
  剪）；阻塞读、消费者组（XREADGROUP/XACK）、XINFO 尚未纳入稳定兼容
  范围；
- 空流删除 key；XDEL 不回退 last_id；TYPE 返回 `stream`。

Lua 脚本当前约束：

- `EVAL script numkeys key... arg...`：脚本在 Leader 上执行一次，
  `redis.call`/`redis.pcall` 产生的写作为确定性 Operation 进入一个 Raft
  事务——副本只重放操作、不执行脚本，`math.random`/`os.time` 等
  非确定性只在 Leader 侧生效一次，不影响一致性；
- 沙箱：64 MiB 内存上限、每 1000 指令的中断钩子 + 5 秒执行预算、
  io/os/require/loadfile 全局被移除；`redis.call` 首版仅接受写命令
  （脚本内读返回错误，读语义与 MULTI/EXEC 相同）；
- `EVALSHA` 返回 `NOSCRIPT`（脚本缓存未实现）、`SCRIPT` 返回不支持
  错误——客户端回退 EVAL 即可；脚本写超过批量上限（1024 条）返回
  稳定错误。

遍历当前约束：

- 游标是已返回成员数的十进制文本，语义为快照内偏移：同一批数据反复迭代
  得到确定顺序（key/field/member 字节序），游标遍历完快照后归零；
- `SCAN` 只返回未过期 key；`ZSCAN` 每成员返回 member/score 两帧；
- `COUNT` 缺省 10，必须为正；`MATCH`、`TYPE`、`NOVALUES` 等其余选项
  尚未纳入稳定兼容范围；
- 游标不是数据库版本号：遍历期间的写入不保证可见或不可见（与 Redis 一致）。

String 当前约束：

- `INCRBY` 使用 `i64` 饱和算术：目标值必须是可解析的 64 位整数，否则返回
  `value is not an integer or out of range`；加减溢出同样返回该错误；
- `INCRBY` 保持 key 的既有 TTL；对不存在的 key 从 0 开始计数；
- Hash/List/Set/Sorted Set 类型的 key 上执行返回 WRONGTYPE；
- `INCR`、`DECR`、`INCRBYFLOAT`、`APPEND`、`STRLEN` 尚未纳入稳定兼容范围。

Hash 当前约束：

- Hash field/value 使用二进制安全的 bulk string；
- `HGETALL` 返回 RESP3 Map，RESP2 客户端按数组降级；
- `HKEYS`、`HVALS` 和 `HMGET` 保持确定性顺序或请求顺序；
- Hash 写入继承 key 的 TTL，删除最后一个 field 后删除整个 key；
- `HINCRBY` 使用 `i64` 饱和算术：field 值必须是可解析的 64 位整数，否则
  返回 `hash value is not an integer`；加减溢出返回
  `increment or decrement would overflow`；对不存在的 field 从 0 开始；
  保持 key 的既有 TTL；
- Hash 过期策略扩展尚未纳入稳定兼容范围。

List 当前约束：

- List element 使用二进制安全的 bulk string；
- `LRANGE`、`LINDEX`、`LSET` 和 `LTRIM` 支持负索引；
- `LPOP/RPOP` 的 count 形式返回数组，无 count 时返回单值或 Null；
- List 写入继承 key 的 TTL，删除最后一个元素后删除整个 key；
- `BLPOP`、`BRPOP`、`LMOVE` 和 List blocking 语义尚未纳入稳定兼容范围。

Set 当前约束：

- Set member 使用二进制安全的 bulk string；
- 内部使用确定性有序集合，集合返回结果按字节序排列；
- `SDIFF`、`SINTER`、`SUNION` 支持多 Key 只读计算，并检查所有存在 Key 的类型；
- Set 写入继承 key 的 TTL，删除最后一个 member 后删除整个 key；
- `SPOP`、`SRANDMEMBER` 和 `*STORE` 写入变体尚未纳入稳定兼容范围。

Sorted Set 当前约束：

- Sorted Set member 使用二进制安全的 bulk string；
- score 接受任意有限 double 文本，内部以 f64 位模式存储（无损往返），
  排序用 IEEE 754 `total_cmp` 全序。非有限 score（NaN/inf）返回稳定错误；
  位模式与全序比较都是确定性的，跨副本重放不会分歧；
- `ZADD` 返回新增 member 数，重复 member 更新 score 不计数；
- 同分 member 按 member 字节序稳定排序，`ZRANK`/`ZREVRANK` 与之一致；
- `ZRANGE`/`ZREVRANGE` 支持负索引和 `WITHSCORES`；
- Sorted Set 写入继承 key 的 TTL，删除最后一个 member 后删除整个 key；
- `ZINCRBY` 沿用 ZADD 的 score 语义（f64，以位模式确定性存储），member 不
  存在时先按 increment 添加，返回新 score 的 bulk string；结果非有限时返回
  溢出错误；
- `ZRANGEBYSCORE`、`ZREMRANGEBY*` 和 `WITHSCORES` 之外的
  选项（`GT`/`LT`/`CH`/`INCR`）尚未纳入稳定兼容范围。

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
