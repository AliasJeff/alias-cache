# alias-cache

用 Rust 写的 Redis 兼容 KV 存储。单 raft 组强一致复制，线性一致读，
WAL 用 TiKV 的 raft-engine。

起因很简单：想要一个"写进去就一定在"的缓存。Redis 的主从复制是异步
的，failover 有丢失窗口；而我也不需要 Redis 那样的命令覆盖面。所以
这个项目的取舍从一开始就是：**一致性语义优先，命令面够用就好**。

它兼容 Redis 协议（RESP2/RESP3），绝大多数 Redis 客户端可以直接连，
但它不是 Redis，也不打算成为 Redis。下面会说清楚它是什么、能干什么、
不能干什么。

## 快速开始

```bash
cargo build --release
./target/release/alias-cache-cli --node-id 1
```

另开一个终端：

```bash
redis-cli -p 6379 SET hello world
redis-cli -p 6379 GET hello
```

三节点集群（节点 1 是种子）：

```bash
for i in 1 2 3; do
  peers=""
  for j in 1 2 3; do
    peers="$peers --peer $j=127.0.0.1:1637$j/127.0.0.1:637$j"
  done
  extra=""
  [ "$i" = "1" ] && extra="--bootstrap"
  ./target/release/alias-cache-cli --node-id $i \
    --client-addr 127.0.0.1:637$i --raft-addr 127.0.0.1:1637$i \
    --data-dir data/node-$i --snapshot-policy 65536 $peers $extra &
done
```

写在任意节点都会转发给 Leader 过多数派；读任意节点默认线性一致
（follower 读转发 Leader）。杀掉 Leader，剩下的节点会选出新 Leader
继续服务。`SENTINEL GET-MASTER-ADDR-BY-NAME` 可以发现当前 Leader——
所以哨兵式接入也能用。

Docker 的话：

```bash
docker compose up --build --detach
```

## 能做什么

- **协议**：RESP2/RESP3（`HELLO` 协商）、pipeline、inline 命令
- **数据结构**：String（SET 全选项矩阵：EX/PX/EXAT/PXAT/NX/XX/KEEPTTL/GET）、
  Hash、List、Set、Sorted Set、Bitmap（含 BITOP 五种位运算）、
  Stream（XADD/XRANGE/XDEL/XTRIM，自动 ID 派生自复制逻辑时钟）、
  Bloom Filter（确定性双 FNV-1a，跨副本一致）
- **过期**：SET EX/PX、EXPIRE/PEXPIRE/PERSIST；内部用 8 级层级时间轮，
  10 万个键同时过期只花一条 raft 日志（见下文）
- **事务**：MULTI/EXEC/DISCARD + WATCH 乐观锁。版本号放在复制状态机里，
  CAS 校验和写入在同一条日志内原子完成，不存在"检查完被改"的窗口
- **Lua**：EVAL，leader 执行、结果作为确定性操作复制（副本永不执行脚本）；
  mlua 沙箱限内存 + 指令中断 + 5s 预算
- **安全**：密码认证（AUTH/HELLO AUTH）、客户端 TLS、raft 复制 mTLS
- **运维**：INFO 五段（health/stats/replication/cluster/server）、
  Sentinel 风格主从发现、`--max-memory` OOM 拒写、`--max-connections`、
  Dockerfile + docker-compose 三节点编排

## 性能

Apple Silicon 10 核笔记本，单机跑 3 节点集群 + 压测客户端。
512 连接 × pipeline 16，40 万键，16B value，全部零错误。

| 负载 | 吞吐 |
| --- | ---: |
| 95% 读 / 5% 写 | ~40 万 ops/s |
| 50% 读 / 50% 写 | ~17 万 ops/s（连续 6 轮无衰减） |
| 5% 读 / 95% 写 | ~18 万 ops/s |
| 纯写 | ~20 万 ops/s |
| 纯读（线性一致 GET） | ~37 万 ops/s |

读为什么能这么快：默认走租约读（lease read），leader 在租约有效期内
直接读本地状态机，不付共识往返；租约计时器失效时回退 ReadIndex，
分区时 fail-closed。所以"快"没有牺牲线性一致性。

写为什么到不了 Redis：每笔写要过 leader 的 raft-engine fsync + 复制到
两个 follower 的网络往返 + 多数派确认。这是强一致复制的物理代价，
另一个 Redis 是异步复制，failover 会丢数据——两者承诺的不是同一种东西。

几个值得一提的数字：

- WAL（raft-engine）fsync 均值 ~0.3ms，且不随文件涨到几个 GB 而变慢
- 10 万个键同一秒过期，回收它们只写 **1 条** raft 日志（8 级层级时间轮
  做到期发现，apply 期批量 unlink——过期键的物理删除基本不占共识带宽）
- 增量快照：base + delta 段，快照构建是字节拼接不是全量序列化，
  崩溃恢复从 16 分钟降到秒级

## 值得一看的实现

- **管线化 raft 复制**：不实现 openraft 默认的"发一条等一条"，每个复制
  流一条持久连接，4 个在途 AppendEntries 按序应答。仅此一项纯写 +14.5%
- **提交合批**：leader 把所有连接的 pipeline 写合并成组提交，条数达标、
  20ms 上限、**队列静默 4ms 提前冲刷**三个条件任一触发。最后一 条是
  混合负载的关键——读在 pipeline 里必须等前面的写提交（Redis 顺序
  语义），写突发一停就提前提交，不让读陪等
- **时间轮过期**：如上。惰性判定保证正确性，时间轮保证发现成本 O(1)
  摊销，物理 unlink 与共识解耦
- **增量快照**：base 段 + delta 追加段，compaction 限频重建

## 做不了 / 没做

说实话比装体面重要：

- 单 raft 组，**没有分片**。想要 Redis Cluster 那种水平扩展，这里没有
- 没有 Pub/Sub、没有 BLPOP 这类阻塞命令、没有 HyperLogLog
- MULTI 里面只能放"会被复制执行"的写命令，GET 会被拒——事务是单条
  日志，读不进日志
- KEYS、RENAME、ZRANGEBYSCORE、GETRANGE/SETRANGE、XREAD 这些还没写
- SELECT 接受但只有一个库
- 每条写的 CPU 开销比极简实现高：RESP 解析、幂等去重、内存计费都在
  热路径上。这是买 WATCH 语义和重试安全的钱

## 测试

```bash
cargo test --workspace --locked
cargo clippy --workspace --all-targets -- -D warnings
```

181 个测试：命令兼容矩阵、WATCH 竞态、TTL 跨重启、分区 fail-closed、
旧 Leader 冲突、快照安装、WAL 撕尾恢复、成员变更、时间轮语义。
存储层跑在 openraft 官方 StorageContract 套件上。

真实三节点故障切换 smoke test（起集群、写数据、杀 Leader、验证新
Leader、重启旧节点验证追赶）：

```bash
python3 scripts/smoke_cluster.py
```

压测工具在仓库里：

```bash
cargo build --release
./target/release/alias-cache-benchmark \
  --address 127.0.0.1:9401 --mode pwrite \
  --clients 512 --pipeline 16 --key-space 400000 \
  --value-size 16 --time 30 --write-percent 50
```

本文所有数字都出自这个工具，条件如上，笔记本上跑的，换机器别迷信
绝对值。

## 构建

Rust 1.88（edition 2024）。`--locked` 构建即可复现；Linux/macOS/Windows
三平台矩阵，Docker 镜像见 `Dockerfile` 与 `docker-compose.yml`。

## 许可证

Apache-2.0
