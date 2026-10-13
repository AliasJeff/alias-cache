# 14 Bitmap、Bloom Filter 与 Stream

## 目标

按"确定性优先、复用既有状态机骨架"的路线扩展三类数据结构。每种结构沿用
同一路径：`Value` 变体 → 确定性 `Operation` → prepare/apply → 重放/快照
测试 → 命令分类表 → 集群级回归。顺序：Bitmap → Bloom Filter → Stream
（非阻塞骨干），风险递增、相互独立。

## 参考 cache-cat

- `cache_cat/src/protocol/bitmap/`（约 2,300 行，不含 bitfield）：
  `setbit.rs`/`getbit.rs`/`bitcount.rs`/`bitpos.rs`/`bitop.rs`：参考
  参数校验、错误文本、位序约定（MSB-first 与 Redis 一致）和 BITOP 的
  多 key AND/OR/XOR/NOT 语义；`bitfield.rs`（840 行）**首版不做**。
- `cache_cat/src/protocol/bf/`（约 1,760 行）：`bf_reserve.rs`/`bf_add.rs`/
  `bf_exits.rs`/`bf_card.rs`/`bf_insert.rs` 参考 BF.RESERVE 的容量/错误率
  参数与校验边界；`bf_scandump.rs`/`bf_loadchunk.rs`（非阻塞转储）**首版
  不做**；`bf_madd`/`bf_mexits` 多 key 变体**首版不做**。
- `cache_cat/src/raft/types/core/structure/stream/`（约 4,400 行含测试）：
  `shared.rs` 的 ID 单调生成与消费者组状态划分；协议层 `xadd.rs`/
  `xread.rs`/`xgroup.rs` 参考 XADD 的 `*`/`MSIZE` 自动 ID 规则与 XREAD
  的阻塞/非阻塞边界。**阻塞 XREAD 与 XREADGROUP 首版不做**（阻塞命令
  需要不占 apply 路径的唤醒机制，是独立工程）。
- `cache_cat/src/raft/types/core/structure/stream/memory.rs`：内存估算的
  归属口径（一条流一个账本，克隆不重复计费）——对应我们的
  `entry_size` 按变体扩展。

## 不直接复制

- cache-cat 的 BF 底层为自研布隆结构；alias-cache 的位数组与 hash 族必须
  全量自定（见设计约束：hash 输入到索引的映射必须跨副本确定）。
- cache-cat 的 Stream ID 依赖其写时钟体系；alias-cache 的 ID 序号基于
  自有逻辑时钟域（与 TTL 同源），不引入新的时间来源。
- 所有命令错误文本对齐 Redis，但内部表示按我们的 `Value`/`Operation`
  骨架重新设计。

## Bitmap 设计（步骤 1，最小风险）

- **表示**：复用 `Value::String`（位数组即字节串），不新增 Value 变体。
  SETBIT 越界时以 0 字节右扩展；读越界返回 0。MSB-first（byte 的最高位
  是 offset 0），与 Redis 一致。
- **命令与 Operation**：
  - `SETBIT key offset value` → `Operation::BitmapSet { key, offset, value }`
    （apply：读改写字节，返回旧位）；offset 上限 `2^32`，超过报错
    `ERR bit offset is not an integer or out of range`。
  - `GETBIT key offset` → 读路径（无 Operation）。
  - `BITCOUNT key [start end]` → 读路径；负索引语义对齐 Redis（字节
    寻址，7.x 的 BIT/BIT 位模式**首版不做**）。
  - `BITPOS key bit [start end]` → 读路径。
  - `BITOP op dest src...` → `Operation::BitmapOp { op, dest, sources }`：
    多 key 读计算 + 单 key 写，apply 逐字节对齐计算（长度取最长、短侧
    补 0）；NOT 仅接受单 src。参考 cache-cat `bitop.rs` 的对齐规则。
- **WRONGTYPE**：对非 String 值的 key 一律 WRONGTYPE（与 GET 同）。
- **测试**：位序断言（SETBIT 0 → 首字节 0x80）；扩展与截断；BITOP 四种
  op 的对齐；跨节点重放一致；分类表更新（SETBIT/BITOP 入 WRITES）。

## Bloom Filter 设计（步骤 2）

- **表示**：`Value::BloomFilter { bits: Vec<u8>, hash_count: u8, seeded: bool }`
  的序列化变体；参数（期望容量/错误率）只在创建时校验并决定 bits 长度
  与 hash_count，不随数据存储。
- **确定性约束（本项目特有，最重要）**：hash 函数族必须跨副本确定——
  采用双重哈希派生：`h_i(x) = (h1(x) + i * h2(x)) mod m`，`h1/h2` 用
  FNV-1a 128 位拆两半（复用 `wal.rs::checksum` 的 FNV 思路，128 位版本
  自实现）。**禁止任何随机盐**。这是与单机 Redis BF 模块的实现差异，
  必须写入兼容文档。
- **命令**（对齐 RedisBloom 子集，键即过滤器）：
  - `BF.RESERVE key error_rate capacity` → 写（创建）；重复创建报错
    `ERR item exists`（RedisBloom 文本）。
  - `BF.ADD key item` → `Operation::BloomAdd { key, item }`。
  - `BF.EXISTS key item` → 读。
  - `BF.MADD`/`BF.MEXISTS` → 批量变体（同一 Operation 内 Vec）。
  - `BF.INFO key` → 读（容量/大小/过滤器数/插入数）。
  - 容量已满时的行为：继续写入（不扩容不报错，错误率超出声明值），
    与 RedisBloom 的 noScale 一致，文档注明。
- **测试**：假阳性率上界冒烟（1% 声明、10k 容量、插入全量后误判率
  < 2%——概率断言用固定种子数据）；跨副本重放一致（同输入同位图）；
  capacity/error_rate 边界校验；分类表更新。

## Stream 设计（步骤 3，非阻塞骨干）

- **表示**：`Value::Stream { entries: BTreeMap<u64, (u64, BTreeMap<Bytes, Bytes>)>, last_id: (u64,u64), groups: BTreeMap<Bytes, GroupState> }`
  ——键为 `ms*2^16|seq` 压缩的 u64（见下），组状态首版可仅保留结构位
  （XGROUP CREATE 的占位），消费者组逻辑后续迭代。
- **ID 生成（关键设计）**：`XADD key * field value...` 的自动 ID =
  `(logical_time_ms, seq)`，seq 在同毫秒内递增。逻辑时钟与 TTL 同域——
  **天然跨副本确定**（日志重放到同一时间点产生同一 ID），这是把
  cache-cat 的写时钟方案映射到我们时钟体系的核心点。显式 ID（`XADD
  key 5-1`）必须大于当前 last_id，否则 `ERR The ID specified in XADD is
  equal or smaller than the target stream top item`。
- **命令**：
  - `XADD key [NOMKSTREAM] id field value...` → 写；`*` 自动 ID；返回
    实际写入的 ID（`ms-seq` 文本）。NOMKSTREAM：流不存在时不创建返回
    Null。
  - `XLEN key` → 读。
  - `XRANGE key start end [COUNT n]` → 读；`-`/`+` 全域；ID 范围含端点；
    顺序按 ID 升序（BTreeMap 天然有序，确定性免费）。
  - `XDEL key id...` → `Operation::StreamDelete { key, ids }`（墓碑式：
    从 entries 移除，last_id 不回退）。
  - `XTRIM key MAXLEN n` → 写（从头部裁剪到 n 条）。
  - **明确不做**：XREAD 阻塞形式、XREADGROUP/XACK（消费者组）、XAUTOCLAIM。
- **测试**：自动 ID 的确定性（同日志重放产生相同 ID 序列——这是最核心
  的护栏测试）；显式 ID 边界（等于/小于 last_id 拒绝）；XRANGE 范围与
  COUNT；XDEL 后 XADD 的 ID 仍单调；空流删除 key；分类表更新。

## 与资源限制/快照的联动（每步必做）

- `entry_size` 为新 Value 变体补内存估算（BF 按 bits.len()+参数、Stream
  按 entries 与字节数）。
- OOM 门 `incoming_bytes` 为新 Operation 补计费。
- 快照序列化路径（bincode 全量）自动获得新变体；三处存储格式版本
  **不动**（serde 向后兼容的新 enum 变体），但需在迁移文档追加说明。

## 完成记录

### Bitmap（2026-10-13，已交付）

- SETBIT/GETBIT/BITCOUNT/BITPOS/BITOP 全部落地在 `Value::String` 位寻址上；
  apply 端零扩展、全零清 key、BITOP 零填充对齐与 NOT 单源校验、
  prepare/apply 响应一致（BITOP 返回目标长度）；WRONGTYPE 边界与
  Redis BITPOS 0 全 1 缓冲 quirk 均有测试。

### Bloom Filter（2026-10-13，已交付）

- `Value::BloomFilter { bits, hash_count, inserted }`；BF.RESERVE 按
  error_rate/capacity 计算 bits 与 hash 数；BF.ADD 经 BytesMut 原位置位
  （确定性双重 FNV-1a-64，无盐）；未保留 key 隐式默认 1024/1%；容量
  写满 noScale；TYPE 返回 `MBbloom--`；假阳性冒烟 <5%。

### Stream 非阻塞骨干（2026-10-13，已交付）

- `Value::Stream { entries: BTreeMap<(ms,seq), fields>, last_id }`；XADD
  自动 ID = 逻辑时钟毫秒 + 严格递增序号（同日志重放确定，护栏测试覆盖
  跨副本 ID 一致）；显式 ID 必须大于顶部 ID；XLEN/XRANGE（含端点解析、
  COUNT、Redis 嵌套条目形状）/XDEL（缺失计 0）/XTRIM MAXLEN；空流删 key。
- 首版裁剪：NOMKSTREAM 标记不生效；阻塞 XREAD、消费者组、XINFO 后续
  立项。

## 完成标准

- 每结构：协议测试（错误文本对齐 Redis）+ 状态机重放测试 + 集群级
  RESP 回归 + 分类护栏更新。
- BF hash 确定性与 Stream ID 确定性各有专属护栏测试（跨副本分歧的
  防线）。
- 兼容矩阵文档补三节约束（Bitmap 位序/BF hash 确定性/Stream ID 语义
  与阻塞边界）。
- `cargo test --workspace` 全绿；fmt/clippy 干净；冒烟脚本不回归。
