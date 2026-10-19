# 19 FileLogStore → raft-engine 替换（写侧根因修复）

## 根因（实验确认，非猜测）

FileLogStore 的 fsync 耗时随 WAL 文件大小线性增长：

| WAL 大小 | fsync 平均 | fsync 最大 | 纯写吞吐 |
| --- | ---: | ---: | ---: |
| ~10 MB（快照截断后） | ~0.1-0.3 ms | — | 103-137k 稳定 |
| ~700 MB（无快照增长） | **2.3 ms** | **54.5 ms** | 186k→**100k 衰减** |

raft-direct 绕过 RESP 后仍然只有 68k——**瓶颈不在 RESP 协议层，在 FileLogStore 的 WAL fsync**。

cache-cat 用 TiKV 的 `raft-engine` crate 作为日志存储，内置分段、组提交、高效 fsync——从设计上消除了这个问题。

## 替换方案

### 依赖

```toml
# cache-cat 使用的版本（git 依赖，锁定 rev）
raft-engine = { git = "https://github.com/tikv/raft-engine", rev = "097c499a19fbb38754c73aa2f31532329df7c0c6" }
```

### 核心结构

```rust
// 参考 cache-cat raft_engine.rs
pub struct RaftEngineLogStore {
    engine: Arc<raft_engine::Engine>,
    committed_path: PathBuf,
    // committed sidecar 保持不变
}

// raft-engine 使用 LogBatch 进行批量写入：
// 多条 entry 合并为一个 LogBatch，一次 write + 一次 sync
// 消除逐条 checksum 和逐条 fsync
```

### 需实现的 trait

| Trait | 方法 | raft-engine API |
| --- | --- | --- |
| RaftLogReader | try_get_log_entries(range) | engine.read_entries(range) |
| RaftLogReader | get_log_state() | engine.last_log_id() + vote |
| RaftLogStorage | append(entries) + flush | engine.write(LogBatch) + sync |
| RaftLogStorage | save_committed / read_committed | 保持现有 sidecar |
| RaftLogStorage | purge(log_id) | engine.purge_logs(upto) |
| RaftLogStorage | get_log_reader | 返回 self |
| RaftSnapshotBuilder | build_snapshot | 保持现有增量快照 |

### 序列化

cache-cat 使用 `Bincode2Codec` + `StoredEntry` 包装 openraft Entry，
通过 raft-engine 的 `ValueCodec` trait 写入/读取。
我们的 TypeConfig 的 Entry 类型需要实现相同的 Codec trait。

### 快照层

快照层（incremental snapshots）不需要改动——它独立于日志存储，
base 文件 + delta 文件的结构保持不变。唯一变化是：截断日志时调用
`engine.purge_logs()` 替代 `FileLogStore::truncate`。

### 迁移步骤

1. 添加 raft-engine 依赖（git，锁 rev）
2. 实现 `RaftEngineLogStore`（参考 cache-cat log_store.rs，463 行）
3. 替换 `new_file_raft_with_metrics` / `new_tcp_raft_for_server_with_metrics` 中的 log store
4. 保持 `MemStateMachine` 不变（SM 与日志存储解耦 ✓）
5. 数据迁移：旧 FileLogStore 数据不迁移（测试环境可重建）
6. 全量测试 + 基准

### 预期效果

| 指标 | 当前（FileLogStore 700MB） | 替换后（raft-engine） |
| --- | ---: | ---: |
| fsync 平均 | 2.3 ms | ~0.1-0.3 ms（内置优化） |
| 纯写（raft-direct） | 68-107k | **150-250k（匹配 cache-cat）** |
| 50/50 | 43-96k | **120-180k（目标）** |
| 5/95 | 75-94k | **150-250k（目标）** |
| WAL 大小增长影响 | 线性退化 | 无（raft-engine 内部分段） |

### 风险

- raft-engine 是 git 依赖（非 crates.io），需要锁定 rev 确保可复现
- openraft alpha.34 的 RaftLogStorage trait 签名需确认与 raft-engine 兼容
- cache-cat 使用的 fork 可能有 openraft alpha.34 特定修改
- 数据格式变化——旧 FileLogStore 数据不可迁移（测试环境可接受）

## 前置条件

本重构是独立的日志存储层替换，不影响 SM（已优化）、不影响 RESP 层、
不影响读路径。可以安全地独立实施和验证。

## 实施记录（2026-10-19 完成）

### 落地内容

- 新模块 `alias-cache/src/raft_engine_store.rs`：`RaftEngineLogStore`
  包装 `Arc<raft_engine::Engine>`（单 raft group，REGION_ID=1），
  实现 openraft v2 `RaftLogStorage` + `RaftLogReader`：
  - `append`：`LogBatch::with_capacity(256)` 合批 + `write(sync=false)`，
    `engine.sync()` 在 `spawn_blocking` 中执行并回调 `IOFlushed`
  - `save_vote`/`truncate_after`：写后显式 sync（vote 与截断必须持久）
  - `truncate_after`：用"重写边界 entry"实现删尾（raft-engine 无 drop-tail
    原语；`compact_to` 只删前缀）——直接采用 cache-cat 的已验证方案
  - `purge`：LastPurged 元数据 + `engine.compact_to(index+1)`
  - vote / last_purged 存 engine 元数据（`ValueCodec` bincode2）；
    committed sidecar（200ms 去抖）原样保留
  - 5 个单元测试（roundtrip / 截断重追加 / purge 后 clamp 读 / sidecar 重开恢复）
- 依赖：`raft-engine` git 锁 rev `097c499`（default-features=false，去掉 rhai）
  + `bincode2 2.0.1` + dev `tempfile`
- 构造函数切换：`new_file_raft_with_metrics` 与 `new_tcp_raft_inner_with_metrics`
  改用 `data_dir/raft/raft-engine`；`FileLogStore` 保留（其崩溃恢复单元测试
  是 16 个已落地修复的回归网，生产路径不再使用）
- `INFO stats` 的 `raft_log_bytes` 改为递归统计 `raft/raft-engine` 目录
- **177 测试全绿（172+5）、clippy 零告警（pinned 1.88.0）**

### 验证结果（2026-10-19，同机 3 节点新鲜集群，512c×p16，ks=400k，v=16B，node1=leader）

| 指标 | 改前（plans/18 Phase 1 记录） | 改后 | 变化 |
| --- | ---: | ---: | --- |
| fsync 均值 @1.6GB WAL | 2.3ms @700MB（随大小增长，峰值 54ms） | **0.31-0.33ms**（max 23-40ms） | **~8×，且与 WAL 大小无关** |
| 纯写吞吐 vs WAL 增长 | 衰减（186k→100k→持续恶化） | **85-88k 平稳**（4 轮，WAL 0.4→1.6GB） | **衰减消除** |
| 5/95（wp95） | 43.7k | **78.7k** | **+80%** |
| 50/50 | 38-44k | **53.9k** | +23-42% |
| 95/5（wp5） | 250-261k | 266k | 持平（仍 ≥ cache-cat 203-236k） |
| 纯写（绝对值） | 预载 97,960 | 85-97k | 持平——瓶颈已移至下一层 |

全部零错误。

### 结论与下一步

- **根因修复达成**：fsync 与 WAL 大小解耦（1.6GB 下仍 0.3ms）、吞吐无时间
  衰减。plans/19 立项时的 E1 稳定性判据满足。
- 混合负载大幅受益（写占比越高收益越大），95/5 读档不受影响。
- 纯写绝对值（~90k）未达 150-250k 预期带：**纯写瓶颈已不在存储层**——
  8192 在途下批量均值 981 条/append、~101 append/s（~10ms/轮），上限由
  RESP 客户端路径 + commit round 速率决定，属 plans/18 Phase 2-5 范围。
  plans/19 的预期表假设存储仍是混合档之外的全部约束，实测证明纯写档
  的约束早已转移到每条成本层。
- 旧记录"首轮 186k"无复现口径（仅存在于 plans/19 根因表），不作为对照基线。

### 遗留

- 长时 soak（>1h）未跑（挂起项，与 CI 账单一起由用户决策）
- ~~`FileLogStore` 及其测试保留在树中，待评估删除~~ **已删除**
  （2026-10-19 设计清理：生产路径确认全部走 raft-engine 后，
  FileLogStore 格式机、rewrite 机制及其 12 项格式测试移除，
  openraft 官方存储契约套件改跑在 RaftEngineLogStore 上）
