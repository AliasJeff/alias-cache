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
