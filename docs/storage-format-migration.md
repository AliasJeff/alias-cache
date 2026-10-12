# 存储格式与版本迁移策略

alias-cache 有三个独立的持久化格式版本号，各自演进、互不绑定：

| 格式 | 位置 | 常量 | 当前版本 |
| --- | --- | --- | --- |
| WAL 记录 | `data/<node>/raft/log` | `wal.rs::VERSION` | 4 |
| 单节点本地快照 | `data/<node>/snapshot.bin`（legacy 单节点路径） | `snapshot.rs::VERSION` | 4 |
| Raft 快照载荷 | `data/<node>/raft/snapshot` | `raft.rs::RAFT_SNAPSHOT_PAYLOAD_VERSION` | 4 |

## 升级规则

1. **只允许拒绝，不允许猜测。** 任何格式号小于当前版本的数据在启动时
   立即失败并输出可诊断错误（例如 `WAL version is unsupported`、
   `raft snapshot payload version 2 is unsupported`），绝不静默迁移、
   也绝不以空状态启动覆盖数据。
2. **版本随结构变更递增。** 复制状态机新增字段或日志操作（如 WATCH 引入
   `versions` 计数器、批量命令日志项）时，三个版本号一起 bump，旧数据显式失效。
3. **恢复后重算派生数据。** 计数器类字段（`memory_bytes`）以
   `#[serde(skip)]` 排除在格式之外，快照/WAL 恢复后由 `recompute_memory`
   从活数据重建——派生值永不持久化，避免格式漂移。
4. **测试锁定语义。** `rejects_wal_from_older_format_version`、
   `rejects_snapshot_from_older_format_version`、
   `raft_snapshot_rejects_unsupported_payload_version` 三个测试分别
   断言：篡改版本号的文件必须启动失败，且错误信息含实际读到的版本号。

## 版本 4

版本 4 为有界 Pipeline 批次新增复制操作 `Operation::ClientBatch`。日志项保存
原始请求、请求 ID、逻辑写入时间、Leader 捕获的内存上限和批量准入上限；每个
副本按实际日志位置执行命令并缓存结果。版本 3 数据会被明确拒绝，必须按本文的
0.x 升级路径导出并重建。

## 升级路径

由于本项目尚处 0.x，升级策略是"备份 + 重建"而非在线迁移：

1. 停止所有节点（集群内已提交数据在三节点多数派上，直接备份任一
   存活的 `data/` 目录即可保留全部已提交状态）；
2. 通过任意客户端导出业务数据（`SCAN` 遍历 + `GET`/`HGETALL` 等）；
3. 清空 `data/` 目录后以新版本二进制重启并重新 bootstrap 集群；
4. 回灌导出的数据。

未来若引入自动在线迁移，必须满足：逐记录前向转换、转换具备幂等性、
迁移完成前拒绝对外服务、迁移失败保留原文件并给出诊断。在此之前，
版本不匹配永远是一次性的、显式的启动失败。
