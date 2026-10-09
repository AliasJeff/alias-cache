# 04 单节点 WAL、恢复与基础快照

## 目标

在接入 Raft 前先把日志存储、崩溃恢复和单节点快照验证清楚，避免把状态机错误和共识错误混在一起。

## 实现内容

- 选择并封装 Raft 日志存储组件。
- 定义日志、元数据和快照格式版本。
- 持久化日志条目、vote、last purged log、membership 和 applied metadata。
- 实现追加、读取、范围读取、截断、前缀清理和同步。
- 明确 write、flush、fsync 和向调用方返回之间的关系。
- 启动时恢复日志状态和状态机。
- 处理尾部部分写入、中间损坏、未知版本和目录权限错误。
- 实现单节点基础快照，包含状态机数据、last applied log id、逻辑时钟、数据库数量和校验信息。
- 建立旧快照保留和新快照原子发布策略。

## 参考 cache-cat

- `cache_cat/src/raft/store/log_store.rs`：重点参考 RaftLogReader/RaftLogStorage 适配、范围读取、vote 持久化、日志截断和 purge 边界。
- `cache_cat/src/raft/store/raft_engine.rs`：参考日志引擎封装、序列化 codec 和引擎初始化。
- `cache_cat/src/raft/store/statemachine.rs`：参考状态机启动恢复、applied state 和 snapshot builder 接口。
- `cache_cat/src/raft/types/file_operator.rs`：参考快照文件句柄、临时文件和本地 hard link 处理。
- `cache_cat/src/raft/store/snapshot/snapshot_handler.rs`：参考快照文件头、元数据占位、flush/rename 和加载流程。
- `cache_cat/src/raft/types/raft_types.rs`：参考 LogId、SnapshotMeta、StoredMembership 等类型边界。

## 重点借鉴

尤其要保留 cache-cat 对 `truncate_after`、日志清理和快照安装后旧尾部的处理思路。这个问题不能只依赖底层日志库默认行为。

## 崩溃窗口

必须分别测试：

1. 日志写入前退出；
2. 日志写入后、fsync 前退出；
3. fsync 后、提交前退出；
4. 提交后、apply 前退出；
5. apply 后、响应前退出；
6. 快照临时文件写入中退出；
7. 快照发布后、日志清理前退出。

## 测试重点

- 写入后正常重启。
- 日志追加后立即退出。
- 日志中间损坏。
- 日志尾部部分写入。
- 恢复后继续追加。
- Leader 和 Follower 分别恢复。
- 日志尾部截断和部分记录恢复。
- 日志中间损坏拒绝启动。
- 截断后追加新日志。
- 旧日志清理后读取边界。
- 快照恢复后继续应用日志。
- 新快照失败时旧快照仍可用。
- 进程级 kill 和重新启动测试。

## 完成标准

- 已提交写入重启后可读。
- 未提交写入不会被错误地当作已提交状态。
- 恢复失败会返回明确错误，不会静默启动空数据库。
- WAL 测试覆盖异常退出窗口。
- 单节点快照恢复结果与逐条重放一致。

## 非目标

- 不做远程快照安装。
- 不做成员变更。
- 不宣称集群高可用。
