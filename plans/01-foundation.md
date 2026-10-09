# 01 项目骨架、测试控制面与工程基线

## 目标

建立 workspace、模块边界、配置体系和可重复的测试控制面。此阶段不实现真实业务存储，但要让后续阶段可以测试半包、时间、进程、网络和磁盘故障。

## 实现内容

- 创建 Cargo workspace，拆分核心库、服务端、测试工具和 benchmark。
- 建立 `protocol`、`command`、`state_machine`、`raft`、`storage`、`snapshot`、`config`、`observability` 模块边界。
- 定义统一错误类型，区分协议错误、客户端错误、Leader 错误、存储错误、配置错误和暂时性错误。
- 定义节点 ID、数据库数量、监听地址、Raft 地址、数据目录和资源限制配置。
- 建立 `Clock` 抽象，至少支持真实时间和测试可控时间。
- 建立测试控制面：
  - 启停本地节点；
  - 等待 Leader；
  - 等待指定 commit/apply 位置；
  - 注入 RPC 延迟、丢包和分区；
  - 注入磁盘写入、fsync 和空间错误；
  - 收集节点日志和指标。
- 加入 `rustfmt`、Clippy、基础 CI 和最小 Docker 构建。
- 提供配置校验和最小启动命令。

## 参考 cache-cat

- `Cargo.toml`、`cache_cat/Cargo.toml`、`cache_cat_ping/Cargo.toml`：参考 workspace、服务端和独立连通性工具的拆分方式。
- `cache_cat/src/cfg/config.rs`、`cfg/default.rs`、`cfg/cli_arg.rs`：参考配置分层、默认值和命令行解析；不要直接复制配置字段，先按 alias-cache 的安全默认值重画配置。
- `cache_cat/src/error.rs`：参考错误类别和 Redis 错误映射；重点借鉴“可重试错误”和“Leader 错误”分离。
- `cache_cat/src/node/parsed_config.rs`：参考把原始配置转换为运行时配置的方式。
- `cache_cat/src/test/mod.rs`、`cache_cat/tests/clusters/cluster.py`：参考测试辅助层和多节点进程管理；alias-cache 需要进一步加入可控时钟、网络分区和磁盘故障注入。
- `cache_cat/src/utils/times.rs`：只参考时间工具的边界测试，不复制直接读取系统时间的业务逻辑。

## 参考原则

参考的是模块职责和测试方式，不直接复制实现。所有涉及网络、磁盘和用户输入的代码都要重新通过 alias-cache 的资源限制和错误处理门禁。

## 重点约束

- 网络、磁盘和用户输入路径禁止 `unwrap`、`expect`、`panic!`。
- 所有边界使用有上限的缓冲区、队列和并发控制。
- 代码注释使用英文，用户文档使用中文。
- 任何测试不能依赖固定的人工 `sleep` 判断状态。
- 设计契约中的术语和状态命名在代码中保持一致。

## 测试任务

- 配置缺失、重复节点 ID、非法端口、非法数据目录测试。
- 可控时钟单调递增、冻结、推进和回拨测试。
- 测试控制面自身的节点启动、停止、等待和故障注入测试。
- 错误类型到 RESP 错误的映射测试。

## 完成标准

- `cargo build --workspace` 通过。
- `cargo test --workspace` 通过。
- `cargo fmt --check` 通过。
- Clippy 门禁加入 CI。
- 文档能说明如何启动一个空服务。
- 测试工具可以启动并关闭一个空节点。
- 可以通过可控时钟重现一个确定的时间场景。
- 文档列出当前支持的配置和资源上限。

## 非目标

- 不实现 Raft。
- 不实现 Redis 命令。
- 不实现生产级网络故障注入的全部能力。
