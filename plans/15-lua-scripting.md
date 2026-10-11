# 15 Lua 脚本（EVAL / EVALSHA / SCRIPT）

## 目标

提供 Redis 语义的 Lua 脚本执行：脚本在 Leader 上执行一次，`redis.call`
产生的写操作作为**确定性 Operation 进入 Raft 日志**，副本只重放操作
不执行脚本。以 mlua 沙箱（内存上限 + 指令钩子中断）约束不可信脚本，
用安全 Rust 桥接（无 unsafe）。

## 架构决策：复制执行结果，不复制脚本

这是整个设计的核心，参照 cache-cat 的选择（`protocol/lua_env.rs` 未对
`math.random`/`os.time` 做任何拦截即为该决策的体现）：

- ** Leader 执行模型**：EVAL 到达 leader 后，在 leader 的当前状态机快照
  上执行脚本；脚本中的 `redis.call("SET",...)` 走与普通命令完全相同的
  `prepare_request` 路径生成 Operation；全部 Operation 按执行顺序打包为
  **一个 `Operation::Transaction`**（复用既有原子提交机制）进入日志。
- **副本只重放**：日志里是确定性 Operation 序列，不含脚本字节码。因此
  `math.random()`、`os.time()`、浮点运算等非确定性只在 leader 侧生效
  一次，**从根上回避了"脚本必须确定性"的一致性风险**——不需要禁任何
  标准库。
- 代价与语义边界（写入兼容文档）：脚本执行期间的读是 leader 当前已
  提交状态（与 MULTI/EXEC 中读的限制相同，我们的事务本就不含读）；
  脚本提交与其他客户端写入的线性化顺序 = 日志顺序，天然一致。

## 参考 cache-cat

- `cache_cat/src/protocol/lua_env.rs`（270 行）：整体骨架——mlua 实例
  池、`set_memory_limit(MAX_MEM)`、`set_hook(HookTriggers::every_nth_
  instruction(1000), ...)` 检查中断标志实现超时/取消、`lua.scope` 内注
  入 `redis` 表（call/pcall/setresp）、脚本编译缓存
  （`get_or_compile_script`）。
- `cache_cat/src/protocol/lua/eval.rs`（296 行）：EVAL 的参数解析
  （KEYS/ARGV 划分）、执行与响应映射（Lua 值 → RESP 帧）。
- `cache_cat/src/protocol/lua/evalsha.rs`（319 行）+ `script.rs`（326 行）：
  SCRIPT LOAD/EXISTS 与 EVALSHA 的脚本缓存键（SHA1）管理。
- `Cargo.toml`：`mlua = { version = "0.11", features = ["lua51",
  "vendored", "send"] }`——lua51 + vendored（内置编译，无系统依赖）+
  send（实例可跨任务移动，安全池的前提）。

## 不直接复制

1. **消除 unsafe**：cache-cat 的 `redis.call` 桥接用裸指针
   （`update_ptr: *mut Update` + 单线程论证）。alias-cache 的桥接结构：
   `redis.call` 收集器——脚本执行期间 `redis.call` 只把参数**记录**到
   `Vec<Request>`，脚本结束后统一在状态机快照上逐条 prepare 并打包
   Transaction。**执行与状态访问分离**，闭包只捕获收集器（`Arc<Mutex<
   Vec<Request>>>` 或独立 struct），零 unsafe。
   - 语义代价（写入文档）：脚本内读命令（GET 等）在收集模型下不可行，
     首版 `redis.call` 仅接受**写命令**，读命令返回错误
     `ERR read commands are not allowed in scripts`（cache-cat 支持脚本
     内读，这是有意的首版裁剪；后续若要支持，需在 leader 快照上同步
     执行读，仍无 unsafe）。
2. **响应映射**：Lua→RESP 的转换表（number→Integer、string→BulkString、
   table→Array、true/false→Integer 1/0、nil→Null）按 RESP2 语义自写，
   RESP3 连接按现有 `write_frame` 降级路径自动处理。

## 实现步骤

### 步骤 1：依赖与沙箱骨架

- workspace 加 `mlua`（版本/features 对齐 cache-cat，加 `rust-version`
  兼容性验证：vendored lua 需 cc 构建，CI 三平台已具备）。
- `lua.rs` 模块：`ScriptSandbox` —— 实例池（每 CPU 一实例，`send`），
  `set_memory_limit`（上限常量如 64 MiB）、每 1000 指令 hook 检查
  `interrupt` 标志；执行超时（如 5 秒）由外层 `tokio::time::timeout`
  置位中断标志实现。
- 测试：死循环脚本在超时内被中断并返回 `ERR Script execution timed
  out`；内存超限脚本报错；沙箱无法访问 io/os 库（mlua 默认不含，断言
  `require("io")` 失败）。

### 步骤 2：redis.call 收集器与原子提交

- 桥接闭包仅捕获 `RequestCollector`（无状态机引用）；`redis.call`/
  `redis.pcall`（pcall 捕获错误转为 Lua error 而非中断脚本框架）。
- EVAL 入口（leader 路径）：解析 KEYS/ARGV → 沙箱执行 → 收集器产出
  `Vec<Request>` → 逐条 `prepare_request`（任一失败则整个 EVAL 返回
  该命令错误，不提交——与事务整批拒绝语义一致）→ 打包
  `Operation::Transaction { watch_expectations: vec![] }` → 复用
  `execute_transaction_request_with_id`（幂等 ID、leader 转发、集群
  提交全部免费获得）。
- follower 收到 EVAL：走现有事务转发路径整段转 leader（脚本只在
  leader 执行一次的保证）。
- 测试：脚本内 3 个 SET → 单日志条目、3 条响应；脚本内命令失败 →
  整个 EVAL 失败且零残留；同脚本重试（幂等 ID）不重复应用；三节点
  集成：副本状态机与 leader 逐字段一致（副本从未执行脚本的证据）。

### 步骤 3：SCRIPT LOAD / EXISTS / EVALSHA 与脚本缓存

- SHA1（RustCrypto `sha1` crate）哈希脚本源码为缓存键；缓存为 leader
  内存表（不复制——副本不需要脚本），容量上限 + LRU 淘汰常量。
- `SCRIPT LOAD` 返回 SHA；`EVALSHA` 未命中时返回 `NOSCRIPT No matching
  script. Please use EVAL.`（Redis 文本）；`SCRIPT EXISTS` 批量查询。
- 测试：LOAD→EVALSHA 往返；NOSCRIPT 错误文本；EXISTS。

### 步骤 4：响应映射与集成收尾

- Lua 值 → RESP 帧完整映射表 + RESP3 连接回归。
- `redis.error_reply`/`redis.status_reply` 辅助函数（对齐 cache-cat
  lua_env 的 redis 表全集：call/pcall/error_reply/status_reply/setresp）。
- 分类表：EVAL/EVALSHA/SCRIPT 归**写路径**（生成 Operation）；
  `is_replicated_write` 更新与护栏测试同步。
- OOM 门：脚本产生的批量 Operation 经 Transaction 路径自动计费
  （验证 `incoming_bytes` 对 Transaction 求和覆盖 Lua 批）。
- 兼容矩阵新章节：执行模型（leader 执行/结果复制）、首版限制（无脚本
  内读、无 SCRIPT FLUSH、超时/内存上限常量）、确定性说明（为何无需
  禁 random）。

## 风险与回退

- mlua vendored 构建在 CI windows 的编译时长/工具链兼容 → 预先在
  本地 `--target x86_64-pc-windows-msvc` 交叉验证；失败则 lua feature
  降级为系统 lua（仅 CI）。
- 脚本执行占 leader 事件循环 → 指令钩子 + 超时双保险已覆盖；若实测
  仍有延迟尖刺，将沙箱移入专用线程池（mlua send 支持实例迁移），
  该调整不影响协议语义。
- 收集器模型不支持脚本内读的首版限制若被真实场景需要 → 迭代二：
  leader 快照同步读（依旧零 unsafe），作为独立小步提交。

## 完成标准

- 沙箱三护栏（超时/内存/io 隔离）测试在案。
- 脚本 = 单条 Transaction 日志、副本零脚本执行的集群级证据测试。
- EVAL/EVALSHA/SCRIPT 家族错误文本对齐 Redis；幂等重试覆盖脚本路径。
- 兼容文档含执行模型与限制章节；分类护栏/测试/冒烟全绿。
