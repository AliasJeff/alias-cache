# 02 RESP2 编解码与连接会话

## 目标

实现独立于存储和 Raft 的可靠 RESP2 入口，并冻结连接级状态与请求生命周期。

## 实现内容

- 支持 Simple String、Error、Integer、Bulk String、Array。
- 支持半包、粘包和 Pipeline。
- 支持二进制安全的 key/value。
- 增加最大帧大小、最大数组元素数、最大嵌套深度、最大 Pipeline 长度。
- 解析命令名称并统一大小写。
- 定义协议层内部请求类型，不在解析器中执行命令。
- 建立连接级状态：数据库、认证状态、RESP 版本、MULTI 状态和连接关闭状态。
- 定义协议错误、命令错误、参数错误和服务端错误的边界。
- 定义连接关闭、半关闭、读超时、写阻塞和客户端断开的行为。

## 参考 cache-cat

- `cache_cat/src/protocol/resp.rs`：参考 RESP 值模型、解析器和编码器的分层方式。
- `cache_cat/src/protocol/command.rs`：参考命令名称到参数解析器的注册和 dispatch 入口。
- `cache_cat/src/raft/network/redis_server.rs`：参考 TCP listener、连接任务和请求循环的组织方式。
- `cache_cat/src/raft/network/connection.rs`、`pipeline_client.rs`：参考连接读写、Pipeline 和请求响应顺序处理。
- `cache_cat/src/protocol/connection/mod.rs`、`connection/client/core.rs`：参考连接状态与客户端上下文的拆分。
- `cache_cat/src/protocol/connection/ping.rs`、`echo.rs`、`quit.rs`：参考最小连接命令和协议响应测试。

## 不直接复制

cache-cat 的命令执行路径与 Raft 状态机耦合较深。alias-cache 的 parser、session、command planner 和 consistency layer 必须保持独立，不能让 RESP parser 直接修改状态机。

## 测试重点

- 空 Bulk String 和 Null Bulk String。
- CRLF 边界。
- 超大长度和整数溢出。
- 半包逐字节输入。
- 多条命令合并输入。
- 非 UTF-8 key/value。
- 恶意嵌套数组。
- 客户端在响应中途断开。
- 多连接并发发送请求。

## 完成标准

- 协议层无需启动 Raft 即可独立测试。
- 所有解析失败不会 panic。
- Pipeline 请求顺序和响应顺序符合定义。
- 协议测试覆盖正常、边界和恶意输入。
- Redis CLI 或等价客户端可以完成协议级 PING/echo 测试。

## 完成记录

- 2026-10：PING/ECHO 已在连接层实现（`alias-cache/src/server.rs::connection_command`），
  不进入 Raft、不依赖 Leader，follower 亦可应答；错误文本与 Redis 对齐。
  测试：`server::tests::connection_commands_answer_ping_and_echo_without_raft`
  覆盖 `+PONG`、bulk 回显和两类参数数量错误。
  Redis CLI 兼容由 `redis-cli -p 6379 PING` 手工验证。

## 非目标

- 不实现完整命令语义。
- 不实现 RESP3。
- 不在协议解析器中处理 Leader 路由。
