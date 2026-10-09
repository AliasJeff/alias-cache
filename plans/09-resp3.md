# 09 RESP3 与客户端兼容

## 目标

在 RESP2、状态机和路由语义稳定后，增加必要的 RESP3 协议能力，不改变命令执行层。

## 实现内容

- 实现 `HELLO 2` 和 `HELLO 3` 协商。
- 增加 Null、Boolean、Double、Map、Set、Push、Big Number、Attribute 内部和编码表示。
- 保持内部响应值与 wire encoding 解耦。
- 为每个已支持命令定义 RESP2/RESP3 返回差异。
- 处理协议切换后的 Pipeline 和连接状态。
- 对 Push 消息预留订阅和监控能力，但未实现的功能不得伪造 Push。
- 验证常见 Rust、Python 和 redis-cli 客户端。

## 参考 cache-cat

- `cache_cat/src/protocol/resp.rs`：参考内部响应类型扩展和编码器集中处理。
- `cache_cat/src/protocol/connection/hello.rs`：参考 `HELLO` 协商入口和连接协议状态。
- `cache_cat/src/protocol/connection/client/core.rs`、`info.rs`、`setinfo.rs`：参考客户端元信息与连接状态的边界。
- `cache_cat/tests/test_hello.py`、`test_pipeline.py`：参考协议协商和 Pipeline 回归测试。

## 说明

cache-cat 当前代码中部分 RESP3 能力可能与具体命令绑定。alias-cache 必须先定义内部响应类型，再由 RESP2/RESP3 编码器分别转换。

## 当前实现进度

- 已实现 RESP3 语义 Frame：Null、Boolean、Double、Map、Set、Push、Big Number、
  Verbatim String、Bulk Error 和 Attribute。
- 已实现 RESP2/RESP3 双向编码降级规则；未协商时连接仍只发送 RESP2。
- 已实现连接级 `HELLO 2`、`HELLO 3` 和重复协商。
- `HELLO AUTH` 当前明确返回未配置错误；AUTH 本身属于安全阶段，不在本阶段伪造支持。
- 已覆盖 RESP3 编解码边界、RESP2 降级、协议切换和非法版本测试。

## 测试任务

- 未协商 RESP3 时绝不发送 RESP3 类型。
- `HELLO 2`、`HELLO 3` 和重复协商。
- Map、Set、Null 和 Boolean 的编码边界。
- RESP2 客户端回归。
- RESP3 Pipeline 顺序。
- 客户端连接中途切换协议。

## 完成标准

- 已支持命令的 RESP2 行为不回归。
- RESP3 客户端可以完成基础读写和错误处理。
- 返回结构与兼容矩阵一致。
- 未实现的 RESP3 命令返回稳定错误。
