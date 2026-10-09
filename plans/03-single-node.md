# 03 确定性状态机与单节点命令

## 目标

实现可序列化、可重放、可独立测试的状态机内核。该阶段是后续 Raft、WAL、快照和 TTL 的共同基础。

## 第一批命令

- 连接/本地：`PING`、`ECHO`、`SELECT`。
- 字符串：`GET`、`SET`、`DEL`、`EXISTS`、`MGET`、`MSET`。
- 信息：`TYPE`、`DBSIZE`。

## 实现内容

- 定义内部响应值，独立于 RESP2/RESP3 编码。
- 定义 String、Null、Integer、Array、Error 等内部响应。
- 定义 `ValueObject` 和顶层数据类型标识。
- 定义可序列化的复制操作和操作版本。
- 实现单 Key 读、单 Key 写、多 Key 读和多 Key 写分类。
- 实现确定的 Key 排序和多 Key 访问范围。
- 明确覆盖写的 TTL 处理，即使 TTL 命令尚未开放。
- 定义缺失 Key、类型错误、空结果和重复 Key 行为。
- 定义状态机 apply 结果与客户端响应的关系。
- 将 TCP session、数据库选择和事务队列与状态机分离。

## 参考 cache-cat

- `cache_cat/src/raft/types/core/value_object.rs`：参考 Redis 值类型的统一表示和类型标签。
- `cache_cat/src/raft/types/core/response_value.rs`：参考内部响应值与 RESP 编码解耦。
- `cache_cat/src/raft/types/core/cache/string.rs`、`cache/key.rs`：参考 String 和 Key 操作的职责划分。
- `cache_cat/src/raft/types/entry/base_operation.rs`、`entry/request.rs`：参考可序列化操作、请求封装和快照可重放操作的组织方式。
- `cache_cat/src/raft/types/core/mocha/core.rs`、`request_handler.rs`：参考状态机统一 apply/request handler 路径。
- `cache_cat/src/protocol/string/get.rs`、`set.rs`、`mget.rs`、`mset.rs`、`protocol/key/del.rs`、`exists.rs`、`dbsize.rs`：参考第一批命令的参数检查、类型错误和返回值测试。
- `cache_cat/src/raft/types/core/mocha/read_command.rs`、`cas.rs`：参考单 Key 读、计算写和多 Key 操作分类。

## 重点借鉴

cache-cat 使用不同 command trait 表达单 Key 读、计算写和多 Key 计算。alias-cache 可以采用等价的显式 command metadata，但不要为了复刻 trait 名称而引入无必要的泛型复杂度。

## 必须冻结的语义

- `SET` 默认清除旧 TTL。
- `MSET` 要么整体应用，要么整体失败。
- `DEL` 对不存在 Key 的返回值。
- `MGET` 的重复 Key 和顺序。
- Key 和 Value 的最大长度。
- 单数据库或固定数据库数量。
- 操作序列化格式、版本和未知版本错误。

## 测试任务

- 单个状态机实例的状态转换测试。
- 两个独立实例重放同一操作序列并比较状态和响应。
- 序列化/反序列化 round-trip。
- 随机操作序列的 replay/property test。
- 多 Key 操作的原子性和 Key 顺序测试。
- 类型错误、非法参数和资源限制测试。
- 真实 TCP 连接的最小命令测试。

## 重点

状态机不能直接依赖 TCP session。相同操作在测试、Raft apply 和恢复重放中必须走同一条执行路径。

## 完成标准

- 单节点 TCP 服务可以被 Redis 客户端访问。
- 第一批命令的 RESP2 响应正确。
- 操作序列可以序列化、反序列化并重放。
- 同一操作序列在多个状态机实例上产生相同结果。
- 状态机不依赖 TCP、线程调度、节点 ID 或系统时钟。
- 多 Key 操作不存在可被外部观察的中间状态。
- 操作格式有版本号和明确的未知版本行为。
