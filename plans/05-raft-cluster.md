# 05 Raft 日志、三节点复制与线性一致读

## 目标

接入成熟 Raft 实现，将已验证的状态机和日志存储组合成三节点集群。

## 实现内容

- 定义节点 ID、节点地址、集群初始配置和 bootstrap 规则。
- 接入 Raft 网络层和日志存储适配。
- 定义业务日志 payload 与 membership entry 的边界。
- 实现日志 apply 到状态机的单一路径。
- 只允许 Leader 接受复制写。
- 实现 Follower 的明确 redirect/error。
- 实现 Leader 写请求等待提交和 apply 后返回。
- 实现 ReadIndex 线性一致读。
- 记录 term、leader、commit index、last applied 和日志长度。
- 建立三节点本地启动和清理脚本。

## 参考 cache-cat

- `cache_cat/src/node/raft_node.rs`：参考 OpenRaft 配置、节点启动和 Raft 实例组装。
- `cache_cat/src/node/raft_builder.rs`：参考节点构建、后台任务和 Leader 条件任务的组织方式。
- `cache_cat/src/raft/mod.rs`、`raft/application/cluster.rs`：参考 Raft application facade 和集群状态查询。
- `cache_cat/src/raft/network/raft_network.rs`、`network/rpc.rs`、`network/model.rs`：参考节点间 RPC、请求模型和快照 RPC 的边界。
- `cache_cat/src/raft/network/client.rs`、`pipeline_client.rs`：参考 Leader RPC 调用、响应等待和连接复用。
- `cache_cat/src/raft/types/raft_types.rs`、`types/entry/request.rs`、`entry/membership.rs`：参考节点类型、业务日志和 membership entry 的建模。
- `cache_cat/conf/node1.toml`、`node2.toml`、`node3.toml`、`cache_cat/tests/test_cluster.py`：参考三节点启动配置和集群测试编排。

## 不能直接复制

cache-cat 的部分读路径包含其特定的 lease/逻辑时钟假设。alias-cache 第一版必须以 ReadIndex 为默认线性一致读实现，只有在单独证明时钟假设后才考虑 lease read。

## 重点

- 写入成功只能发生在日志提交之后。
- Follower 不能把本地未提交状态暴露给客户端。
- 日志操作必须确定性执行。
- Leader 变化必须可观测。
- 没有 Leader 时写入返回 `NOLEADER` 或 `TRYAGAIN`。
- Leader 失去多数派时不得返回成功写入。
- Follower 不得读取未提交状态。
- Leader 在提交后响应前退出时，客户端请求状态可以未知，但日志不能丢失。

## 完成标准

- 三节点可以完成选举。
- Leader 写入能在多数节点复制并应用。
- Follower 不会独立产生冲突写入。
- 杀掉 Leader 后可以重新选举并继续写入。
- 集群测试不依赖人工 sleep 判断成功。
- 三节点可以稳定选出唯一 Leader。
- 写成功只在提交和 apply 后返回。
- 线性一致读测试通过。
- 测试由控制面等待状态，不依赖固定 sleep。
- Leader、commit/apply 位置和错误原因可观察。
