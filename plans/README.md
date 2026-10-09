# 开发计划

计划按“先冻结语义，再实现状态机和恢复，最后扩展协议与外围能力”的顺序执行。每个阶段完成后必须补充：

- 实际实现范围；
- 测试命令和测试结果；
- 性能或资源数据；
- 已知限制；
- 与设计文档的偏差；
- 下一阶段的前置条件。

阶段可以提前编写测试和设计，但不能跳过完成门禁。任何涉及日志格式、快照格式、TTL 语义或 Redis 返回值的改变，都必须同步更新 `docs/` 和兼容性测试。

## 阶段列表

1. [项目骨架、测试控制面与工程基线](01-foundation.md)
2. [RESP2 编解码与连接会话](02-resp2.md)
3. [确定性状态机与单节点命令](03-single-node.md)
4. [单节点 WAL、恢复与基础快照](04-wal-recovery.md)
5. [Raft 日志、三节点复制与线性一致读](05-raft-cluster.md)
6. [并发快照、安装与落后节点追赶](06-snapshot.md)
7. [TTL、逻辑时钟与过期清理](07-ttl.md)
8. [故障切换、路由与成员管理](08-failover-membership.md)
9. [RESP3 与客户端兼容](09-resp3.md)
10. [安全、资源限制与可观测性](10-security-observability.md)
11. [数据结构、Pipeline 与事务扩展](11-data-structures-transactions.md)
12. [Benchmark、兼容矩阵与发布](12-benchmark-release.md)

核心契约见：

- [核心设计契约](../docs/core-design-contracts.md)
- [兼容性与故障模型](../docs/compatibility-and-failure-model.md)
