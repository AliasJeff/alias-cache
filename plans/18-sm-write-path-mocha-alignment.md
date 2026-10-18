# 18 状态机写路径 Mocha 化改造

## 目标

按 cache-cat Mocha 的设计改造状态机写路径，消除每条写的额外开销，
使写主导负载（50/50、5/95）达到同机 cache-cat 的水平。

## 设计差距对照（每条写 SET 的逐操作对比）

### cache-cat Mocha 模型（每条 SET）

```
map.pin().compute(key, |entry| {
    // 一次原子操作：读旧值 + 写新值 + 检查过期
    Operation::Insert(new_entry)
})
→ 返回 Value::ok()
```

- **1 次并发 HashMap 操作**（lock-free compute）
- 无版本追踪、无内存计费、无幂等缓存、无 Frame 构造
- 响应由 RPC 层 bincode 序列化

### alias-cache 当前模型（每条 SET）

```
① client_results.get(&request_id)          // BTreeMap 查找（幂等检查）
② bump_version(&key.clone())               // versions HashMap + key.clone()
③ insert_entry(key, entry)                 // values HashMap + entry_size + memory_bytes
④ Frame::simple_string("OK")               // 响应构造（Bytes::from_static，便宜）
⑤ cache_client_response(id, resp.clone())  // BTreeMap 插入 + 淘汰检查
⑥ ApplyResult::Response(response)           // Frame clone
```

- **3 次 map 操作**（versions + values + client_results）vs Mocha 的 1 次
- **2 次 BTreeMap 操作**（client_results get + insert）vs 0
- **1 次 entry_size 计算** + memory_bytes 更新 vs 0
- **1 次 key.clone()** + **1 次 response.clone()** vs 0
- 合计每条额外 ~2-3µs vs Mocha 的 ~0.2µs

## 分阶段改造

### Phase 1：合并 versions 到 Entry（消除 versions HashMap）

**设计**：Entry 增加 `version: u64` 字段；`bump_version` 变为对 values
中 Entry 的 in-place version 递增；删除 `versions: HashMap<Bytes, u64>`。

**正确性**：WATCH 的 CAS 语义不变——版本计数仍在每次写时递增，只是
存储位置从独立 map 移入 Entry。WATCH 读取版本时通过 values 查找。

**影响**：每条写减少 1 次 HashMap 操作 + 1 次 key.clone()。
**预期收益**：apply 每条 -0.3-0.5µs。
**风险**：低。Entry 序列化格式变化需版本门禁。

### Phase 2：client_results 移出热路径

**设计**：幂等缓存的目的是让重试的客户端拿到相同响应。在 openraft
中，每条日志有唯一 index；重试的写会追加新日志但 openraft 可以检测
重复。方案：
- client_results 改为 HashMap + 定期批量清理（不是 BTreeMap 逐条淘汰）
- 或者：仅在显式 retry 场景启用（客户端重连时重放），正常流不查

**影响**：每条写减少 1 次 BTreeMap get + 1 次 BTreeMap insert
+ 淘汰检查。**预期收益**：apply 每条 -0.5-1µs。
**风险**：中。幂等语义需要验证。

### Phase 3：prepare 瘦身

**设计**：`prepare_client_batch_with_limits` 每条做：
- `bincode::serialized_size(&(request_id, request))` — 走整个结构
- `request_memory_bytes()` — 逐参数求和
- `batchable_write_has_bounded_response()` — 命令分类

优化：SET/MSET/DEL 等常见命令的序列化大小用公式直接计算
（key_len + value_len + 固定偏移），不走 bincode。

**影响**：prepare 每条 -0.5-1µs。
**风险**：低。公式覆盖所有命令类型。

### Phase 4：Frame 构造与编码优化

**设计**：`Frame::simple_string("OK")` 已经用 `Bytes::from_static`（无
堆分配）。但 `buffer_response` 的 `encode_for` 调度（match 语句 + 逐
字段写入）可以预编码。对于 `+OK\r\n`（5 字节），可以直接 extend。

**影响**：每条响应 -0.2-0.5µs。
**风险**：极低。

### Phase 5：WAL 批量校验和

**设计**：FileLogStore 的 `append_record` 每条记录计算逐字节 fold
checksum。改为批量追加多条记录后计算一个批量校验和。

**影响**：每条 -0.2µs。
**风险**：中。需要验证崩溃恢复语义。

## 实施顺序

Phase 1 → 2 → 3 → 4 → 5，每阶段独立验证 + 实测。
Phase 1+2 合计预期将 apply 每条从 ~3.7µs 降到 ~1.5-2µs。
写吞吐预期从 ~104k 提升到 ~150-200k。

## 实施记录

### Phase 1 完成（2026-10-18，76d1c91）

versions HashMap 消除，版本合并到 Entry。微基准 3.7-5.3 → **2.4µs/op
（418k ops/s，-35%）**。集群新鲜态：5% 写（95% 读）= **250-261k**
（同机 cache-cat 203-236k，**达到/超越**）；50/50 = 38-44k；纯写预载
97,960。全部零错误。172 测试全绿。

### Phase 2（待实施）
### Phase 3（待实施）
### Phase 4（待实施）
### Phase 5（待实施）

## 完成标准

- 50/50 ≥ 100k、5/95 ≥ 150k（同机同负载）
- 172+ 项功能测试全绿；WATCH/事务/幂等语义不回退
- 快照序列化/恢复不回退
