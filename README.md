# 岩芯切片制备批次台

地质实验室切片岗的批次制片管理：录入岩芯样本 → 按批次一次建多张切片 → 每张切片按
**取样 → 切割 → 研磨 → 染色 → 观察 → 交付** 推进，全程留痕。

零依赖，Node.js 原生 ESM + JSON 文件持久化（原子写，重启不丢数据）。

## 运行

```bash
npm start          # http://localhost:3025
```

端到端走查（自管 3099 端口与独立数据文件，不影响正式实例）：

```bash
node demo-walkthrough.mjs
```

## 功能与规则

- **建档**：录入样本（项目/钻孔/岩芯箱/深度/岩性/负责人），再建立制备批次。
- **批量添加**：一次请求在批次下创建多张切片，统一指定染色方法、操作人与依据，全部进入「取样」工序。
- **工序推进**：每次推进必须提交操作人、依据（作业票/委托单编号）；系统记录时间、前状态、后状态到审计台账。
- **批次台**：显示总体进度（百分比/已交付数）、逾期项（停留超 SLA 的切片、超期小时数、责任岗）、
  下一步负责人（按当前工序汇总人数）、未处理缺陷，支持缺陷登记与处理闭环。
- **观察与交付**：岩矿鉴定师在观察/待交付工序登记镜下观察结果；**观察结果为空时交付被拒绝**。
- **缺陷**：任何工序都可登记缺陷（轻微/一般/严重），批次页汇总未处理项，处理需填写处理说明。
- **审计查询**：支持按类型、成功/拒绝、操作人、切片、批次、时间窗过滤；**被拒绝的违规操作同样留痕**。
- **逾期（SLA）**：各工序标准时限（取样 24h、切割 48h、研磨 72h、染色 24h、观察 48h、待交付 24h），
  可用环境变量 `SLA_SAMPLING`/`SLA_CUTTING`/`SLA_GRINDING`/`SLA_STAINING`/`SLA_OBSERVE`/`SLA_DELIVER` 覆盖。

### 强约束（失败时原状态保留，并写拒绝审计）

| 场景 | 结果 |
|---|---|
| 跳步（如 切割→染色 越过研磨） | `409 skip_denied` |
| 回退（如 切割→取样） | `409 rollback_denied` |
| 空观察结果交付 | `409 empty_observation` |
| 已交付再推进/再交付 | `409 already_delivered` |
| 非观察阶段登记观察 | `409 not_observing` |

### 并发与幂等

- 写操作经全局事务锁串行化，排队请求读到的始终是最新落盘状态。
- 推进等写请求支持 `Idempotency-Key` 头（或请求体 `idempotencyKey`）：同一键的并发/重复提交只真正执行一次，
  其余重放首次响应（响应头 `Idempotent-Replay: true`），不产生重复审计；幂等记录持久化，重启后依然生效。
- 即使不带幂等键，状态机本身也保证重复推进无法二次生效。

## API 摘要

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/samples` | 录入样本 |
| POST | `/api/batches` | 建立批次 |
| POST | `/api/batches/:id/slices` | 批量添加切片（`count` 或 `slices[]`，支持 `backdateHours` 造逾期演示） |
| GET | `/api/batches` / `/api/batches/:id` | 批次页数据（进度/逾期/负责人/缺陷） |
| POST | `/api/slices/:id/advance` | 推进工序（`operator`/`basis`/`targetStage`/`note`） |
| POST | `/api/slices/:id/observation` | 登记观察结果 |
| POST | `/api/slices/:id/defects` | 登记缺陷 |
| POST | `/api/defects/:id/resolve` | 处理缺陷 |
| POST | `/api/slices/:id/deliver` | 交付 |
| GET | `/api/audits` | 审计查询（`type/result/operator/sliceId/batchId/from/to/limit`） |

数据文件：`data/core-slices.json`（可用 `DB_PATH` 环境变量改位置）。
