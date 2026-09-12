#!/usr/bin/env node
/**
 * 岩芯切片制备批次台 —— 端到端走查
 *
 * 覆盖：建档 → 批量添加（含逾期样）→ 全流程推进 → 并发重复提交 →
 *       跳步/回退/空观察交付拦截（原状态保留 + 拒绝留痕）→
 *       观察 → 缺陷登记/处理 → 交付 → 审计查询 → 重启持久化
 *
 * 用法：node demo-walkthrough.mjs
 * 脚本会自管一个使用独立数据文件的服务进程（端口 3099），不影响 3025 的正式实例。
 */
import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { rmSync, existsSync } from "node:fs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const BASE = process.env.BASE_URL || "http://127.0.0.1:3099";
const DEMO_DB = join(__dirname, "data", "demo-walkthrough.json");

let passed = 0, failed = 0;
// HTTP 头不允许非 ASCII，幂等键用稳定内容做 ASCII 哈希
function idem(parts) {
  const str = parts.filter(x => x != null).join("|");
  let h = 5381;
  for (let i = 0; i < str.length; i++) h = ((h << 5) + h + str.charCodeAt(i)) >>> 0;
  return "idem-" + h.toString(36) + "-" + Buffer.from(str).length;
}
function check(name, cond, detail = "") {
  if (cond) { passed++; console.log(`  ✅ ${name}${detail ? " — " + detail : ""}`); }
  else { failed++; console.log(`  ❌ ${name}${detail ? " — " + detail : ""}`); }
}
function section(t) { console.log(`\n${"─".repeat(72)}\n${t}\n${"─".repeat(72)}`); }

async function api(path, { method = "GET", body, idemKey } = {}) {
  const headers = {};
  if (body) headers["Content-Type"] = "application/json";
  if (idemKey) headers["Idempotency-Key"] = idemKey;
  const res = await fetch(BASE + path, {
    method,
    headers,
    body: body ? JSON.stringify(body) : undefined,
  });
  const data = await res.json().catch(() => ({}));
  return { status: res.status, replayed: res.headers.get("Idempotent-Replay") === "true", data };
}

function startServer() {
  if (existsSync(DEMO_DB)) rmSync(DEMO_DB);
  const proc = spawn(process.execPath, [join(__dirname, "server.js")], {
    env: {
      ...process.env,
      PORT: "3099",
      DB_PATH: DEMO_DB,
      SLA_SAMPLING: "8", SLA_CUTTING: "8", SLA_GRINDING: "8",
      SLA_STAINING: "8", SLA_OBSERVE: "8", SLA_DELIVER: "8",
    },
    stdio: ["ignore", "pipe", "inherit"],
  });
  return proc;
}
async function waitReady(proc, tries = 40) {
  for (let i = 0; i < tries; i++) {
    try {
      const r = await fetch(BASE + "/api/meta");
      if (r.ok) return;
    } catch { /* 未就绪 */ }
    if (proc.exitCode != null) throw new Error("服务进程提前退出");
    await sleep(250);
  }
  throw new Error("服务启动超时");
}

const proc = startServer();
process.on("exit", () => { try { proc.kill(); } catch {} });
await waitReady(proc);
console.log("演示服务已就绪：" + BASE);

try {
  // ============ 1. 建档 ============
  section("1. 录入岩芯样本");
  const s = await api("/api/samples", { method: "POST", body: {
    project: "东岭铜矿薄片鉴定", borehole: "ZK-17", coreBox: "BX-09",
    depth: "128.4-128.8m", rockType: "含铜矽卡岩", owner: "陆川",
  }});
  check("样本创建 201", s.status === 201, s.data.sample?.id);
  const sampleId = s.data.sample.id;

  section("2. 建立制备批次");
  const b = await api("/api/batches", { method: "POST", body: {
    sampleId, name: "ZK-17 含矿段批次", plan: "《岩矿制片作业规范》2026版", createdBy: "陆川",
  }});
  check("批次创建 201", b.status === 201, b.data.batch?.id);
  const batchId = b.data.batch.id;

  // ============ 3. 批量添加切片 ============
  section("3. 按批次一次建多张切片（4 张，其中 SL-3/SL-4 回溯 200 小时制造逾期）");
  const add = await api(`/api/batches/${batchId}/slices`, { method: "POST", body: {
    operator: "取样工 韩梅",
    basis: "批次制片委托单 WT-2026-017",
    slices: [
      { label: "切片1-矿化条带", stainMethod: "茜素红-S", backdateHours: 0 },
      { label: "切片2-围岩", stainMethod: "茜素红-S", backdateHours: 0 },
      { label: "切片3-脉体(早班取样)", stainMethod: "茜素红-S", backdateHours: 200 },
      { label: "切片4-接触带(早班取样)", stainMethod: "茜素红-S", backdateHours: 200 },
    ],
  }, idemKey: idem(["addslices", batchId, "init"]) });
  check("批量添加 201 且返回 4 张", add.status === 201 && add.data.created.length === 4);
  const [sl1, sl2, sl3, sl4] = add.data.created;
  check("新切片全部进入「取样」工序", add.data.created.every(x => x.stage === "取样"));
  check("回溯切片被计算为逾期", sl3.overdueHours != null && sl4.overdueHours != null,
    `逾期 ${sl3.overdueHours}h / ${sl4.overdueHours}h`);
  check("批次页统计逾期数=2", add.data.batch.stats.overdueCount === 2);
  check("批次页下一步负责人含取样工",
    add.data.batch.nextOwners.some(o => o.role === "取样工" && o.count === 4),
    JSON.stringify(add.data.batch.nextOwners));

  // 同键重放：不产生第二批切片
  const replay = await api(`/api/batches/${batchId}/slices`, { method: "POST", body: {
    operator: "取样工 韩梅", basis: "批次制片委托单 WT-2026-017",
    slices: [{ label: "切片1-矿化条带", stainMethod: "茜素红-S" }],
  }, idemKey: idem(["addslices", batchId, "init"]) });
  check("同幂等键重放命中首次结果", replay.replayed === true && replay.data.created.length === 4);

  const basisFor = {
    取样: "取样作业票 SMP-2026-017",
    切割: "切割作业票 CUT-2026-017",
    研磨: "研磨作业票 GRD-2026-017",
    染色: "染色作业票 STN-2026-017",
    观察: "岩矿鉴定任务单 OBS-2026-017",
    待交付: "制片完成移交单",
  };
  const operatorFor = {
    取样: "取样工 韩梅", 切割: "切片工 周铎", 研磨: "磨片工 白兰",
    染色: "染色工 叶青", 观察: "岩矿鉴定师 顾鉴", 待交付: "资料员 方澄",
  };
  async function advance(sliceId, from, target) {
    const body = { operator: operatorFor[target], basis: basisFor[target], targetStage: target };
    return api(`/api/slices/${sliceId}/advance`, { method: "POST", body,
      idemKey: idem([sliceId, from, body.basis, body.targetStage]) });
  }

  // ============ 4. 非法推进：跳步 / 回退 ============
  section("4. 非法推进必须失败且原状态保留");
  const skip = await advance(sl1.id, "取样", "切割"); // 正常推进一次，制造可回退场景
  check("取样→切割 成功", skip.status === 200 && skip.data.after === "切割");

  const jump = await advance(sl1.id, "切割", "染色"); // 跳步：越过研磨
  check("跳步 切割→染色 返回 409 skip_denied", jump.status === 409 && jump.data.error === "skip_denied", jump.data.message);

  const back = await advance(sl1.id, "切割", "取样"); // 回退
  check("回退 切割→取样 返回 409 rollback_denied", back.status === 409 && back.data.error === "rollback_denied", back.data.message);

  const sl1Now1 = (await api(`/api/slices/${sl1.id}`)).data;
  check("被拒后原状态保留（仍为切割）", sl1Now1.stage === "切割");

  // 已交付切片的操作在交付后测试（见第 9 步）

  // ============ 5. 并发重复推进只生效一次 ============
  section("5. 同一推进请求并发 ×8 重复提交，只生效一次");
  const key = idem([sl2.id, "取样", basisFor["切割"], "切割"]);
  const concurrent = await Promise.all(Array.from({ length: 8 }, () =>
    api(`/api/slices/${sl2.id}/advance`, { method: "POST", body: {
      operator: operatorFor["切割"], basis: basisFor["切割"], targetStage: "切割",
    }, idemKey: key })));
  const accepted = concurrent.filter(r => r.status === 200 && !r.replayed);
  const replays = concurrent.filter(r => r.replayed === true);
  check("恰好 1 次真正执行", accepted.length === 1, `accepted=${accepted.length}, replay=${replays.length}`);
  check("其余 7 次重放首次响应", replays.length === 7);
  const sl2Now = (await api(`/api/slices/${sl2.id}`)).data;
  check("工序只推进一格（取样→切割）", sl2Now.stage === "切割");
  const advAudits = (await api(`/api/audits?type=advance&sliceId=${sl2.id}&result=accepted`)).data;
  check("成功推进审计仅 1 条", advAudits.total === 1,
    `operator=${advAudits.items[0]?.operator}, basis=${advAudits.items[0]?.basis}`);
  check("审计记录前状态=取样、后状态=切割",
    advAudits.items[0]?.fromStage === "取样" && advAudits.items[0]?.toStage === "切割");

  // 不带幂等键的重复推进：第二次因状态已变而必然失败（状态机本身也只允许一次）
  const dup1 = await api(`/api/slices/${sl2.id}/advance`, { method: "POST", body: {
    operator: operatorFor["切割"], basis: basisFor["切割"], targetStage: "切割" } });
  check("无键重复提交同样无法二次生效（409）", dup1.status === 409, dup1.data.error);

  // ============ 6. 正常推进整条流水线 ============
  section("6. SL-1 正常走完 切割→研磨→染色→观察→待交付");
  let cur = (await api(`/api/slices/${sl1.id}`)).data.stage;
  for (const target of ["研磨", "染色", "观察", "待交付"]) {
    const r = await advance(sl1.id, cur, target);
    check(`${cur}→${target} 成功`, r.status === 200, `操作人=${operatorFor[target]}，依据=${basisFor[target]}`);
    cur = target;
  }
  const sl1Now2 = (await api(`/api/slices/${sl1.id}`)).data;
  check("SL-1 到达「待交付」", sl1Now2.stage === "待交付");
  check("每步记录进入时间与 SLA 时限", !!sl1Now2.enteredStageAt && !!sl1Now2.dueAt);

  // ============ 7. 空观察交付失败 ============
  section("7. 空观察结果交付必须失败且保留状态");
  const emptyDeliver = await api(`/api/slices/${sl1.id}/deliver`, { method: "POST", body: {
    operator: "资料员 方澄", basis: "成果移交单 DEL-2026-001" } });
  check("空观察交付 409 empty_observation",
    emptyDeliver.status === 409 && emptyDeliver.data.error === "empty_observation",
    emptyDeliver.data.message);
  const sl1Now3 = (await api(`/api/slices/${sl1.id}`)).data;
  check("交付被拒后状态仍为「待交付」", sl1Now3.stage === "待交付");

  // ============ 8. 登记观察结果 ============
  section("8. 岩矿鉴定师登记观察结果");
  const obs = await api(`/api/slices/${sl1.id}/observation`, { method: "POST", body: {
    operator: "岩矿鉴定师 顾鉴",
    basis: "岩矿鉴定任务单 OBS-2026-017",
    observation: "镜下：他形-半自形晶粒状结构，黄铜矿呈脉状沿透辉石粒间充填，"
      + "脉宽0.1-0.4mm，伴生磁铁矿与孔雀石化；见黄铁矿残留。",
  }});
  check("观察登记成功，observation 落库", obs.status === 200 && /黄铜矿/.test(obs.data.slice.observation));
  check("记录观察人与观察时间", obs.data.slice.observedBy === "岩矿鉴定师 顾鉴" && !!obs.data.slice.observedAt);

  const badObs = await api(`/api/slices/${sl2.id}/observation`, { method: "POST", body: {
    operator: "岩矿鉴定师 顾鉴", basis: "OBS", observation: "" } });
  check("非观察工序/空观察登记被拒", badObs.status === 409 || badObs.status === 400, badObs.data.message);

  // ============ 9. 缺陷登记 + 处理 ============
  section("9. 缺陷登记、批次页未处理缺陷统计、缺陷处理");
  const defect = await api(`/api/slices/${sl1.id}/defects`, { method: "POST", body: {
    operator: "岩矿鉴定师 顾鉴", basis: "镜下复检", level: "轻微",
    description: "盖玻片边缘有少量气泡，不影响矿化条带观察区域" } });
  check("缺陷登记 201", defect.status === 201, defect.data.defect?.id);
  const defectId = defect.data.defect.id;

  const batchMid = (await api(`/api/batches/${batchId}`)).data;
  check("批次页未处理缺陷数=1", batchMid.stats.openDefectCount === 1);
  check("批次页 openDefects 含描述与级别",
    batchMid.openDefects[0]?.description.includes("气泡") && batchMid.openDefects[0]?.level === "轻微");
  check("批次页进度为有限百分比且 SL-1 未交付",
    batchMid.stats.progress > 0 && batchMid.stats.delivered === 0,
    `progress=${batchMid.stats.progress}%`);

  const resolve = await api(`/api/defects/${defectId}/resolve`, { method: "POST", body: {
    operator: "磨片工 白兰", resolution: "重新封片排气后复检合格" } });
  check("缺陷处理成功", resolve.status === 200 && resolve.data.defect.status === "已处理");

  // ============ 10. 交付 ============
  section("10. 有观察结果后交付成功；已交付不可再推进");
  const deliver = await api(`/api/slices/${sl1.id}/deliver`, { method: "POST", body: {
    operator: "资料员 方澄", basis: "成果移交单 DEL-2026-001" } });
  check("交付 200，状态=已交付", deliver.status === 200 && deliver.data.after === "已交付");
  check("记录交付人/交付时间", !!deliver.data.slice.deliveredBy && !!deliver.data.slice.deliveredAt);

  const reAdvance = await api(`/api/slices/${sl1.id}/advance`, { method: "POST", body: {
    operator: "资料员 方澄", basis: "x", targetStage: "切割" } });
  check("已交付切片再推进返回 409", reAdvance.status === 409, reAdvance.data.error);
  const reDeliver = await api(`/api/slices/${sl1.id}/deliver`, { method: "POST", body: {
    operator: "资料员 方澄", basis: "成果移交单 DEL-2026-001" },
  idemKey: idem(["deliver", sl1.id, "again"]) });
  check("重复交付 409 already_delivered", reDeliver.status === 409 && reDeliver.data.error === "already_delivered");

  // ============ 11. 审计查询 ============
  section("11. 审计台账查询");
  const all = await api("/api/audits?limit=500");
  const rejected = all.data.items.filter(r => r.result === "rejected");
  check("拒绝操作全部留痕（跳步/回退/空观察/重复交付/无键重复/非法观察）",
    rejected.some(r => r.rejectCode === "skip_denied") &&
    rejected.some(r => r.rejectCode === "rollback_denied") &&
    rejected.some(r => r.rejectCode === "empty_observation") &&
    rejected.some(r => r.rejectCode === "already_delivered"),
    `拒绝记录共 ${rejected.length} 条`);
  const rejSample = rejected.find(r => r.rejectCode === "skip_denied" && r.fromStage === "切割" && r.toStage === "染色");
  check("拒绝记录含操作人/依据/前后状态",
    !!rejSample && rejSample.operator && rejSample.basis && rejSample.fromStage === "切割" && rejSample.toStage === "染色",
    rejSample ? `${rejSample.operator} / ${rejSample.basis} / ${rejSample.fromStage}→${rejSample.toStage}` : "未找到跳步拒绝记录");

  const byOp = await api("/api/audits?operator=" + encodeURIComponent("岩矿鉴定师 顾鉴") + "&limit=500");
  check("按操作人过滤", byOp.data.items.length > 0 && byOp.data.items.every(r => r.operator === "岩矿鉴定师 顾鉴"));
  const byResult = await api("/api/audits?result=accepted&type=advance&limit=500");
  check("按类型+结果组合过滤", byResult.data.items.every(r => r.type === "advance" && r.result === "accepted"));
  const bySlice = await api(`/api/slices/${sl1.id}`).then(() => api(`/api/audits?sliceId=${sl1.id}&limit=500`));
  check("按切片过滤可还原完整履历", bySlice.data.items.length >= 9,
    `${bySlice.data.total} 条（建片/推进/观察/缺陷/交付/拒绝尝试）`);
  const timeWindow = await api(`/api/audits?from=2099-01-01&limit=500`);
  check("时间窗过滤生效（2099 年后应为 0 条）", timeWindow.data.total === 0);

  // ============ 12. 重启持久化 ============
  section("12. 重启后数据保留");
  proc.kill();
  await sleep(600);
  const proc2 = startServer2();
  await waitReady(proc2);
  const batchAfter = (await api(`/api/batches/${batchId}`)).data;
  const sl1After = batchAfter.slices.find(x => x.id === sl1.id);
  check("批次/切片重启后仍在", !!sl1After);
  check("SL-1 状态仍为已交付，观察与交付信息保留",
    sl1After.stage === "已交付" && /黄铜矿/.test(sl1After.observation) && sl1After.deliveredBy === "资料员 方澄");
  check("逾期统计重启后可重算", batchAfter.stats.overdueCount >= 2,
    `overdueCount=${batchAfter.stats.overdueCount}`);
  const auditsAfter = await api("/api/audits?sliceId=" + sl1.id + "&limit=500");
  check("审计履历重启后完整", auditsAfter.data.total >= 9, `${auditsAfter.data.total} 条`);
  const defectsAfter = await api("/api/defects");
  check("缺陷处理结果保留", defectsAfter.data.some(d => d.id === defectId && d.status === "已处理"));
  // 重启后旧幂等键仍然生效（防重启期间客户端重试造成双份）
  const replayAfter = await api(`/api/batches/${batchId}/slices`, { method: "POST", body: {
    operator: "取样工 韩梅", basis: "批次制片委托单 WT-2026-017",
    slices: [{ label: "不应再建", stainMethod: "茜素红-S" }],
  }, idemKey: idem(["addslices", batchId, "init"]) });
  const countAfter = (await api(`/api/batches/${batchId}`)).data.stats.total;
  check("重启后同幂等键仍重放，不新增切片", replayAfter.replayed === true && countAfter === 4, `切片总数=${countAfter}`);
  proc2.kill();
} catch (e) {
  console.error("走查异常：", e);
  failed++;
} finally {
  section(`结果：${passed} 通过，${failed} 失败`);
  try { proc.kill(); } catch {}
  process.exit(failed ? 1 : 0);
}

function startServer2() {
  // 复用同一数据文件，验证持久化
  const p = spawn(process.execPath, [join(__dirname, "server.js")], {
    env: {
      ...process.env,
      PORT: "3099", DB_PATH: DEMO_DB,
      SLA_SAMPLING: "8", SLA_CUTTING: "8", SLA_GRINDING: "8",
      SLA_STAINING: "8", SLA_OBSERVE: "8", SLA_DELIVER: "8",
    },
    stdio: ["ignore", "pipe", "inherit"],
  });
  process.on("exit", () => { try { p.kill(); } catch {} });
  return p;
}
