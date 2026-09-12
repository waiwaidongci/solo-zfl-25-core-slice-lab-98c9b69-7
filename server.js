import http from "node:http";
import { mkdir, readFile, writeFile, rename } from "node:fs/promises";
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { readFileSync } from "node:fs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const dbPath = process.env.DB_PATH || join(__dirname, "data", "core-slices.json");
const port = Number(process.env.PORT || 3025);

// 制片流水线：取样 → 切割 → 研磨 → 染色 → 观察 → 待交付 → 已交付
const STAGES = ["取样", "切割", "研磨", "染色", "观察", "待交付", "已交付"];
const ACTIVE_STAGES = ["取样", "切割", "研磨", "染色", "观察"];
// 每道工序的责任岗，批次页“下一步负责人”按此映射
const STAGE_OWNERS = {
  取样: "取样工",
  切割: "切片工",
  研磨: "磨片工",
  染色: "染色工",
  观察: "岩矿鉴定师",
  待交付: "资料员",
  已交付: null,
};
// 各岗标准作业时限（小时），可用环境变量覆盖，便于演示逾期
const SLA_HOURS = {
  取样: Number(process.env.SLA_SAMPLING || 24),
  切割: Number(process.env.SLA_CUTTING || 48),
  研磨: Number(process.env.SLA_GRINDING || 72),
  染色: Number(process.env.SLA_STAINING || 24),
  观察: Number(process.env.SLA_OBSERVE || 48),
  待交付: Number(process.env.SLA_DELIVER || 24),
};
const DEFECT_LEVELS = ["轻微", "一般", "严重"];

const emptyDb = () => ({
  version: 2,
  seq: { sample: 0, batch: 0, slice: 0, defect: 0 },
  samples: [],
  batches: [],
  slices: [],
  defects: [],
  // 审计台账：每次推进/交付/观察/缺陷处置都留痕（含被拒绝的操作）
  audits: [],
  // 幂等键 → 原始响应，保证并发重复提交只生效一次
  idempotency: {},
});

// ---------- 持久化：全量读 + 原子写（tmp + rename），重启后保留 ----------
async function loadDb() {
  if (!existsSync(dbPath)) {
    await mkdir(dirname(dbPath), { recursive: true });
    const db = emptyDb();
    await persist(db);
    return db;
  }
  const db = JSON.parse(await readFile(dbPath, "utf8"));
  if (!db || db.version !== 2) {
    // 旧版本数据不兼容：备份后重建
    await writeFile(dbPath + ".v1.bak", JSON.stringify(db, null, 2)).catch(() => {});
    const fresh = emptyDb();
    await persist(fresh);
    return fresh;
  }
  return db;
}
async function persist(db) {
  await mkdir(dirname(dbPath), { recursive: true });
  const tmp = `${dbPath}.tmp-${process.pid}`;
  await writeFile(tmp, JSON.stringify(db, null, 2));
  await rename(tmp, dbPath);
}

// ---------- 写事务串行化：同一时刻只允许一个变更落库 ----------
let writeChain = Promise.resolve();
function withLock(fn) {
  const run = writeChain.then(() => fn());
  // 不让单个失败污染后续请求的链
  writeChain = run.then(() => {}, () => {});
  return run;
}

function nowIso() {
  return new Date().toISOString();
}
function nextId(db, kind, prefix) {
  db.seq[kind] = (db.seq[kind] || 0) + 1;
  return `${prefix}${String(db.seq[kind]).padStart(4, "0")}`;
}
function findSample(db, id) {
  return db.samples.find((s) => s.id === id);
}
function findBatch(db, id) {
  return db.batches.find((b) => b.id === id);
}
function findSlice(db, id) {
  return db.slices.find((s) => s.id === id);
}

// 切片完成当前工序、进入下一道工序时，重新计算该工序的时限
function setStage(slice, stage, at) {
  slice.stage = stage;
  slice.enteredStageAt = at;
  slice.dueAt = stage && SLA_HOURS[stage] != null
    ? new Date(new Date(at).getTime() + SLA_HOURS[stage] * 3600 * 1000).toISOString()
    : null;
}

function recordAudit(db, entry) {
  const id = `AUD-${String(db.audits.length + 1).padStart(6, "0")}`;
  const row = { id, at: nowIso(), ...entry };
  db.audits.push(row);
  return row;
}

function sliceView(db, slice) {
  const openDefects = db.defects.filter(
    (d) => d.sliceId === slice.id && d.status === "未处理"
  );
  let overdueHours = null;
  if (slice.dueAt && slice.stage !== "已交付") {
    const ms = Date.now() - new Date(slice.dueAt).getTime();
    if (ms > 0) overdueHours = Math.round((ms / 3600000) * 10) / 10;
  }
  return {
    ...slice,
    nextOwner: STAGE_OWNERS[slice.stage] || null,
    overdueHours,
    openDefectCount: openDefects.length,
    openDefects,
  };
}

function batchView(db, batch) {
  const slices = db.slices.filter((s) => s.batchId === batch.id);
  const views = slices.map((s) => sliceView(db, s));
  const total = views.length;
  const delivered = views.filter((s) => s.stage === "已交付").length;
  const order = STAGES;
  const sumStageIndex = views.reduce((acc, s) => acc + order.indexOf(s.stage), 0);
  // 进度：每道工序等权，已交付为全部 6 段
  const progress = total ? Math.round((sumStageIndex / (total * (order.length - 1))) * 100) : 0;
  const overdue = views.filter((s) => s.overdueHours != null);
  const openDefects = db.defects.filter(
    (d) => d.batchId === batch.id && d.status === "未处理"
  );
  // 下一步负责人：尚未交付切片当前岗位的并集，附人数
  const ownerTally = new Map();
  for (const s of views) {
    if (s.stage !== "已交付" && s.nextOwner) {
      ownerTally.set(s.nextOwner, (ownerTally.get(s.nextOwner) || 0) + 1);
    }
  }
  const nextOwners = [...ownerTally.entries()].map(([role, count]) => ({ role, count }));
  return {
    ...batch,
    sample: findSample(db, batch.sampleId) || null,
    slices: views,
    stats: {
      total,
      delivered,
      progress,
      overdueCount: overdue.length,
      openDefectCount: openDefects.length,
    },
    overdue: overdue.map((s) => ({
      sliceId: s.id,
      label: s.label,
      stage: s.stage,
      dueAt: s.dueAt,
      overdueHours: s.overdueHours,
      nextOwner: s.nextOwner,
    })),
    openDefects,
    nextOwners,
  };
}

function sampleView(db, sample) {
  const batches = db.batches
    .filter((b) => b.sampleId === sample.id)
    .map((b) => batchView(db, b));
  const sliceCount = batches.reduce((n, b) => n + b.stats.total, 0);
  const delivered = batches.reduce((n, b) => n + b.stats.delivered, 0);
  return { ...sample, batches, sliceCount, delivered };
}

// ---------- 业务规则 ----------
class HttpError extends Error {
  constructor(status, code, message, extra = {}) {
    super(message || code);
    this.status = status;
    this.code = code;
    this.extra = extra;
  }
}
function requireStr(input, key, label) {
  const v = input[key];
  if (typeof v !== "string" || !v.trim()) {
    throw new HttpError(400, "missing_field", `${label || key}不能为空`);
  }
  return v.trim();
}

// 推进状态机：只允许进入严格相邻的下一工序；跳步、回退一律拒绝且状态不变
function advanceStage(db, slice, input) {
  const operator = requireStr(input, "operator", "操作人");
  const basis = requireStr(input, "basis", "依据");
  const target = typeof input.targetStage === "string" ? input.targetStage.trim() : null;

  if (slice.stage === "已交付") {
    throw new HttpError(409, "already_delivered", "切片已交付，不能继续推进", {
      stage: slice.stage,
    });
  }
  const idx = STAGES.indexOf(slice.stage);
  const expectedNext = STAGES[idx + 1];
  if (target && target !== expectedNext) {
    // 显式目标工序不是紧邻下一步 → 判定跳步或回退
    const targetIdx = STAGES.indexOf(target);
    const code = targetIdx >= 0 && targetIdx < idx ? "rollback_denied" : "skip_denied";
    throw new HttpError(409, code,
      `非法推进：${slice.stage} → ${target}，只允许进入${expectedNext}`,
      { stage: slice.stage, targetStage: target, expectedNext });
  }

  const before = slice.stage;
  const at = nowIso();
  const note = typeof input.note === "string" ? input.note.trim() : "";

  // 进入“观察”工序时可以同时登记初步观察；空观察不允许交付，但允许先推进
  if (expectedNext === "观察" && typeof input.observation === "string" && input.observation.trim()) {
    slice.observation = input.observation.trim();
    slice.observedBy = operator;
    slice.observedAt = at;
  }

  setStage(slice, expectedNext, at);
  slice.updatedAt = at;
  recordAudit(db, {
    type: "advance",
    result: "accepted",
    operator,
    basis,
    sliceId: slice.id,
    batchId: slice.batchId,
    fromStage: before,
    toStage: expectedNext,
    note,
    batchNo: slice.batchNo,
    label: slice.label,
  });
  return { slice: sliceView(db, slice), before, after: expectedNext };
}

// 观察记录：岩矿鉴定师在“观察”工序填写鉴定结果，是交付的前置条件
function recordObservation(db, slice, input) {
  const operator = requireStr(input, "operator", "操作人");
  const basis = requireStr(input, "basis", "依据");
  const observation = requireStr(input, "observation", "观察结果");
  if (slice.stage !== "观察" && slice.stage !== "待交付") {
    throw new HttpError(409, "not_observing",
      `当前工序为「${slice.stage}」，完成染色进入观察工序后才可登记观察结果`,
      { stage: slice.stage });
  }
  const before = slice.stage;
  const at = nowIso();
  slice.observation = observation;
  slice.observedBy = operator;
  slice.observedAt = at;
  slice.updatedAt = at;
  recordAudit(db, {
    type: "observe",
    result: "accepted",
    operator,
    basis,
    sliceId: slice.id,
    batchId: slice.batchId,
    fromStage: before,
    toStage: before,
    note: observation.slice(0, 200),
    batchNo: slice.batchNo,
    label: slice.label,
  });
  return { slice: sliceView(db, slice) };
}

// 交付：必须先过观察工序且观察结果非空，否则拒绝并保留状态
function deliverSlice(db, slice, input) {
  const operator = requireStr(input, "operator", "操作人");
  const basis = requireStr(input, "basis", "依据");
  const before = slice.stage;

  if (before === "已交付") {
    throw new HttpError(409, "already_delivered", "切片已交付", { stage: before });
  }
  if (before !== "待交付") {
    throw new HttpError(409, "skip_denied",
      `非法交付：当前工序为「${before}」，必须完成全部制片与观察工序`,
      { stage: before, expectedNext: "待交付" });
  }
  if (!slice.observation || !slice.observation.trim()) {
    // 空观察交付：失败且原状态保留
    throw new HttpError(409, "empty_observation",
      "观察结果为空，禁止交付：请先由岩矿鉴定师登记观察结果",
      { stage: before });
  }
  const at = nowIso();
  setStage(slice, "已交付", at);
  slice.deliveredBy = operator;
  slice.deliveredAt = at;
  slice.updatedAt = at;
  recordAudit(db, {
    type: "deliver",
    result: "accepted",
    operator,
    basis,
    sliceId: slice.id,
    batchId: slice.batchId,
    fromStage: before,
    toStage: "已交付",
    batchNo: slice.batchNo,
    label: slice.label,
  });
  return { slice: sliceView(db, slice), before, after: "已交付" };
}

// ---------- HTTP 辅助 ----------
async function readBody(req) {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  if (!chunks.length) return {};
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    throw new HttpError(400, "bad_json", "请求体不是合法 JSON");
  }
}
function sendJson(res, status, data) {
  res.writeHead(status, { "Content-Type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(data));
}
// 失败操作同样写入审计台账，便于追溯违规尝试
function auditRejection(db, type, input, slice, code, message, extra) {
  recordAudit(db, {
    type,
    result: "rejected",
    operator: typeof input.operator === "string" ? input.operator : "(未提供)",
    basis: typeof input.basis === "string" ? input.basis : "(未提供)",
    sliceId: slice ? slice.id : (typeof input.sliceId === "string" ? input.sliceId : null),
    batchId: slice ? slice.batchId : (typeof input.batchId === "string" ? input.batchId : null),
    fromStage: slice ? slice.stage : null,
    toStage: typeof input.targetStage === "string" ? input.targetStage
      : type === "deliver" ? "已交付" : null,
    rejectCode: code,
    rejectReason: message,
    batchNo: slice ? slice.batchNo : null,
    label: slice ? slice.label : null,
    ...extra,
  });
}

// 幂等：同一幂等键的并发/重复请求只真正执行一次，其余重放首次响应
// 读库发生在串行锁内，保证排队请求拿到的是前一个请求落盘后的最新状态
function mutation(req, res, handler, resourceKey) {
  return withLock(async () => {
    const db = await loadDb();
    const body = await readBody(req);
    const idemKey = req.headers["idempotency-key"]
      ? String(req.headers["idempotency-key"])
      : (typeof body.idempotencyKey === "string" && body.idempotencyKey.trim()
        ? body.idempotencyKey.trim() : null);
    if (idemKey) {
      const stored = db.idempotency[idemKey];
      if (stored) {
        res.setHeader("Idempotent-Replay", "true");
        return sendJson(res, stored.status, stored.body);
      }
    }
    let status = 200, out;
    try {
      out = await handler(body, db, idemKey);
      status = out && out.__status ? out.__status : 200;
      if (out && out.__status) delete out.__status;
    } catch (err) {
      await persist(db).catch(() => {}); // 保留拒绝审计
      if (err instanceof HttpError) return sendJson(res, err.status, { error: err.code, message: err.message, ...err.extra });
      throw err;
    }
    if (idemKey) db.idempotency[idemKey] = { status, body: out, resourceKey: resourceKey || null, at: nowIso() };
    await persist(db);
    return sendJson(res, status, out);
  });
}

// ---------- 页面 ----------
const pageHtml = readFileSync(join(__dirname, "public", "index.html"), "utf8")
  .replace("__STAGES__", JSON.stringify(STAGES))
  .replace("__ACTIVE_STAGES__", JSON.stringify(ACTIVE_STAGES))
  .replace("__STAGE_OWNERS__", JSON.stringify(STAGE_OWNERS))
  .replace("__SLA_HOURS__", JSON.stringify(SLA_HOURS))
  .replace("__DEFECT_LEVELS__", JSON.stringify(DEFECT_LEVELS));

// ---------- 路由 ----------
const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, `http://${req.headers.host}`);
    const p = url.pathname;
    const q = url.searchParams;

    if (req.method === "GET" && p === "/") {
      res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
      return res.end(pageHtml);
    }

    // ---- 读接口 ----
    if (req.method === "GET" && p === "/api/meta") {
      return sendJson(res, 200, { stages: STAGES, activeStages: ACTIVE_STAGES, owners: STAGE_OWNERS, slaHours: SLA_HOURS, defectLevels: DEFECT_LEVELS });
    }
    if (req.method === "GET" && p === "/api/samples") {
      const db = await loadDb();
      return sendJson(res, 200, db.samples.map((s) => sampleView(db, s)));
    }
    const sampleGet = p.match(/^\/api\/samples\/([^/]+)$/);
    if (sampleGet && req.method === "GET") {
      const db = await loadDb();
      const s = findSample(db, sampleGet[1]);
      if (!s) return sendJson(res, 404, { error: "sample_not_found", message: "样本不存在" });
      return sendJson(res, 200, sampleView(db, s));
    }
    if (req.method === "GET" && p === "/api/batches") {
      const db = await loadDb();
      const rows = db.batches.map((b) => batchView(db, b));
      rows.sort((a, b) => (b.createdAt > a.createdAt ? 1 : -1));
      return sendJson(res, 200, rows);
    }
    const batchGet = p.match(/^\/api\/batches\/([^/]+)$/);
    if (batchGet && req.method === "GET") {
      const db = await loadDb();
      const b = findBatch(db, batchGet[1]);
      if (!b) return sendJson(res, 404, { error: "batch_not_found", message: "批次不存在" });
      return sendJson(res, 200, batchView(db, b));
    }
    const sliceGet = p.match(/^\/api\/slices\/([^/]+)$/);
    if (sliceGet && req.method === "GET") {
      const db = await loadDb();
      const s = findSlice(db, sliceGet[1]);
      if (!s) return sendJson(res, 404, { error: "slice_not_found", message: "切片不存在" });
      return sendJson(res, 200, sliceView(db, s));
    }
    if (req.method === "GET" && p === "/api/defects") {
      const db = await loadDb();
      let rows = db.defects;
      if (q.get("status")) rows = rows.filter((d) => d.status === q.get("status"));
      if (q.get("batchId")) rows = rows.filter((d) => d.batchId === q.get("batchId"));
      return sendJson(res, 200, rows);
    }
    if (req.method === "GET" && p === "/api/audits") {
      const db = await loadDb();
      let rows = db.audits;
      const filters = {};
      for (const [k, v] of q) {
        if (["type", "result", "operator", "sliceId", "batchId", "rejectCode"].includes(k) && v) {
          rows = rows.filter((r) => r[k] === v);
          filters[k] = v;
        }
      }
      if (q.get("from")) rows = rows.filter((r) => r.at >= q.get("from"));
      if (q.get("to")) rows = rows.filter((r) => r.at <= q.get("to"));
      const total = rows.length;
      const limit = Math.min(Number(q.get("limit") || 100), 500);
      const offset = Number(q.get("offset") || 0);
      rows = rows.slice().reverse().slice(offset, offset + limit);
      return sendJson(res, 200, { total, limit, offset, filters, items: rows });
    }

    // ---- 写接口（串行 + 幂等）----
    // 录入岩芯样本
    if (req.method === "POST" && p === "/api/samples") {
      return mutation(req, res,(input, db) => {
        const sample = {
          id: nextId(db, "sample", "CORE-"),
          project: requireStr(input, "project", "项目名称"),
          borehole: requireStr(input, "borehole", "钻孔编号"),
          coreBox: requireStr(input, "coreBox", "岩芯箱号"),
          depth: requireStr(input, "depth", "取样深度"),
          rockType: typeof input.rockType === "string" ? input.rockType.trim() : "",
          owner: requireStr(input, "owner", "负责人"),
          createdAt: nowIso(),
        };
        db.samples.push(sample);
        recordAudit(db, {
          type: "sample_create", result: "accepted",
          operator: sample.owner, basis: "样本建档",
          sampleId: sample.id, fromStage: null, toStage: null,
          note: `${sample.project} / ${sample.borehole} / ${sample.depth}`,
        });
        return { __status: 201, sample: sampleView(db, sample) };
      }, "samples");
    }

    // 建立批次
    if (req.method === "POST" && p === "/api/batches") {
      return mutation(req, res,(input, db) => {
        const sampleId = requireStr(input, "sampleId", "样本编号");
        const sample = findSample(db, sampleId);
        if (!sample) throw new HttpError(404, "sample_not_found", "样本不存在");
        const batch = {
          id: nextId(db, "batch", "BATCH-"),
          sampleId,
          name: typeof input.name === "string" && input.name.trim() ? input.name.trim()
            : `${sample.borehole} 批次`,
          plan: typeof input.plan === "string" ? input.plan.trim() : "",
          createdBy: requireStr(input, "createdBy", "建档人"),
          createdAt: nowIso(),
        };
        db.batches.push(batch);
        recordAudit(db, {
          type: "batch_create", result: "accepted",
          operator: batch.createdBy, basis: "批次建档",
          batchId: batch.id, sampleId,
          fromStage: null, toStage: null, note: batch.name,
        });
        return { __status: 201, batch: batchView(db, batch) };
      }, "batches");
    }

    // 按批次一次建多张切片（批量添加）
    const slicesAdd = p.match(/^\/api\/batches\/([^/]+)\/slices$/);
    if (slicesAdd && req.method === "POST") {
      return mutation(req, res,(input, db) => {
        const batch = findBatch(db, slicesAdd[1]);
        if (!batch) throw new HttpError(404, "batch_not_found", "批次不存在");
        const operator = requireStr(input, "operator", "操作人");
        const basis = requireStr(input, "basis", "依据");
        let specs = [];
        if (Array.isArray(input.slices)) {
          specs = input.slices.map((s, i) => ({
            label: typeof s.label === "string" && s.label.trim() ? s.label.trim() : `切片${i + 1}`,
            stainMethod: typeof s.stainMethod === "string" ? s.stainMethod.trim() : "",
            backdateHours: Number.isFinite(+s.backdateHours) ? +s.backdateHours : 0,
          }));
        } else if (Number.isFinite(+input.count) && +input.count > 0) {
          const n = Math.min(Math.floor(+input.count), 100);
          const stain = typeof input.stainMethod === "string" ? input.stainMethod.trim() : "";
          for (let i = 0; i < n; i++) {
            specs.push({ label: `切片${db.slices.filter((x) => x.batchId === batch.id).length + i + 1}`, stainMethod: stain, backdateHours: 0 });
          }
        }
        if (!specs.length) throw new HttpError(400, "empty_batch", "至少添加一张切片");
        const created = [];
        const at = nowIso();
        for (const spec of specs) {
          const id = nextId(db, "slice", "SL-");
          const entered = new Date(new Date(at).getTime() - (spec.backdateHours || 0) * 3600 * 1000).toISOString();
          const slice = {
            id,
            batchId: batch.id,
            batchNo: batch.id,
            sampleId: batch.sampleId,
            label: spec.label,
            stainMethod: spec.stainMethod,
            stage: "取样",
            enteredStageAt: entered,
            dueAt: new Date(new Date(entered).getTime() + SLA_HOURS["取样"] * 3600 * 1000).toISOString(),
            observation: "",
            observedBy: null,
            observedAt: null,
            deliveredBy: null,
            deliveredAt: null,
            createdAt: at,
            updatedAt: at,
          };
          db.slices.push(slice);
          created.push(sliceView(db, slice));
          recordAudit(db, {
            type: "slice_create", result: "accepted",
            operator, basis,
            sliceId: id, batchId: batch.id,
            fromStage: null, toStage: "取样",
            batchNo: batch.id, label: spec.label,
          });
        }
        return { __status: 201, created, batch: batchView(db, batch) };
      }, "slices");
    }

    // 推进工序
    const advance = p.match(/^\/api\/slices\/([^/]+)\/advance$/);
    if (advance && req.method === "POST") {
      return mutation(req, res,(input, db, idemKey) => {
        const slice = findSlice(db, advance[1]);
        if (!slice) throw new HttpError(404, "slice_not_found", "切片不存在");
        try {
          const out = advanceStage(db, slice, input);
          return out;
        } catch (err) {
          if (err instanceof HttpError) {
            auditRejection(db, "advance", input, slice, err.code, err.message, err.extra);
          }
          throw err;
        }
      }, `slice:${advance[1]}`);
    }

    // 登记观察结果
    const observe = p.match(/^\/api\/slices\/([^/]+)\/observation$/);
    if (observe && req.method === "POST") {
      return mutation(req, res,(input, db) => {
        const slice = findSlice(db, observe[1]);
        if (!slice) throw new HttpError(404, "slice_not_found", "切片不存在");
        try {
          return recordObservation(db, slice, input);
        } catch (err) {
          if (err instanceof HttpError) auditRejection(db, "observe", input, slice, err.code, err.message, err.extra);
          throw err;
        }
      }, `slice:${observe[1]}`);
    }

    // 交付
    const deliver = p.match(/^\/api\/slices\/([^/]+)\/deliver$/);
    if (deliver && req.method === "POST") {
      return mutation(req, res,(input, db) => {
        const slice = findSlice(db, deliver[1]);
        if (!slice) throw new HttpError(404, "slice_not_found", "切片不存在");
        try {
          return deliverSlice(db, slice, input);
        } catch (err) {
          if (err instanceof HttpError) auditRejection(db, "deliver", input, slice, err.code, err.message, err.extra);
          throw err;
        }
      }, `slice:${deliver[1]}`);
    }

    // 登记缺陷
    const defectAdd = p.match(/^\/api\/slices\/([^/]+)\/defects$/);
    if (defectAdd && req.method === "POST") {
      return mutation(req, res,(input, db) => {
        const slice = findSlice(db, defectAdd[1]);
        if (!slice) throw new HttpError(404, "slice_not_found", "切片不存在");
        const operator = requireStr(input, "operator", "操作人");
        const basis = requireStr(input, "basis", "依据");
        const description = requireStr(input, "description", "缺陷描述");
        const level = DEFECT_LEVELS.includes(input.level) ? input.level : "一般";
        const at = nowIso();
        const defect = {
          id: nextId(db, "defect", "DEF-"),
          sliceId: slice.id,
          batchId: slice.batchId,
          batchNo: slice.batchNo,
          label: slice.label,
          level,
          description,
          stage: slice.stage,
          status: "未处理",
          reportedBy: operator,
          reportedAt: at,
          resolvedBy: null,
          resolvedAt: null,
          resolution: null,
        };
        db.defects.push(defect);
        recordAudit(db, {
          type: "defect_report", result: "accepted",
          operator, basis,
          sliceId: slice.id, batchId: slice.batchId,
          fromStage: slice.stage, toStage: slice.stage,
          note: `[${level}] ${description}`,
          batchNo: slice.batchNo, label: slice.label,
        });
        return { __status: 201, defect };
      }, `slice:${defectAdd[1]}`);
    }

    // 处理缺陷
    const defectResolve = p.match(/^\/api\/defects\/([^/]+)\/resolve$/);
    if (defectResolve && req.method === "POST") {
      return mutation(req, res,(input, db) => {
        const defect = db.defects.find((d) => d.id === defectResolve[1]);
        if (!defect) throw new HttpError(404, "defect_not_found", "缺陷不存在");
        const operator = requireStr(input, "operator", "操作人");
        const resolution = requireStr(input, "resolution", "处理说明");
        if (defect.status !== "未处理") throw new HttpError(409, "already_resolved", "缺陷已处理");
        const at = nowIso();
        defect.status = "已处理";
        defect.resolvedBy = operator;
        defect.resolvedAt = at;
        defect.resolution = resolution;
        const slice = findSlice(db, defect.sliceId);
        recordAudit(db, {
          type: "defect_resolve", result: "accepted",
          operator, basis: typeof input.basis === "string" && input.basis.trim() ? input.basis.trim() : "缺陷处理",
          sliceId: defect.sliceId, batchId: defect.batchId,
          fromStage: slice ? slice.stage : null, toStage: slice ? slice.stage : null,
          note: resolution,
          batchNo: defect.batchNo, label: defect.label,
        });
        return { defect };
      }, `defect:${defectResolve[1]}`);
    }

    return sendJson(res, 404, { error: "not_found", message: "接口不存在" });
  } catch (error) {
    if (error instanceof HttpError) {
      return sendJson(res, error.status, { error: error.code, message: error.message, ...error.extra });
    }
    console.error(error);
    return sendJson(res, 500, { error: "internal_error", message: error.message });
  }
});

server.listen(port, () => {
  console.log(`岩芯切片制备批次台已启动: http://localhost:${port}`);
  console.log(`数据文件: ${dbPath}`);
});
