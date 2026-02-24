// worker-local/worker.mjs — GOODPDF Worker (FAST v4.0)
// Core goals (locked):
//  - DEFAULT(Auto/System-Fit): try to shrink PDF to <= 5×9MB (≈45MB) FAST,
//      using Selective Image Recompression (Python) first.
//      If still too large, fallback to ONE Ghostscript pass (85 DPI / JPEG Q45).
//      Then split with hard max 5 parts (best effort near 9MB each).
//  - MANUAL: user target MB (default 9) + quality presets:
//      High   = 110 DPI / Q50
//      Medium = 100 DPI / Q45
//      Original = no compression
//  - Speed-first: avoid multi-pass loops. qpdf used for splitting.
//  - Oversize single-page (>target) will be emitted as OVERSIZE_pageNN.pdf (no fail).

import "dotenv/config";
import fs from "fs";
import path from "path";
import os from "os";
import { spawn } from "child_process";
import crypto from "crypto";
import { createClient } from "@supabase/supabase-js";
import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  DeleteObjectCommand,
} from "@aws-sdk/client-s3";
import { estimatePartsForTarget } from "./tools/scan_engine.mjs";

// ----------------------
// 7z resolve
// ----------------------
function resolve7zExe() {
  const envPath = (process.env.SEVEN_Z_EXE || "").trim();
  const candidates = [
    envPath || null,
    "C:\\Program Files\\7-Zip\\7z.exe",
    "C:\\Program Files (x86)\\7-Zip\\7z.exe",
    "7z",
  ].filter(Boolean);

  for (const c of candidates) {
    if (c === "7z") return c;
    try {
      if (c.toLowerCase().endsWith(".exe") && fs.existsSync(c)) return c;
    } catch {}
  }
  const err = new Error("7-Zip not found. Install 7-Zip or set SEVEN_Z_EXE.");
  err.code = "MISSING_7Z";
  throw err;
}
const SEVEN_Z_EXE = resolve7zExe();

// ----------------------
// ENV
// ----------------------
const WORKER_ID =
  process.env.WORKER_ID || `worker_${crypto.randomBytes(3).toString("hex")}`;

const SUPABASE_URL =
  process.env.SUPABASE_ADMIN_URL ||
  process.env.SUPABASE_URL ||
  process.env.NEXT_PUBLIC_SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const R2_ENDPOINT = process.env.R2_ENDPOINT;
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID;
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY;

const R2_BUCKET_IN =
  process.env.R2_BUCKET_IN || process.env.R2_BUCKET || "goodpdf-in";
const R2_BUCKET_OUT =
  process.env.R2_BUCKET_OUT || process.env.R2_BUCKET || "goodpdf-out";

const POLL_MS = Number(process.env.POLL_MS || 2000);
const CPU_CORES = Math.max(1, Number(os.cpus()?.length || 1));
const CONCURRENCY = Math.max(
  1,
  Math.min(
    Number(process.env.CONCURRENCY || 1),
    Math.max(1, Number(process.env.MAX_WORKER_CONCURRENCY || CPU_CORES - 1)),
  ),
);
const SPLIT_PAR = Math.max(
  1,
  Math.min(
    Number(process.env.SPLIT_PAR || 4),
    Math.max(1, Number(process.env.MAX_SPLIT_PAR || CPU_CORES)),
  ),
);
const QUEUE_FETCH_LIMIT = Math.max(
  1,
  Math.min(CONCURRENCY, Number(process.env.QUEUE_FETCH_LIMIT || CONCURRENCY)),
);
const DEFAULT_TTL_MINUTES = Math.max(
  5,
  Number(process.env.DEFAULT_TTL_MINUTES || 10),
);

const DO_CLEANUP =
  String(process.env.DO_CLEANUP || "true").toLowerCase() !== "false";
const CLEANUP_EVERY_MS = Math.max(
  10_000,
  Number(process.env.CLEANUP_EVERY_MS || 30_000),
);

// Binaries
const GS_EXE = process.env.GS_EXE || "gs";
const QPDF_EXE = process.env.QPDF_EXE || "qpdf";

// Timeouts (bounded by expires_at too)
const TIMEOUT_QPDF_MS = Math.max(10_000, Number(process.env.TIMEOUT_QPDF_MS || 120_000));
const TIMEOUT_GS_MS = Math.max(10_000, Number(process.env.TIMEOUT_GS_MS || 75_000));
const TIMEOUT_7Z_MS = Math.max(
  10_000,
  Number(process.env.TIMEOUT_7Z_MS || 120_000),
);

// UX cap (best-effort)
const UX_CAP_MS = Math.max(60_000, Number(process.env.UX_CAP_MS || 180_000)); // default 3min
const UX_SOFTSTOP_MS = Math.max(
  50_000,
  Math.min(UX_CAP_MS - 10_000, Number(process.env.UX_SOFTSTOP_MS || 150_000)),
);

// DEFAULT system-fit policy
const SYSTEM_MAX_PARTS = Number(process.env.SYSTEM_MAX_PARTS || 5);
const SYSTEM_PART_MB = Number(process.env.SYSTEM_PART_MB || 9);
const SYSTEM_PART_BYTES = Math.floor(SYSTEM_PART_MB * 1024 * 1024);
const SYSTEM_TOTAL_CAP_BYTES = Math.floor(
  SYSTEM_PART_BYTES * SYSTEM_MAX_PARTS - 512 * 1024, // headroom
);
const SYSTEM_TOTAL_FLOOR_MB = Math.max(
  10,
  Math.min(SYSTEM_PART_MB * SYSTEM_MAX_PARTS, Number(process.env.SYSTEM_TOTAL_FLOOR_MB || 40)),
);
const SYSTEM_TOTAL_FLOOR_BYTES = Math.floor(SYSTEM_TOTAL_FLOOR_MB * 1024 * 1024);
const SYSTEM_TURBO_TRIGGER_MB = Math.max(
  20,
  Number(process.env.SYSTEM_TURBO_TRIGGER_MB || 80),
);
const SYSTEM_TURBO_PAGE_TRIGGER = Math.max(
  40,
  Number(process.env.SYSTEM_TURBO_PAGE_TRIGGER || 120),
);
const SYSTEM_FORCE_5_FROM_MB = Math.max(
  30,
  Number(process.env.SYSTEM_FORCE_5_FROM_MB || 100),
);
const SYSTEM_MIN_PARTS = Math.max(
  1,
  Math.min(SYSTEM_MAX_PARTS, Number(process.env.SYSTEM_MIN_PARTS || 2)),
);

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}
if (!R2_ENDPOINT || !R2_ACCESS_KEY_ID || !R2_SECRET_ACCESS_KEY) {
  console.error(
    "Missing R2 env vars (R2_ENDPOINT/R2_ACCESS_KEY_ID/R2_SECRET_ACCESS_KEY)",
  );
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
const r2 = new S3Client({
  region: "auto",
  endpoint: R2_ENDPOINT,
  credentials: {
    accessKeyId: R2_ACCESS_KEY_ID,
    secretAccessKey: R2_SECRET_ACCESS_KEY,
  },
});

// ----------------------
// Utils
// ----------------------
function nowIso() {
  return new Date().toISOString();
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function safeExists(p) {
  try {
    return fs.existsSync(p);
  } catch {
    return false;
  }
}
function safeUnlink(p) {
  try {
    if (safeExists(p)) fs.unlinkSync(p);
  } catch {}
}
function safeRm(p) {
  try {
    fs.rmSync(p, { recursive: true, force: true });
  } catch {}
}
function safeStatSize(p) {
  try {
    if (!safeExists(p)) return null;
    const st = fs.statSync(p);
    return Number.isFinite(st?.size) ? st.size : null;
  } catch {
    return null;
  }
}
function bytesToMb(n) {
  return n / (1024 * 1024);
}
function rand6() {
  return crypto.randomBytes(3).toString("hex");
}
function remainingMsFromExpiresAt(expiresAtIso) {
  const t = Date.parse(expiresAtIso || "");
  if (!Number.isFinite(t)) return null;
  return t - Date.now();
}
function boundedTimeout(
  defaultMs,
  expiresAtIso,
  floorMs = 15_000,
  reserveMs = 60_000,
) {
  const remaining = remainingMsFromExpiresAt(expiresAtIso);
  if (remaining == null) return defaultMs;
  const maxAllowed = remaining - reserveMs;
  return Math.max(floorMs, Math.min(defaultMs, maxAllowed));
}

// Simple async pool
async function asyncPool(limit, items, iteratorFn) {
  const ret = new Array(items.length);
  let i = 0;
  const workers = new Array(Math.min(limit, items.length))
    .fill(0)
    .map(async () => {
      while (i < items.length) {
        const idx = i++;
        ret[idx] = await iteratorFn(items[idx], idx);
      }
    });
  await Promise.all(workers);
  return ret;
}

function streamToFile(stream, outPath) {
  return new Promise((resolve, reject) => {
    const w = fs.createWriteStream(outPath);
    stream.pipe(w);
    stream.on("error", reject);
    w.on("error", reject);
    w.on("finish", resolve);
  });
}

function runCmd(cmd, args, timeoutMs, spawnOpts = {}, okCodes = [0]) {
  return new Promise((resolve, reject) => {
    const p = spawn(cmd, args, {
      stdio: ["ignore", "pipe", "pipe"],
      ...spawnOpts,
    });

    let killed = false;
    const to = setTimeout(() => {
      killed = true;
      try {
        p.kill("SIGKILL");
      } catch {}
    }, timeoutMs);

    let out = "";
    let err = "";
    p.stdout.on("data", (d) => (out += d.toString()));
    p.stderr.on("data", (d) => (err += d.toString()));

    p.on("close", (code) => {
      clearTimeout(to);
      if (killed) {
        const e = new Error(`${cmd} timeout after ${timeoutMs}ms`);
        e.code = "CMD_TIMEOUT";
        return reject(e);
      }
      if (!okCodes.includes(code)) {
        const e = new Error(`${cmd} failed code=${code}\n${err || out}`);
        e.code = "CMD_FAILED";
        e.exitCode = code;
        return reject(e);
      }
      resolve({ out, err });
    });
  });
}

// ----------------------
// Python helper (Selective recompression)
// ----------------------
// ----------------------
// Supabase helpers
// ----------------------
async function updateJob(jobId, patch) {
  const { error } = await supabase.from("jobs").update(patch).eq("id", jobId);
  if (error) throw error;
}

async function fetchQueue(limit = 1) {
  // Robust queue fetch with debug + timeout (prevents silent hangs)
  const t0 = Date.now();

  // Process only explicit QUEUED jobs.
  // This prevents early processing before /api/jobs/start sets split_mb/preset.
  const query = supabase
    .from("jobs")
    .select(
      "id,status,stage,created_at,input_path,claimed_by,split_mb,ttl_minutes,expires_at,delete_at",
    )
    .eq("status", "QUEUED")
    .is("claimed_by", null)
    .order("created_at", { ascending: true })
    .limit(limit);

  // 12s hard timeout to surface network/SDK hangs
  const timeoutMs = 12000;
  const timeout = sleep(timeoutMs).then(() => ({
    data: null,
    error: new Error("fetchQueue timeout after " + timeoutMs + "ms"),
  }));

  let res;
  try {
    res = await Promise.race([query, timeout]);
  } catch (e) {
    console.log("[fetchQueue] EXCEPTION:", e?.message || e);
    return [];
  }

  const ms = Date.now() - t0;
  if (res?.error) {
    console.log(
      "[fetchQueue] ERROR:",
      res.error?.message || res.error,
      "(ms =",
      ms + ")",
    );
    return [];
  }

  const rows = res?.data || [];
  if (rows.length > 0) {
    console.log("[fetchQueue] exit got =", rows.length, "(ms =", ms + ")");
  }
  return rows;
}

async function claimJob(jobId) {
  const patch = {
    status: "PROCESSING",
    stage: "QUEUE",
    progress: 1,
    split_progress: 0,
    claimed_by: WORKER_ID,
    claimed_at: nowIso(),
    error_text: null,
    error_code: null,
    updated_at: nowIso(),
  };

  const { data, error } = await supabase
    .from("jobs")
    .update(patch)
    .eq("id", jobId)
    .eq("status", "QUEUED")
    .is("claimed_by", null)
    .select("id")
    .maybeSingle();

  if (error) return false;
  return !!data;
}

// ----------------------
// R2 helpers
// ----------------------
function normalizeInputKey(job) {
  const raw = job.input_path ? String(job.input_path) : `${job.id}/input.pdf`;
  const normalized = raw.replace(/^\/+/, "");
  if (
    !normalized ||
    normalized.includes("..") ||
    normalized.includes("\\") ||
    !/^[A-Za-z0-9._/-]+$/.test(normalized)
  ) {
    const e = new Error("INPUT_KEY_INVALID");
    e.code = "INPUT_KEY_INVALID";
    throw e;
  }
  return normalized;
}
function outputZipKeyFor(job) {
  return `${job.id}/goodpdf.zip`;
}

async function downloadFromR2(key, outPath) {
  const res = await r2.send(
    new GetObjectCommand({ Bucket: R2_BUCKET_IN, Key: key }),
  );
  if (!res?.Body) throw new Error("R2 GetObject missing Body");
  await streamToFile(res.Body, outPath);
}

async function uploadToR2(key, filePath, contentType) {
  const body = fs.createReadStream(filePath);
  await r2.send(
    new PutObjectCommand({
      Bucket: R2_BUCKET_OUT,
      Key: key,
      Body: body,
      ContentType: contentType,
    }),
  );
}

// ----------------------
// Cleanup expired (privacy-first)
// ----------------------
async function cleanupExpiredOutputs() {
  if (!DO_CLEANUP) return 0;

  const now = nowIso();
  const { data: rowsDeleteAt, error: errDeleteAt } = await supabase
    .from("jobs")
    .select(
      "id,input_path,output_zip_path,zip_path,expires_at,delete_at,cleaned_at,status",
    )
    .is("cleaned_at", null)
    .not("delete_at", "is", null)
    .lt("delete_at", now)
    .limit(50);

  if (errDeleteAt) throw errDeleteAt;

  const { data: rowsExpiresAt, error: errExpiresAt } = await supabase
    .from("jobs")
    .select(
      "id,input_path,output_zip_path,zip_path,expires_at,delete_at,cleaned_at,status",
    )
    .is("cleaned_at", null)
    .is("delete_at", null)
    .not("expires_at", "is", null)
    .lt("expires_at", now)
    .limit(50);

  if (errExpiresAt) throw errExpiresAt;

  const byId = new Map();
  for (const r of rowsDeleteAt || []) byId.set(r.id, r);
  for (const r of rowsExpiresAt || []) byId.set(r.id, r);
  const rows = Array.from(byId.values());
  let cleaned = 0;

  for (const r of rows) {
    const inputKey =
      (r.input_path && String(r.input_path).replace(/^\/+/, "")) ||
      `${r.id}/input.pdf`;

    const outKey =
      (r.output_zip_path && String(r.output_zip_path).replace(/^\/+/, "")) ||
      (r.zip_path && String(r.zip_path).replace(/^\/+/, "")) ||
      null;

    try {
      await r2.send(
        new DeleteObjectCommand({ Bucket: R2_BUCKET_IN, Key: inputKey }),
      );
    } catch {}
    if (outKey) {
      try {
        await r2.send(
          new DeleteObjectCommand({ Bucket: R2_BUCKET_OUT, Key: outKey }),
        );
      } catch {}
    }

    try {
      await supabase
        .from("jobs")
        .update({
          status: "CLEANED",
          stage: "CLEANUP",
          cleaned_at: nowIso(),
          updated_at: nowIso(),
        })
        .eq("id", r.id);
      cleaned++;
    } catch {}
  }

  return cleaned;
}

// ----------------------
// qpdf helpers
// ----------------------
async function qpdfPages(inPdf, expiresAtIso) {
  const tQ = Math.min(
    boundedTimeout(TIMEOUT_QPDF_MS, expiresAtIso, 10_000, 60_000),
    25_000,
  );
  const { out } = await runCmd(QPDF_EXE, ["--show-npages", inPdf], tQ);
  const pages = Number(String(out).trim());
  if (!Number.isFinite(pages) || pages <= 0) {
    const e = new Error("NPAGES_FAILED");
    e.code = "NPAGES_FAILED";
    throw e;
  }
  return pages;
}

// very fast, stable recompress/normalize (good before GS)
async function qpdfFastRecompress({ inPdf, outPdf, expiresAtIso }) {
  const tQ = boundedTimeout(TIMEOUT_QPDF_MS, expiresAtIso, 15_000, 60_000);
  const args = [
    "--stream-data=compress",
    "--object-streams=generate",
    "--compress-streams=y",
    "--normalize-content=y",
    "--remove-unreferenced-resources=yes",
    "--",
    inPdf,
    outPdf,
  ];
  await runCmd(QPDF_EXE, args, tQ);
  const outBytes = safeStatSize(outPdf) ?? 0;
  return outBytes > 0;
}

// ----------------------
// Split helpers (fast)
// ----------------------
function buildEqualRanges(pages, parts) {
  const p = Math.max(1, pages);
  const k = Math.max(1, parts);
  const ranges = [];
  let start = 1;

  for (let i = 0; i < k; i++) {
    const left = k - i;
    const remaining = p - start + 1;
    const size = Math.ceil(remaining / left);
    const end = Math.min(p, start + size - 1);
    ranges.push({ start, end });
    start = end + 1;
    if (start > p) break;
  }
  return ranges;
}

async function estimatePageBytes({
  inPdf,
  pages,
  probeDir,
  expiresAtIso,
}) {
  safeRm(probeDir);
  fs.mkdirSync(probeDir, { recursive: true });
  const pageNums = [];
  for (let p = 1; p <= pages; p++) pageNums.push(p);

  const rows = await asyncPool(SPLIT_PAR, pageNums, async (p) => {
    const onePath = path.join(probeDir, `p${String(p).padStart(5, "0")}.pdf`);
    const b = await qpdfExtractPage({
      inPdf,
      pageNum: p,
      outPdf: onePath,
      expiresAtIso,
    });
    return { p, bytes: b };
  });

  safeRm(probeDir);
  rows.sort((a, b) => a.p - b.p);
  return rows.map((r) => Math.max(0, Number(r.bytes || 0)));
}

function buildRangesNearTarget({
  pageBytes,
  targetBytes,
  maxParts,
}) {
  const pages = pageBytes.length;
  if (pages <= 0) return [{ start: 1, end: 1 }];

  const totalBytes = pageBytes.reduce((a, b) => a + b, 0);
  const parts = Math.max(
    1,
    Math.min(
      maxParts,
      pages,
      Math.ceil(totalBytes / Math.max(256 * 1024, Math.floor(targetBytes * 0.96))),
    ),
  );

  const suffix = new Array(pages + 2).fill(0);
  for (let i = pages; i >= 1; i--) suffix[i] = suffix[i + 1] + pageBytes[i - 1];

  const ranges = [];
  let start = 1;
  let running = 0;
  let remainingParts = parts;

  for (let p = 1; p <= pages; p++) {
    const remainingPages = pages - p + 1;
    if (remainingPages === remainingParts) {
      if (start <= p - 1) ranges.push({ start, end: p - 1 });
      for (let q = p; q <= pages; q++) ranges.push({ start: q, end: q });
      return ranges;
    }

    const remainingBytes = suffix[p];
    const dynamicTarget = Math.min(
      Math.floor(targetBytes * 0.98),
      Math.max(256 * 1024, Math.floor(remainingBytes / Math.max(1, remainingParts))),
    );

    const b = pageBytes[p - 1];
    if (running > 0 && running + b > dynamicTarget) {
      ranges.push({ start, end: p - 1 });
      remainingParts--;
      start = p;
      running = 0;
    }
    running += b;
  }

  if (start <= pages) ranges.push({ start, end: pages });
  return ranges;
}

async function splitSinglePass({ inPdf, partsDir, ranges, expiresAtIso }) {
  fs.mkdirSync(partsDir, { recursive: true });

  const tEach = boundedTimeout(TIMEOUT_QPDF_MS, expiresAtIso, 15_000, 60_000);
  const tmpName = (i, s, e) =>
    path.join(
      partsDir,
      `.__range_${String(i + 1).padStart(3, "0")}_${s}_${e}_${rand6()}.pdf`,
    );

  const results = await asyncPool(SPLIT_PAR, ranges, async (r, i) => {
    const outPath = tmpName(i, r.start, r.end);
    await runCmd(
      QPDF_EXE,
      ["--empty", "--pages", inPdf, `${r.start}-${r.end}`, "--", outPath],
      tEach,
    );
    const b = safeStatSize(outPath) ?? 0;
    return { tmpPath: outPath, start: r.start, end: r.end, bytes: b };
  });

  const totalParts = results.length;
  const partFiles = [];
  const partMeta = [];

  for (let idx = 0; idx < results.length; idx++) {
    const p = results[idx];
    const outName = `goodPDF-${totalParts}(${idx + 1}).pdf`;
    const outPath = path.join(partsDir, outName);

    try {
      fs.renameSync(p.tmpPath, outPath);
    } catch {
      fs.copyFileSync(p.tmpPath, outPath);
      safeUnlink(p.tmpPath);
    }

    partFiles.push(outPath);
    partMeta.push({
      name: path.basename(outPath),
      bytes: p.bytes,
      sizeMb: Math.round(bytesToMb(p.bytes) * 10) / 10,
      startPageIndex: p.start,
      endPageIndex: p.end,
    });
  }

  // cleanup tmp
  try {
    for (const f of fs.readdirSync(partsDir)) {
      if (f.startsWith(".__range_")) safeUnlink(path.join(partsDir, f));
    }
  } catch {}

  return { partFiles, partMeta };
}

function maxPartBytes(partMeta) {
  let m = 0;
  for (const p of partMeta || []) m = Math.max(m, Number(p?.bytes || 0));
  return m;
}

async function recompressPdfInPlace({
  pdfPath,
  expiresAtIso,
  dpi,
  jpegQ,
  pdfSettings = "/screen",
}) {
  const tmpOut = `${pdfPath}.re_${rand6()}.pdf`;
  const ok = await gsCompressOnce({
    inPdf: pdfPath,
    outPdf: tmpOut,
    mode: "PRESET",
    dpi,
    jpegQ,
    pdfSettings,
    expiresAtIso,
  });
  if (!ok) {
    safeUnlink(tmpOut);
    return safeStatSize(pdfPath) ?? 0;
  }

  const newBytes = safeStatSize(tmpOut) ?? 0;
  if (newBytes > 0) {
    try {
      fs.renameSync(tmpOut, pdfPath);
    } catch {
      fs.copyFileSync(tmpOut, pdfPath);
      safeUnlink(tmpOut);
    }
  } else {
    safeUnlink(tmpOut);
  }
  return safeStatSize(pdfPath) ?? 0;
}

async function shrinkOversizePartsAggressive({
  res,
  targetBytes,
  expiresAtIso,
  maxPasses = 1,
  onProgress,
}) {
  const fileByName = new Map(
    (res.partFiles || []).map((p) => [path.basename(p), p]),
  );
  const passes = [
    { dpi: 42, jpegQ: 14 },
    { dpi: 36, jpegQ: 10 },
    { dpi: 30, jpegQ: 8 },
  ];

  const passCount = Math.max(1, Math.min(maxPasses, passes.length));
  for (let i = 0; i < passCount; i++) {
    const pass = passes[i];
    const overs = (res.partMeta || []).filter(
      (m) => Number(m?.bytes || 0) > targetBytes,
    );
    if (!overs.length) break;

    let done = 0;
    const total = overs.length;
    await asyncPool(
      Math.max(1, Math.min(SPLIT_PAR, overs.length)),
      overs,
      async (meta) => {
        const filePath = fileByName.get(meta.name);
        if (!filePath) return;
        const nb = await recompressPdfInPlace({
          pdfPath: filePath,
          expiresAtIso,
          dpi: pass.dpi,
          jpegQ: pass.jpegQ,
          pdfSettings: "/screen",
        });
        meta.bytes = nb;
        meta.sizeMb = Math.round(bytesToMb(nb) * 10) / 10;
        done += 1;
        if (typeof onProgress === "function") {
          await Promise.resolve(
            onProgress({
              pass: i + 1,
              passCount,
              done,
              total,
              maxPartBytes: maxPartBytes(res.partMeta),
            }),
          ).catch(() => {});
        }
      },
    );

    if (maxPartBytes(res.partMeta) <= targetBytes) break;
  }
  return res;
}

// ----------------------
// qpdf page extract / concat helpers (oversize-safe split fallback)
// ----------------------
async function qpdfExtractPage({ inPdf, pageNum, outPdf, expiresAtIso }) {
  const tQ = boundedTimeout(TIMEOUT_QPDF_MS, expiresAtIso, 15_000, 60_000);
  await runCmd(
    QPDF_EXE,
    ["--empty", "--pages", inPdf, `${pageNum}-${pageNum}`, "--", outPdf],
    tQ,
  );
  const b = safeStatSize(outPdf) ?? 0;
  return b;
}

async function qpdfConcatSinglePages({ pagePdfs, outPdf, expiresAtIso }) {
  // Each file in pagePdfs is a 1-page PDF. We stitch them into one multi-page PDF.
  const tQ = boundedTimeout(TIMEOUT_QPDF_MS, expiresAtIso, 20_000, 60_000);
  const args = ["--empty", "--pages"];
  for (const p of pagePdfs) args.push(p, "1");
  args.push("--", outPdf);
  await runCmd(QPDF_EXE, args, tQ);
  const b = safeStatSize(outPdf) ?? 0;
  return b;
}

async function splitOversizeSafe({
  inPdf,
  partsDir,
  targetBytes,
  expiresAtIso,
}) {
  // Fallback mode when normal range-split can't enforce <=targetBytes due to huge single pages.
  // Strategy:
  //  1) Extract every page as its own PDF.
  //  2) Any page > targetBytes becomes OVERSIZE_pageNN.pdf (kept as-is).
  //  3) Remaining pages are greedily packed into parts by concatenating single-page PDFs.
  fs.mkdirSync(partsDir, { recursive: true });

  const pages = await qpdfPages(inPdf, expiresAtIso);

  const oneDir = path.join(partsDir, ".__single_pages");
  safeRm(oneDir);
  fs.mkdirSync(oneDir, { recursive: true });

  const pageNums = [];
  for (let p = 1; p <= pages; p++) pageNums.push(p);
  const single = await asyncPool(SPLIT_PAR, pageNums, async (p) => {
    const onePath = path.join(oneDir, `p${String(p).padStart(5, "0")}.pdf`);
    const b = await qpdfExtractPage({
      inPdf,
      pageNum: p,
      outPdf: onePath,
      expiresAtIso,
    });
    return { p, path: onePath, bytes: b };
  });
  single.sort((a, b) => a.p - b.p);

  const oversize = [];
  const normal = [];
  for (const it of single) {
    if (it.bytes > targetBytes) oversize.push(it);
    else normal.push(it);
  }

  const partFiles = [];
  const partMeta = [];

  // 3-A) Write oversize pages as standalone parts
  for (const it of oversize) {
    const outName = `OVERSIZE_page${it.p}.pdf`;
    const outPath = path.join(partsDir, outName);
    try {
      fs.renameSync(it.path, outPath);
    } catch {
      fs.copyFileSync(it.path, outPath);
    }
    const b = safeStatSize(outPath) ?? it.bytes ?? 0;
    partFiles.push(outPath);
    partMeta.push({
      name: path.basename(outPath),
      bytes: b,
      sizeMb: Math.round(bytesToMb(b) * 10) / 10,
      startPageIndex: it.p,
      endPageIndex: it.p,
      note: "OVERSIZE_SINGLE_PAGE",
    });
  }

  // 3-B) Greedy pack remaining normal pages
  let pack = [];
  let packBytes = 0;
  let packStart = null;
  let packEnd = null;

  const flushPack = async () => {
    if (!pack.length) return;
    const idx = partFiles.length + 1;
    const outName = `goodPDF-OVERSIZEPACK(${idx}).pdf`;
    const outPath = path.join(partsDir, outName);
    await qpdfConcatSinglePages({
      pagePdfs: pack.map((x) => x.path),
      outPdf: outPath,
      expiresAtIso,
    });
    const b = safeStatSize(outPath) ?? 0;
    partFiles.push(outPath);
    partMeta.push({
      name: path.basename(outPath),
      bytes: b,
      sizeMb: Math.round(bytesToMb(b) * 10) / 10,
      startPageIndex: packStart,
      endPageIndex: packEnd,
    });
    pack = [];
    packBytes = 0;
    packStart = null;
    packEnd = null;
  };

  for (const it of normal) {
    if (!pack.length) {
      packStart = it.p;
      packEnd = it.p;
      pack = [it];
      packBytes = it.bytes;
      continue;
    }

    // if adding exceeds, flush current and start new
    if (packBytes + it.bytes > Math.floor(targetBytes * 0.98)) {
      await flushPack();
      packStart = it.p;
      packEnd = it.p;
      pack = [it];
      packBytes = it.bytes;
    } else {
      pack.push(it);
      packBytes += it.bytes;
      packEnd = it.p;
    }
  }
  await flushPack();

  // cleanup singles dir
  safeRm(oneDir);

  // Rename goodPDF-OVERSIZEPACK(...) into standard naming if desired
  return { partFiles, partMeta };
}
// ----------------------
// Ghostscript profiles (FAST, 1–2 passes max)
// ----------------------

function gsArgsPreset({ inPdf, outPdf, dpi, jpegQ, pdfSettings }) {
  // Speed-first + strong shrink preset. (Single pass)
  return gsArgsStrongFast({
    inPdf,
    outPdf,
    dpi,
    jpegQ,
    pdfSettings,
  });
}

function gsArgsStrongFast({
  inPdf,
  outPdf,
  dpi = 110,
  jpegQ = 58,
  pdfSettings = "/ebook",
}) {
  return [
    "-q",
    "-dSAFER",
    "-dBATCH",
    "-dNOPAUSE",
    "-sDEVICE=pdfwrite",
    "-dCompatibilityLevel=1.4",
    "-dDetectDuplicateImages=false",
    "-dAutoRotatePages=/None",
    "-dCompressFonts=true",
    "-dSubsetFonts=true",
    "-dEmbedAllFonts=true",
    "-dPassThroughICCProfiles=false",

    "-dDownsampleColorImages=true",
    "-dDownsampleGrayImages=true",
    "-dDownsampleMonoImages=true",
    "-dColorImageDownsampleType=/Subsample",
    "-dGrayImageDownsampleType=/Subsample",
    "-dMonoImageDownsampleType=/Subsample",
    `-dColorImageResolution=${dpi}`,
    `-dGrayImageResolution=${dpi}`,
    `-dMonoImageResolution=${Math.max(180, dpi * 2)}`,

    "-dAutoFilterColorImages=false",
    "-dAutoFilterGrayImages=false",
    "-dColorImageFilter=/DCTEncode",
    "-dGrayImageFilter=/DCTEncode",
    `-dJPEGQ=${jpegQ}`,

    `-dPDFSETTINGS=${pdfSettings}`,
    `-sOutputFile=${outPdf}`,
    inPdf,
  ];
}

function gsArgsNuclear({ inPdf, outPdf }) {
  // Must-fit mode for hard cap enforcement.
  return gsArgsStrongFast({
    inPdf,
    outPdf,
    dpi: 42,
    jpegQ: 12,
    pdfSettings: "/screen",
  });
}

async function gsCompressOnce({
  inPdf,
  outPdf,
  mode,
  expiresAtIso,
  dpi,
  jpegQ,
  pdfSettings,
}) {
  const tGS = boundedTimeout(TIMEOUT_GS_MS, expiresAtIso, 20_000, 90_000);

  const args =
    mode === "NUCLEAR"
      ? gsArgsNuclear({ inPdf, outPdf })
      : mode === "PRESET"
        ? gsArgsPreset({
            inPdf,
            outPdf,
            dpi: Number(dpi) || 110,
            jpegQ: Number(jpegQ) || 58,
            pdfSettings: pdfSettings || "/ebook",
          })
        : gsArgsStrongFast({ inPdf, outPdf });

  try {
    await runCmd(GS_EXE, args, tGS);
    const b = safeStatSize(outPdf) ?? 0;
    return b > 0;
  } catch {
    return false;
  }
}

// ----------------------
// ZIP
// ----------------------
async function zipParts(partsDir, outZipPath, expiresAtIso) {
  const pdfs = fs
    .readdirSync(partsDir)
    .filter((f) => f.toLowerCase().endsWith(".pdf"))
    .sort();
  if (!pdfs.length) {
    const e = new Error("NO_PART_PDFS_TO_ZIP");
    e.code = "NO_PART_PDFS_TO_ZIP";
    throw e;
  }

  const timeoutZip = boundedTimeout(
    TIMEOUT_7Z_MS,
    expiresAtIso,
    30_000,
    60_000,
  );
  // -mx=0 fastest zip
  await runCmd(
    SEVEN_Z_EXE,
    ["a", "-tzip", "-mx=0", "-mmt=on", outZipPath, ...pdfs],
    timeoutZip,
    { cwd: partsDir },
  );
}

// ----------------------
// Mode decide
// ----------------------

function choosePartCountBySize({
  bytes,
  pages,
  targetBytes,
  minParts = 2,
  maxParts = 5,
}) {
  const b = Math.max(0, Number(bytes || 0));
  const p = Math.max(1, Number(pages || 1));
  const t = Math.max(256 * 1024, Number(targetBytes || SYSTEM_PART_BYTES));
  const maxP = Math.max(1, Math.min(Number(maxParts || 5), p));
  const minP = Math.max(1, Math.min(Number(minParts || 1), maxP));

  // Keep each part close to 9MB, but never drop below min part policy.
  const ideal = Math.ceil(b / Math.floor(t * 0.98));
  return Math.max(minP, Math.min(maxP, Math.max(1, ideal)));
}

function buildHardFitPassPlan({ bytes, pages, forceFast }) {
  const b = Math.max(0, Number(bytes || 0));
  const p = Math.max(1, Number(pages || 1));
  const bpp = b / p;
  const hugeImageLikely =
    bytesToMb(b) >= 80 || p >= 120 || bpp >= 700 * 1024 || !!forceFast;

  if (hugeImageLikely) {
    // 3-minute SLA priority lane (few strong passes).
    return [
      { dpi: 120, jpegQ: 54, pdfSettings: "/ebook" },
      { dpi: 102, jpegQ: 46, pdfSettings: "/ebook" },
      { dpi: 88, jpegQ: 40, pdfSettings: "/screen" },
      { dpi: 76, jpegQ: 32, pdfSettings: "/screen" },
      { dpi: 66, jpegQ: 26, pdfSettings: "/screen" },
    ];
  }

  return [
    { dpi: 132, jpegQ: 58, pdfSettings: "/ebook" },
    { dpi: 116, jpegQ: 52, pdfSettings: "/ebook" },
    { dpi: 100, jpegQ: 46, pdfSettings: "/ebook" },
    { dpi: 86, jpegQ: 38, pdfSettings: "/screen" },
    { dpi: 72, jpegQ: 30, pdfSettings: "/screen" },
  ];
}

// ----------------------
// Processors
// ----------------------
async function processDefaultFast({
  jobId,
  inPdf,
  wd,
  expiresAtIso,
  startedAtMs,
}) {
  const elapsed = () => Date.now() - startedAtMs;
  const softStop = () => elapsed() >= UX_SOFTSTOP_MS;

  const partsDir = path.join(wd, "parts");
  fs.mkdirSync(partsDir, { recursive: true });
  const preInBytes = safeStatSize(inPdf) ?? 0;
  const forceFast = bytesToMb(preInBytes) >= SYSTEM_FORCE_5_FROM_MB;

  await updateJob(jobId, {
    stage: "PREFLIGHT",
    progress: 8,
    split_progress: 2,
    updated_at: nowIso(),
  });

  // qpdf pre-normalize is cheap and helps size stability.
  let workPdf = inPdf;
  const qpdfPdf = path.join(wd, `.__qpdf_${rand6()}.pdf`);
  try {
    const okQ = await qpdfFastRecompress({
      inPdf,
      outPdf: qpdfPdf,
      expiresAtIso,
    });
    if (okQ) workPdf = qpdfPdf;
  } catch {}

  const prePages = await qpdfPages(workPdf, expiresAtIso);
  const preBytes = safeStatSize(workPdf) ?? safeStatSize(inPdf) ?? 0;
  const targetTotalBytes = SYSTEM_TOTAL_CAP_BYTES;
  const floorTotalBytes = Math.min(targetTotalBytes, SYSTEM_TOTAL_FLOOR_BYTES);
  let afterCompressBytes = preBytes;
  let bestFitPdf = null;
  let bestFitBytes = 0;

  // Hard law: total output must fit <= 44MB.
  if (afterCompressBytes > targetTotalBytes && !softStop()) {
    await updateJob(jobId, {
      stage: "COMPRESS_HARDFIT",
      progress: 22,
      split_progress: 12,
      updated_at: nowIso(),
    });

    const passPlan = buildHardFitPassPlan({
      bytes: preBytes,
      pages: prePages,
      forceFast,
    });
    for (let i = 0; i < passPlan.length; i++) {
      if (softStop()) break;
      const pass = passPlan[i];
      const outPdf = path.join(wd, `.__hardfit_${i + 1}_${rand6()}.pdf`);
      const ok = await gsCompressOnce({
        inPdf: workPdf,
        outPdf,
        mode: "PRESET",
        dpi: pass.dpi,
        jpegQ: pass.jpegQ,
        pdfSettings: pass.pdfSettings || "/screen",
        expiresAtIso,
      });
      if (!ok) continue;
      const outBytes = safeStatSize(outPdf) ?? 0;
      if (outBytes <= 0) continue;

      workPdf = outPdf;
      afterCompressBytes = outBytes;

      // Keep the largest candidate that still satisfies <=44MB.
      if (outBytes <= targetTotalBytes && outBytes > bestFitBytes) {
        bestFitPdf = outPdf;
        bestFitBytes = outBytes;
      }

      // Stop when we are inside the target quality band [40MB..44MB].
      if (outBytes <= targetTotalBytes && outBytes >= floorTotalBytes) break;
    }

    if (bestFitPdf && bestFitBytes > 0) {
      workPdf = bestFitPdf;
      afterCompressBytes = bestFitBytes;
    }
  }

  if (afterCompressBytes > targetTotalBytes && !softStop()) {
    await updateJob(jobId, {
      stage: "COMPRESS_HARDFIT_NUCLEAR",
      progress: 38,
      split_progress: 24,
      updated_at: nowIso(),
    });
    const pN = path.join(wd, `.__hardfit_nuclear_${rand6()}.pdf`);
    const okN = await gsCompressOnce({
      inPdf: workPdf,
      outPdf: pN,
      mode: "NUCLEAR",
      expiresAtIso,
    });
    if (okN) {
      workPdf = pN;
      afterCompressBytes = safeStatSize(workPdf) ?? afterCompressBytes;
    }
  }

  await updateJob(jobId, {
    stage: "SPLIT",
    progress: 55,
    split_progress: 40,
    updated_at: nowIso(),
  });

  const targetBytes = Math.floor(SYSTEM_PART_BYTES * 0.985); // keep slight safety margin under 9MB
  const maxParts = SYSTEM_MAX_PARTS;
  const minPartsPolicy = Math.max(
    1,
    Math.min(maxParts, SYSTEM_MIN_PARTS, Math.max(1, prePages)),
  );

  const wipePartsDir = () => {
    try {
      for (const f of fs.readdirSync(partsDir)) {
        if (f.toLowerCase().endsWith(".pdf"))
          safeUnlink(path.join(partsDir, f));
      }
      // remove internal temp dirs if any
      safeRm(path.join(partsDir, ".__single_pages"));
    } catch {}
  };

  // Keep parts in [2..5] and near 9MB each.
  const splitForSystem = async (pdfPath) => {
    wipePartsDir();
    const pages0 = await qpdfPages(pdfPath, expiresAtIso);
    const maxPartsForFile = Math.min(maxParts, pages0);
    const minPartsForFile = Math.min(
      maxPartsForFile,
      pages0 >= 2 ? Math.max(2, minPartsPolicy) : 1,
    );
    const bytes0 = safeStatSize(pdfPath) ?? 0;
    const desiredParts = choosePartCountBySize({
      bytes: bytes0,
      pages: pages0,
      targetBytes,
      minParts: minPartsForFile,
      maxParts: maxPartsForFile,
    });

    const minParts = Math.max(
      desiredParts,
      estimatePartsForTarget({
        bytes: bytes0,
        targetBytes,
        maxParts: maxPartsForFile,
        pages: pages0,
      }),
    );

    let best = null;
    for (let parts = minParts; parts <= maxPartsForFile; parts++) {
      wipePartsDir();
      const ranges = buildEqualRanges(pages0, parts);
      const res = await splitSinglePass({
        inPdf: pdfPath,
        partsDir,
        ranges,
        expiresAtIso,
      });

      const peak = maxPartBytes(res.partMeta);
      if (!best || peak < best.peak) best = { res, peak, parts };
      if (peak <= targetBytes) return { ...res, _fit: true, _parts: parts };
    }

    return {
      ...(best?.res || { partFiles: [], partMeta: [] }),
      _fit: false,
      _parts: best?.parts ?? maxParts,
    };
  };

  let res = await splitForSystem(workPdf);

  // Push oversized parts down to <=9MB aggressively.
  if (maxPartBytes(res.partMeta) > targetBytes) {
    await updateJob(jobId, {
      stage: "PART_FIT_9MB",
      progress: 68,
      split_progress: 52,
      updated_at: nowIso(),
    });
    res = await shrinkOversizePartsAggressive({
      res,
      targetBytes,
      expiresAtIso,
      maxPasses: 2,
      onProgress: async ({ pass, passCount, done, total, maxPartBytes: mpb }) => {
        const localPct = total > 0 ? done / total : 0;
        const pct = Math.round(68 + ((pass - 1 + localPct) / passCount) * 18);
        await updateJob(jobId, {
          stage: "PART_FIT_9MB",
          progress: Math.min(95, Math.max(68, pct)),
          split_progress: Math.min(95, Math.max(52, pct)),
          max_part_mb: Math.round(bytesToMb(mpb) * 100) / 100,
          updated_at: nowIso(),
        });
      },
    });
  }

  // Fallback: page-level pack for highly uneven PDFs.
  if (maxPartBytes(res.partMeta) > targetBytes && !softStop()) {
    await updateJob(jobId, {
      stage: "OVERSIZE_SAFE_SPLIT",
      progress: 86,
      split_progress: 78,
      updated_at: nowIso(),
    });
    res = await splitOversizeSafe({
      inPdf: workPdf,
      partsDir,
      targetBytes,
      expiresAtIso,
    });
    if (maxPartBytes(res.partMeta) > targetBytes) {
      res = await shrinkOversizePartsAggressive({
        res,
        targetBytes,
        expiresAtIso,
        maxPasses: 2,
      });
    }
  }

  if (maxPartBytes(res.partMeta) > targetBytes) {
    console.warn(
      "[system-fit] hard-fit warning: some parts still exceed 9MB after aggressive fallback",
    );
  }

  return res;
}

// ----------------------
// Job runner
// ----------------------
function tmpDir(jobId) {
  const d = path.join(os.tmpdir(), `goodpdf_${jobId}_${rand6()}`);
  fs.mkdirSync(d, { recursive: true });
  return d;
}

async function processOneJob(job) {
  const jobId = job.id;
  const jobStartMs = Date.now();
  const queueWaitMs = job?.created_at
    ? Math.max(0, Date.now() - Date.parse(String(job.created_at)))
    : null;
  const timing = {
    claim_ms: 0,
    download_ms: 0,
    process_ms: 0,
    zip_ms: 0,
    upload_ms: 0,
    finalize_ms: 0,
    total_ms: 0,
    queue_wait_ms: queueWaitMs,
  };

  const tClaim = Date.now();
  const claimed = await claimJob(jobId);
  timing.claim_ms = Date.now() - tClaim;
  if (!claimed) return;

  const startedAtMs = Date.now();
  const wd = tmpDir(jobId);

  const inPdf = path.join(wd, "input.pdf");
  const partsDir = path.join(wd, "parts");
  const outZip = path.join(wd, "goodpdf.zip");

  const inputKey = normalizeInputKey(job);
  const outZipKey = outputZipKeyFor(job);

  const ttl = Number(job.ttl_minutes || 0);
  const ttlMinutes =
    Number.isFinite(ttl) && ttl > 0 ? ttl : DEFAULT_TTL_MINUTES;
  const expiresAtIso =
    job.expires_at ||
    job.delete_at ||
    new Date(Date.now() + ttlMinutes * 60_000).toISOString();

  try {
    await updateJob(jobId, {
      stage: "DOWNLOAD",
      progress: 5,
      split_progress: 0,
      expires_at: expiresAtIso,
      delete_at: expiresAtIso,
      updated_at: nowIso(),
    });
    const tDownload = Date.now();
    await downloadFromR2(inputKey, inPdf);
    timing.download_ms = Date.now() - tDownload;

    const inBytes = safeStatSize(inPdf) ?? 0;
    if (inBytes <= 0) {
      const e = new Error("INPUT_EMPTY");
      e.code = "INPUT_EMPTY";
      throw e;
    }
    const sigBuf = Buffer.alloc(5);
    const fd = fs.openSync(inPdf, "r");
    try {
      fs.readSync(fd, sigBuf, 0, 5, 0);
    } finally {
      fs.closeSync(fd);
    }
    const sig = sigBuf.toString("ascii");
    if (sig !== "%PDF-") {
      const e = new Error("INVALID_PDF_SIGNATURE");
      e.code = "INVALID_PDF_SIGNATURE";
      throw e;
    }

    await updateJob(jobId, {
      stage: "ANALYZE",
      progress: 7,
      split_progress: 1,
      updated_at: nowIso(),
    });
    const tAnalyze = Date.now();
    let analyzedPages = null;
    try {
      analyzedPages = await qpdfPages(inPdf, expiresAtIso);
    } catch {}
    const analyzeMs = Date.now() - tAnalyze;
    console.log("[JOB_ANALYZE]", {
      jobId,
      in_mb: Math.round(bytesToMb(inBytes) * 100) / 100,
      pages: analyzedPages,
      mb_per_page:
        analyzedPages && analyzedPages > 0
          ? Math.round((bytesToMb(inBytes) / analyzedPages) * 100) / 100
          : null,
      analyze_ms: analyzeMs,
    });

    const systemFit = true;

    let res;
    const tProcess = Date.now();
    await updateJob(jobId, {
      stage: "DEFAULT",
      progress: 6,
      split_progress: 1,
      updated_at: nowIso(),
    });
    res = await processDefaultFast({
      jobId,
      inPdf,
      wd,
      expiresAtIso,
      startedAtMs,
    });
    timing.process_ms = Date.now() - tProcess;

    // ZIP
    const tZip = Date.now();
    await updateJob(jobId, {
      stage: "ZIP",
      progress: 80,
      split_progress: 90,
      updated_at: nowIso(),
    });
    await zipParts(partsDir, outZip, expiresAtIso);
    timing.zip_ms = Date.now() - tZip;

    // UPLOAD
    const tUpload = Date.now();
    await updateJob(jobId, {
      stage: "UPLOAD_OUT",
      progress: 92,
      split_progress: 95,
      updated_at: nowIso(),
    });
    await uploadToR2(outZipKey, outZip, "application/zip");
    timing.upload_ms = Date.now() - tUpload;

    const outZipBytes = safeStatSize(outZip) ?? null;
    const partBytes = (res.partMeta || [])
      .map((p) => p.bytes)
      .filter((x) => typeof x === "number" && Number.isFinite(x));
    const totalPartsBytes = partBytes.length
      ? partBytes.reduce((a, b) => a + b, 0)
      : null;
    const maxPartB = partBytes.length ? Math.max(...partBytes) : null;
    const warningText =
      systemFit &&
      ((res.partFiles?.length || 0) > SYSTEM_MAX_PARTS ||
        (typeof maxPartB === "number" && maxPartB > SYSTEM_PART_BYTES))
        ? `Best effort output: could not fully fit <=${SYSTEM_PART_MB}MB within ${SYSTEM_MAX_PARTS} files for this PDF.`
        : null;

    const tFinalize = Date.now();
    await updateJob(jobId, {
      status: "DONE",
      stage: "DONE",
      progress: 100,
      split_progress: 100,
      output_zip_path: outZipKey,
      zip_path: outZipKey,
      ttl_minutes: ttlMinutes,
      expires_at: expiresAtIso,
      delete_at: expiresAtIso,
      parts_count: res.partFiles?.length || null,
      parts_json: res.partMeta || null,
      total_parts_bytes: totalPartsBytes,
      output_zip_bytes: outZipBytes,
      target_mb: SYSTEM_PART_MB,
      max_part_mb:
        typeof maxPartB === "number"
          ? Math.round(bytesToMb(maxPartB) * 100) / 100
          : null,
      warning_text: warningText,
      error_text: null,
      error_code: null,
      updated_at: nowIso(),
    });
    timing.finalize_ms = Date.now() - tFinalize;
    timing.total_ms = Date.now() - jobStartMs;
    console.log("[JOB_TIMING]", {
      jobId,
      systemFit,
      parts: res.partFiles?.length || 0,
      max_part_mb:
        typeof maxPartB === "number"
          ? Math.round(bytesToMb(maxPartB) * 100) / 100
          : null,
      ...timing,
    });
  } catch (e) {
    const msg = String(e?.message || e);
    timing.total_ms = Date.now() - jobStartMs;
    console.error("[JOB_TIMING_FAILED]", {
      jobId,
      ...timing,
      error: msg.slice(0, 200),
    });

    await updateJob(jobId, {
      status: "FAILED",
      stage: "FAILED",
      progress: 100,
      split_progress: 100,
      error_code: e?.code ? String(e.code) : "WORKER_ERROR",
      error_text: msg.slice(0, 800),
      updated_at: nowIso(),
    });
  } finally {
    safeRm(wd);
  }
}

// ----------------------
// Main loop
// ----------------------
async function main() {
  console.log("goodpdf worker started", {
    WORKER_ID,
    POLL_MS,
    CONCURRENCY,
    QUEUE_FETCH_LIMIT,
    SPLIT_PAR,
    R2_BUCKET_IN,
    R2_BUCKET_OUT,
    DEFAULT_TTL_MINUTES,
    DO_CLEANUP,
    CLEANUP_EVERY_MS,
    UX_CAP_MS,
    UX_SOFTSTOP_MS,
    SYSTEM_PART_MB,
    SYSTEM_MAX_PARTS,
    SYSTEM_TURBO_TRIGGER_MB,
    SYSTEM_TURBO_PAGE_TRIGGER,
    SYSTEM_FORCE_5_FROM_MB,
  });

  // Print critical env sanity (never print secrets)
  console.log("[ENV] SUPABASE_URL =", SUPABASE_URL);
  console.log(
    "[ENV] SUPABASE_SERVICE_ROLE_KEY present =",
    !!SUPABASE_SERVICE_ROLE_KEY,
  );
  console.log("[ENV] R2_ENDPOINT present =", !!R2_ENDPOINT);

  let lastCleanupAt = 0;
  let pollTick = 0;
  let lastIdlePollLogAt = 0;

  while (true) {
    try {
      const now = Date.now();
      if (DO_CLEANUP && now - lastCleanupAt >= CLEANUP_EVERY_MS) {
        lastCleanupAt = now;
        cleanupExpiredOutputs().catch((err) => {
          console.error("[cleanup] failed:", err?.message || err);
        });
      }

      const jobs = await fetchQueue(QUEUE_FETCH_LIMIT);
      pollTick++;

      if (!jobs.length) {
        const nowMs = Date.now();
        if (nowMs - lastIdlePollLogAt >= 30_000) {
          lastIdlePollLogAt = nowMs;
          console.log("[POLL] idle tick =", pollTick, "jobs = 0");
        }
        await sleep(POLL_MS);
        continue;
      }

      console.log(
        "[POLL] taking first job =",
        jobs[0]?.id,
        "status =",
        jobs[0]?.status,
      );

      await Promise.all(
        jobs.map((j) =>
          processOneJob(j).catch((err) => {
            console.error("[JOB_RUNNER] unhandled:", err?.message || err);
          }),
        ),
      );
    } catch (e) {
      console.error("Loop error:", e?.message || e);
      await sleep(POLL_MS);
    }
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
