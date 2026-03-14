// worker-local/worker.mjs — GOODPDF Worker (FAST v4.0)
// Core goals (locked):
//  - Zone A (<200MB): Smart Quality Split — fewest files first, 1→2→3→4→5 parts.
//    Fallback: compress toward ~43–45MB (stop at <=45MB), then exact 5-part split.
//  - Zone B (200–500MB): Hard Limit Split — aggressive compression, <=10 parts near 9MB each.
//  - MANUAL: user target MB (default 9) + quality presets:
//      High   = 110 DPI / Q50
//      Medium = 100 DPI / Q45
//      Original = no compression
//  - Speed-first: avoid multi-pass loops. qpdf used for splitting.
//  - Oversize single-page (>target) will be emitted as OVERSIZE_pageNN.pdf (no fail).

import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
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
import {
  estimatePartsForTarget,
  buildRangesByEstimatedSize,
  buildRangesFromPageBytes,
} from "./tools/scan_engine.mjs";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
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
const WORKER_PROFILE = String(
  process.env.WORKER_PROFILE || "normal",
).toLowerCase();
const IS_HEAVY_PROFILE = WORKER_PROFILE === "heavy";
const NORMAL_CAN_PROCESS_HEAVY =
  String(process.env.NORMAL_CAN_PROCESS_HEAVY || "true").toLowerCase() !==
  "false";
const QUEUE_STAGE = IS_HEAVY_PROFILE ? "QUEUE_HEAVY" : "QUEUE";
const ACCEPTED_QUEUE_STAGES = IS_HEAVY_PROFILE
  ? ["QUEUE_HEAVY"]
  : NORMAL_CAN_PROCESS_HEAVY
    ? ["QUEUE", "QUEUE_HEAVY"]
    : ["QUEUE"];

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
const POLL_IDLE_MAX_MS = Math.max(
  60_000,
  Number(process.env.POLL_IDLE_MAX_MS) || 60_000,
);
const POLL_IDLE_LOG_EVERY_MS = Math.max(
  5_000,
  Number(process.env.POLL_IDLE_LOG_EVERY_MS) || 30_000,
);
const MAX_IDLE_BACKOFF_MS = Math.max(
  60_000,
  Math.min(120_000, Number(process.env.POLL_IDLE_MAX_MS) || 60_000),
);
const WORKER_IDLE_EXIT_MS = Math.max(
  0,
  Number(process.env.WORKER_IDLE_EXIT_MS || 0),
);
const CPU_CORES = Math.max(1, Number(os.cpus()?.length || 1));
const CONCURRENCY = Math.max(
  1,
  Math.min(
    Number(process.env.CONCURRENCY || 2),
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
const QUEUE_NEWEST_FIRST =
  String(process.env.QUEUE_NEWEST_FIRST || "true").toLowerCase() !== "false";
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
const STALE_RECOVERY_EVERY_MS = Math.max(
  10_000,
  Number(process.env.STALE_RECOVERY_EVERY_MS || 20_000),
);
const CLAIM_STALE_MS = Math.max(
  60_000,
  Number(process.env.CLAIM_STALE_MS || 6 * 60_000),
);
const HEARTBEAT_MS = Math.max(5_000, Number(process.env.HEARTBEAT_MS || 8_000));
const MAX_STALE_RECOVERY_BATCH = Math.max(
  1,
  Number(process.env.MAX_STALE_RECOVERY_BATCH || 20),
);
const ENABLE_LOCAL_INGEST_FAST_PATH =
  String(process.env.ENABLE_LOCAL_INGEST_FAST_PATH || "false").toLowerCase() ===
  "true";
const USE_PDF_WORKER =
  String(process.env.USE_PDF_WORKER || "false").toLowerCase() === "true";

// Binaries
const GS_EXE = process.env.GS_EXE || "gs";
const QPDF_EXE = process.env.QPDF_EXE || "qpdf";

// Timeouts (bounded by expires_at too)
const TIMEOUT_QPDF_MS = Math.max(
  10_000,
  Number(process.env.TIMEOUT_QPDF_MS || 120_000),
);
const TIMEOUT_GS_MS = Math.max(
  10_000,
  Number(process.env.TIMEOUT_GS_MS || 75_000),
);
const TIMEOUT_7Z_MS = Math.max(
  10_000,
  Number(process.env.TIMEOUT_7Z_MS || 120_000),
);

// UX cap (best-effort)
const UX_CAP_MS = Math.max(
  60_000,
  Number(process.env.UX_CAP_MS || (IS_HEAVY_PROFILE ? 420_000 : 180_000)),
); // normal ~3min, heavy ~7min default cap
const UX_SOFTSTOP_MS = Math.max(
  50_000,
  Math.min(UX_CAP_MS - 10_000, Number(process.env.UX_SOFTSTOP_MS || 150_000)),
);

// DEFAULT system-fit policy: 9MB per part
const SYSTEM_PART_MB = Number(process.env.SYSTEM_PART_MB || 9);
const SYSTEM_PART_BYTES = Math.floor(SYSTEM_PART_MB * 1024 * 1024);
// Legacy compat cap (used for some env-driven knobs; Manual mode can override)
const SYSTEM_MAX_PARTS = Number(process.env.SYSTEM_MAX_PARTS || 5);
const SYSTEM_TARGET_PARTS_GOAL = 5;
const SYSTEM_MAX_PARTS_DEFAULT = 5; // hard cap for DEFAULT; Manual can exceed this
// Absolute hard cap for dynamic system-fit expansion (final acceptance gate).
const SYSTEM_MAX_PARTS_HARD_CAP = 25;
// Zone A: preferred ~43–44MB, hard stop <=45MB. Used when fewest-files-first fails; then exact 5-part split.
const ZONE_A_TARGET_MB_IDEAL_MIN = 43;
const ZONE_A_TARGET_MB_IDEAL_MAX = 44;
const ZONE_A_TARGET_MB_HARD_MAX = 45;
const ZONE_A_TOTAL_TARGET_HARD_MAX = ZONE_A_TARGET_MB_HARD_MAX * 1024 * 1024;
const SYSTEM_TARGET_TOTAL_BYTES = Math.floor(
  SYSTEM_TARGET_PARTS_GOAL * SYSTEM_PART_BYTES * 0.97,
);
const SYSTEM_DEFAULT_MAX_INPUT_BYTES = 500 * 1024 * 1024; // 500MB hard cap for DEFAULT
const SYSTEM_TOTAL_CAP_BYTES = Math.floor(
  SYSTEM_PART_BYTES * SYSTEM_MAX_PARTS_DEFAULT - 512 * 1024, // headroom
);
const SOFT_TARGET_BYTES = SYSTEM_TARGET_TOTAL_BYTES;
const SOFT_FLOOR_BYTES = Math.floor(SOFT_TARGET_BYTES * 0.3);
const HEAVY_FLOOR_BYTES = Math.floor(SYSTEM_TOTAL_CAP_BYTES * 0.8); // Zone B: avoid over-shrinking heavy inputs
const SYSTEM_TOTAL_FLOOR_MB = Math.max(
  10,
  Math.min(
    SYSTEM_PART_MB * SYSTEM_MAX_PARTS_DEFAULT,
    Number(process.env.SYSTEM_TOTAL_FLOOR_MB || 40),
  ),
);
const SYSTEM_TOTAL_FLOOR_BYTES = Math.floor(
  SYSTEM_TOTAL_FLOOR_MB * 1024 * 1024,
);
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
  Math.min(SYSTEM_MAX_PARTS, Number(process.env.SYSTEM_MIN_PARTS || 1)),
);
const QPDF_PRE_NORMALIZE_MAX_BYTES = 25 * 1024 * 1024; // skip qpdf pre-normalize above 25MB
const QPDF_PRE_NORMALIZE_TIMEOUT_MS = 12_000; // hard timeout; fall back to inPdf on timeout
const HEAVY_TARGET_MARGIN = Math.max(
  0.94,
  Math.min(0.99, Number(process.env.HEAVY_TARGET_MARGIN || 0.975)),
);
const HEAVY_EXTRA_SHRINK_PASSES = Math.max(
  1,
  Math.min(6, Number(process.env.HEAVY_EXTRA_SHRINK_PASSES || 4)),
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
function tmark(obj, key) {
  obj[key] = Date.now();
}
function td(obj, a, b) {
  return (obj[b] ?? 0) - (obj[a] ?? 0);
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

// Exit handling for qpdf: 0 = success, 3 = success_with_warning (stderr → warning_text), other = fail.
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
      resolve({ out, err, exitCode: code });
    });
  });
}

// ----------------------
// Python helper (Selective recompression)

function pickPythonExe() {
  const env = String(process.env.PYTHON_EXE || "").trim();
  if (env) return env;
  // Common fallbacks
  if (process.platform === "win32") return "python";
  return "python3";
}

async function runZoneACompressionLadder(
  inputPdfPath,
  outDir,
  expiresAtIso,
  jobId = null,
) {
  // Stage A: quality-preserving ladder (current behavior).
  // Stage B: mild additional image-quality sacrifice, only if Stage A cannot reach <=45MB.
  const ladderStageA = [
    { target: 70, passes: 1 },
    { target: 60, passes: 2 },
    { target: 50, passes: 2 },
    { target: 45, passes: 3 },
  ];
  const ladderStageB = [
    // Slightly stronger than final Stage A step; still non-destructive.
    { target: 45, passes: 3, jpegQ: 38, maxSide: 1350 },
    { target: 45, passes: 4, jpegQ: 36, maxSide: 1300 },
  ];
  const hardMaxBytes = ZONE_A_TOTAL_TARGET_HARD_MAX;
  let curInput = inputPdfPath;
  let prevOutput = null;
  const pyExe = pickPythonExe();
  const truncLog = (s, n = 4000) =>
    typeof s === "string" && s.length > n ? s.slice(0, n) + "..." : s || "";
  const inputBytes = safeStatSize(inputPdfPath) ?? 0;

  console.log("[ZONE_A_COMPRESS_START]", {
    jobId,
    inputMb: Math.round(bytesToMb(inputBytes) * 100) / 100,
    intent: "preserve quality, reduce toward ~43–45MB, stop at <=45MB",
  });

  // Stage A — preserve quality first.
  console.log("[ZONE_A_COMPRESS_STAGE_A]", { jobId, stage: "A" });
  for (let i = 0; i < ladderStageA.length; i++) {
    const step = ladderStageA[i];
    const outPath = path.join(
      outDir,
      `.__zone_a_ladder_${step.target}_${rand6()}.pdf`,
    );
    const tPy = boundedTimeout(120_000, expiresAtIso, 20_000, 120_000);
    const curInputExists = safeExists(curInput);
    const curInputSize = safeStatSize(curInput);
    console.log("[ZONE_A_COMPRESS_PY_INVOKE]", {
      jobId,
      stage: "A",
      stepTarget: step.target,
      inputPath: curInput,
      inputExists: curInputExists,
      inputSizeBytes: curInputSize,
      outputPath: outPath,
    });
    const pyResultA = await runCmd(
      pyExe,
      [
        path.join(__dirname, "tools", "selective_recompress.py"),
        curInput,
        outPath,
        "--target_mb",
        String(step.target),
        "--passes",
        String(step.passes),
        "--top_k",
        "220",
        "--min_stream_kb",
        "180",
        "--max_side",
        "1400",
        "--jpeg_q",
        "42",
        "--force",
      ],
      tPy,
      {},
      [0],
    );
    if (pyResultA.out) {
      console.log("[ZONE_A_COMPRESS_PY_STDOUT]", truncLog(pyResultA.out));
    }
    if (pyResultA.err) {
      console.log("[ZONE_A_COMPRESS_PY_STDERR]", truncLog(pyResultA.err));
    }

    const outBytes = safeStatSize(outPath) ?? 0;
    const resultMb = Math.round(bytesToMb(outBytes) * 100) / 100;
    console.log("[ZONE_A_COMPRESS_STEP]", {
      jobId,
      stage: "A",
      stepTarget: step.target,
      resultMb,
    });

    if (prevOutput && prevOutput !== inputPdfPath) safeUnlink(prevOutput);
    prevOutput = outPath;

    if (outBytes > 0 && outBytes <= hardMaxBytes) {
      const finalMb = Math.round(bytesToMb(outBytes) * 100) / 100;
      console.log("[ZONE_A_COMPRESS_STOP]", {
        jobId,
        resultMb: finalMb,
        reason: "under_hard_max",
      });
      console.log("[ZONE_A_COMPRESS_RESULT]", {
        finalMb,
        inIdealRange:
          finalMb >= ZONE_A_TARGET_MB_IDEAL_MIN &&
          finalMb <= ZONE_A_TARGET_MB_IDEAL_MAX,
        hardMaxReached: finalMb <= ZONE_A_TARGET_MB_HARD_MAX,
      });
      return outPath;
    }

    curInput = outPath;
  }

  // Stage B — allow mild additional image-quality sacrifice if still above 45MB.
  curInput = prevOutput || inputPdfPath;
  console.log("[ZONE_A_COMPRESS_STAGE_B]", { jobId, stage: "B" });
  for (let i = 0; i < ladderStageB.length; i++) {
    const step = ladderStageB[i];
    const outPath = path.join(
      outDir,
      `.__zone_a_ladder_B${step.target}_${rand6()}.pdf`,
    );
    const tPy = boundedTimeout(120_000, expiresAtIso, 20_000, 120_000);
    const curInputExists = safeExists(curInput);
    const curInputSize = safeStatSize(curInput);
    console.log("[ZONE_A_COMPRESS_PY_INVOKE]", {
      jobId,
      stage: "B",
      stepTarget: step.target,
      inputPath: curInput,
      inputExists: curInputExists,
      inputSizeBytes: curInputSize,
      outputPath: outPath,
    });
    const pyResultB = await runCmd(
      pyExe,
      [
        path.join(__dirname, "tools", "selective_recompress.py"),
        curInput,
        outPath,
        "--target_mb",
        String(step.target),
        "--passes",
        String(step.passes),
        "--top_k",
        "220",
        "--min_stream_kb",
        "180",
        "--max_side",
        String(step.maxSide || 1400),
        "--jpeg_q",
        String(step.jpegQ || 42),
        "--force",
      ],
      tPy,
      {},
      [0],
    );
    if (pyResultB.out) {
      console.log("[ZONE_A_COMPRESS_PY_STDOUT]", truncLog(pyResultB.out));
    }
    if (pyResultB.err) {
      console.log("[ZONE_A_COMPRESS_PY_STDERR]", truncLog(pyResultB.err));
    }

    const outBytes = safeStatSize(outPath) ?? 0;
    const resultMb = Math.round(bytesToMb(outBytes) * 100) / 100;
    console.log("[ZONE_A_COMPRESS_STEP]", {
      jobId,
      stage: "B",
      stepTarget: step.target,
      resultMb,
    });

    if (prevOutput && prevOutput !== inputPdfPath) safeUnlink(prevOutput);
    prevOutput = outPath;

    if (outBytes > 0 && outBytes <= hardMaxBytes) {
      const finalMb = Math.round(bytesToMb(outBytes) * 100) / 100;
      console.log("[ZONE_A_COMPRESS_STOP]", {
        jobId,
        resultMb: finalMb,
        reason: "under_hard_max",
        stage: "B",
      });
      console.log("[ZONE_A_COMPRESS_RESULT]", {
        finalMb,
        inIdealRange:
          finalMb >= ZONE_A_TARGET_MB_IDEAL_MIN &&
          finalMb <= ZONE_A_TARGET_MB_IDEAL_MAX,
        hardMaxReached: finalMb <= ZONE_A_TARGET_MB_HARD_MAX,
      });
      return outPath;
    }

    curInput = outPath;
  }

  const finalMb = prevOutput
    ? Math.round(bytesToMb(safeStatSize(prevOutput) ?? 0) * 100) / 100
    : null;
  console.log("[ZONE_A_COMPRESS_STOP]", {
    jobId,
    resultMb: finalMb,
    reason: "ladder_exhausted",
  });
  if (finalMb != null) {
    console.log("[ZONE_A_COMPRESS_RESULT]", {
      finalMb,
      inIdealRange:
        finalMb >= ZONE_A_TARGET_MB_IDEAL_MIN &&
        finalMb <= ZONE_A_TARGET_MB_IDEAL_MAX,
      hardMaxReached: finalMb <= ZONE_A_TARGET_MB_HARD_MAX,
    });
  }
  return prevOutput;
}

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
      "id,status,stage,error_code,created_at,input_path,claimed_by,split_mb,ttl_minutes,expires_at,delete_at",
    )
    .eq("status", "QUEUED")
    .is("claimed_by", null)
    .order("created_at", { ascending: !QUEUE_NEWEST_FIRST })
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

async function claimJob(jobId, queueStage = "QUEUE") {
  const claimOwner = `${WORKER_PROFILE}:${WORKER_ID}`;
  const safeQueueStage =
    String(queueStage || "QUEUE").toUpperCase() === "QUEUE_HEAVY"
      ? "QUEUE_HEAVY"
      : "QUEUE";
  const patch = {
    status: "PROCESSING",
    stage: safeQueueStage,
    progress: 1,
    split_progress: 0,
    claimed_by: claimOwner,
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

async function heartbeatJob(jobId, stage = "PROCESSING") {
  try {
    await updateJob(jobId, {
      status: "PROCESSING",
      stage,
      claimed_by: `${WORKER_PROFILE}:${WORKER_ID}`,
      updated_at: nowIso(),
    });
  } catch {}
}

async function requeueStaleProcessingJobs() {
  const staleIso = new Date(Date.now() - CLAIM_STALE_MS).toISOString();
  const { data: rows, error } = await supabase
    .from("jobs")
    .select("id,updated_at,claimed_by,status")
    .eq("status", "PROCESSING")
    .lt("updated_at", staleIso)
    .limit(MAX_STALE_RECOVERY_BATCH);

  if (error || !rows?.length) return 0;

  let recovered = 0;
  for (const r of rows) {
    try {
      const claimedBy = String(r?.claimed_by || "").toLowerCase();
      const queueStageForRecover = claimedBy.startsWith("heavy:")
        ? "QUEUE_HEAVY"
        : "QUEUE";
      const { data: ok } = await supabase
        .from("jobs")
        .update({
          status: "QUEUED",
          stage: queueStageForRecover,
          progress: 1,
          split_progress: 0,
          claimed_by: null,
          claimed_at: null,
          error_code: "RECOVERED_STALE",
          error_text: null,
          updated_at: nowIso(),
        })
        .eq("id", r.id)
        .eq("status", "PROCESSING")
        .select("id")
        .maybeSingle();
      if (ok?.id) recovered += 1;
    } catch {}
  }
  return recovered;
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
async function qpdfPages(inPdf, expiresAtIso, warnings = null) {
  const tQ = Math.min(
    boundedTimeout(TIMEOUT_QPDF_MS, expiresAtIso, 10_000, 60_000),
    25_000,
  );
  const result = await runCmd(
    QPDF_EXE,
    ["--show-npages", inPdf],
    tQ,
    {},
    [0, 3],
  );
  if (result.exitCode === 3 && Array.isArray(warnings)) {
    warnings.push((result.err || "").trim().slice(0, 1000));
  }
  const pages = Number(String(result.out).trim());
  if (!Number.isFinite(pages) || pages <= 0) {
    const e = new Error("NPAGES_FAILED");
    e.code = "NPAGES_FAILED";
    throw e;
  }
  return pages;
}

const KILL_TREE_GRACE_MS = 800;

function killProcessTree(child) {
  if (child == null) return;
  if (child.exitCode != null) return;
  try {
    child.kill();
  } catch {}
  setTimeout(() => {
    if (child.exitCode != null) return;
    try {
      if (process.platform === "win32") {
        spawn("taskkill", ["/PID", String(child.pid), "/T", "/F"], {
          stdio: "ignore",
          windowsHide: true,
        });
      } else {
        child.kill("SIGKILL");
      }
    } catch {}
  }, KILL_TREE_GRACE_MS);
}

// very fast, stable recompress/normalize (good before GS)
// Returns { promise, child }: promise resolves true/false; never rejects on nonzero exit (fallback to inPdf).
function qpdfFastRecompress({ inPdf, outPdf }) {
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
  const child = spawn(QPDF_EXE, args, {
    stdio: ["ignore", "pipe", "pipe"],
  });
  let errText = "";
  child.stderr.on("data", (d) => (errText += d.toString()));

  const promise = new Promise((resolve, reject) => {
    child.on("close", (code) => {
      const outBytes = safeStatSize(outPdf) ?? 0;

      if (code === 0) {
        resolve({ ok: outBytes > 0 });
        return;
      }
      if (code === 3) {
        // qpdf exit 3 = success_with_warning; capture stderr for warning_text
        resolve({
          ok: outBytes > 0,
          warning: (errText || "").trim().slice(0, 1000),
        });
        return;
      }

      // any other non-zero → fail (do not treat as success even if output exists)
      resolve({ ok: false });
    });
    child.on("error", (err) => reject(err));
  });
  return { promise, child };
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

async function estimatePageBytes(
  { inPdf, pages, probeDir, expiresAtIso },
  warnings = null,
) {
  safeRm(probeDir);
  fs.mkdirSync(probeDir, { recursive: true });
  const pageNums = [];
  for (let p = 1; p <= pages; p++) pageNums.push(p);

  const rows = await asyncPool(SPLIT_PAR, pageNums, async (p) => {
    const onePath = path.join(probeDir, `p${String(p).padStart(5, "0")}.pdf`);
    const b = await qpdfExtractPage(
      {
        inPdf,
        pageNum: p,
        outPdf: onePath,
        expiresAtIso,
      },
      warnings,
    );
    return { p, bytes: b };
  });

  safeRm(probeDir);
  rows.sort((a, b) => a.p - b.p);
  return rows.map((r) => Math.max(0, Number(r.bytes || 0)));
}

function buildRangesNearTarget({ pageBytes, targetBytes, maxParts }) {
  const pages = pageBytes.length;
  if (pages <= 0) return [{ start: 1, end: 1 }];

  const totalBytes = pageBytes.reduce((a, b) => a + b, 0);
  const parts = Math.max(
    1,
    Math.min(
      maxParts,
      pages,
      Math.ceil(
        totalBytes / Math.max(256 * 1024, Math.floor(targetBytes * 0.96)),
      ),
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
      Math.max(
        256 * 1024,
        Math.floor(remainingBytes / Math.max(1, remainingParts)),
      ),
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

async function splitSinglePass(
  { inPdf, partsDir, ranges, expiresAtIso },
  warnings = null,
) {
  fs.mkdirSync(partsDir, { recursive: true });

  const tEach = boundedTimeout(TIMEOUT_QPDF_MS, expiresAtIso, 15_000, 60_000);
  const tmpName = (i, s, e) =>
    path.join(
      partsDir,
      `.__range_${String(i + 1).padStart(3, "0")}_${s}_${e}_${rand6()}.pdf`,
    );

  const results = await asyncPool(SPLIT_PAR, ranges, async (r, i) => {
    const outPath = tmpName(i, r.start, r.end);
    const result = await runCmd(
      QPDF_EXE,
      ["--empty", "--pages", inPdf, `${r.start}-${r.end}`, "--", outPath],
      tEach,
      {},
      [0, 3],
    );
    if (result.exitCode === 3 && Array.isArray(warnings)) {
      warnings.push((result.err || "").trim().slice(0, 1000));
    }
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
  zoneA = false,
}) {
  const fileByName = new Map(
    (res.partFiles || []).map((p) => [path.basename(p), p]),
  );
  // Zone A (<200MB): gentle, quality-preserving part-fit ladder.
  // Zone B (>=200MB): retain existing aggressive ladder for hard-fit behavior.
  const passes = zoneA
    ? [
        { dpi: 150, jpegQ: 60, pdfSettings: "/ebook" },
        { dpi: 135, jpegQ: 54, pdfSettings: "/ebook" },
        { dpi: 120, jpegQ: 50, pdfSettings: "/screen" },
      ]
    : [
        { dpi: 42, jpegQ: 14, pdfSettings: "/screen" },
        { dpi: 36, jpegQ: 10, pdfSettings: "/screen" },
        { dpi: 30, jpegQ: 8, pdfSettings: "/screen" },
      ];

  // For Zone A we keep a soft lower guard so we don't keep shrinking already-small parts.
  const lowerGuardBytes = zoneA
    ? Math.max(Math.floor(targetBytes * 0.55), 5 * 1024 * 1024)
    : 0;

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
          pdfSettings: pass.pdfSettings,
        });
        // Stop tracking as oversize once it is reasonably close to target.
        // For Zone A, avoid driving parts far below the useful 5–9MB band.
        meta.bytes = nb;
        meta.sizeMb = Math.round(bytesToMb(nb) * 10) / 10;
        if (zoneA && nb > 0 && nb < lowerGuardBytes) {
          console.log("[ZONE_A_PART_RESCUE]", {
            name: meta.name,
            bytesMb: Math.round(bytesToMb(nb) * 100) / 100,
            targetMb: Math.round(bytesToMb(targetBytes) * 100) / 100,
            lowerGuardMb: Math.round(bytesToMb(lowerGuardBytes) * 100) / 100,
          });
        }
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
async function qpdfExtractPage(
  { inPdf, pageNum, outPdf, expiresAtIso },
  warnings = null,
) {
  const tQ = boundedTimeout(TIMEOUT_QPDF_MS, expiresAtIso, 15_000, 60_000);
  const result = await runCmd(
    QPDF_EXE,
    ["--empty", "--pages", inPdf, `${pageNum}-${pageNum}`, "--", outPdf],
    tQ,
    {},
    [0, 3],
  );
  if (result.exitCode === 3 && Array.isArray(warnings)) {
    warnings.push((result.err || "").trim().slice(0, 1000));
  }
  const b = safeStatSize(outPdf) ?? 0;
  return b;
}

async function qpdfConcatSinglePages(
  { pagePdfs, outPdf, expiresAtIso },
  warnings = null,
) {
  // Each file in pagePdfs is a 1-page PDF. We stitch them into one multi-page PDF.
  const tQ = boundedTimeout(TIMEOUT_QPDF_MS, expiresAtIso, 20_000, 60_000);
  const args = ["--empty", "--pages"];
  for (const p of pagePdfs) args.push(p, "1");
  args.push("--", outPdf);
  const result = await runCmd(QPDF_EXE, args, tQ, {}, [0, 3]);
  if (result.exitCode === 3 && Array.isArray(warnings)) {
    warnings.push((result.err || "").trim().slice(0, 1000));
  }
  const b = safeStatSize(outPdf) ?? 0;
  return b;
}

async function splitOversizeSafe(
  { inPdf, partsDir, targetBytes, expiresAtIso, maxParts },
  warnings = null,
) {
  const effectiveMaxParts =
    Number.isFinite(maxParts) && maxParts > 0 ? maxParts : 10;
  // Fallback when range-split can't enforce <=targetBytes. Cap at maxParts (policy-based).
  fs.mkdirSync(partsDir, { recursive: true });

  const pages = await qpdfPages(inPdf, expiresAtIso, warnings);

  const oneDir = path.join(partsDir, ".__single_pages");
  safeRm(oneDir);
  fs.mkdirSync(oneDir, { recursive: true });

  const pageNums = [];
  for (let p = 1; p <= pages; p++) pageNums.push(p);
  const single = await asyncPool(SPLIT_PAR, pageNums, async (p) => {
    const onePath = path.join(oneDir, `p${String(p).padStart(5, "0")}.pdf`);
    const b = await qpdfExtractPage(
      {
        inPdf,
        pageNum: p,
        outPdf: onePath,
        expiresAtIso,
      },
      warnings,
    );
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

  // 3-B) Greedy pack remaining normal pages; cap total parts at effectiveMaxParts
  const maxPackedParts = Math.max(1, effectiveMaxParts - partFiles.length);
  let pack = [];
  let packBytes = 0;
  let packStart = null;
  let packEnd = null;

  const flushPack = async () => {
    if (!pack.length) return;
    const idx = partFiles.length + 1;
    const outName = `goodPDF-OVERSIZEPACK(${idx}).pdf`;
    const outPath = path.join(partsDir, outName);
    await qpdfConcatSinglePages(
      {
        pagePdfs: pack.map((x) => x.path),
        outPdf: outPath,
        expiresAtIso,
      },
      warnings,
    );
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

    // if adding exceeds, flush current and start new (unless we'd exceed effectiveMaxParts)
    const wouldExceedCap =
      partFiles.length + 1 > effectiveMaxParts && pack.length > 0;
    if (
      !wouldExceedCap &&
      packBytes + it.bytes > Math.floor(targetBytes * 0.98)
    ) {
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
  timeoutMs,
}) {
  const tGS = boundedTimeout(TIMEOUT_GS_MS, expiresAtIso, 20_000, 90_000);
  const effectiveTimeout = timeoutMs != null ? Math.min(timeoutMs, tGS) : tGS;

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

  const child = spawn(GS_EXE, args, {
    stdio: ["ignore", "pipe", "pipe"],
  });
  const promise = new Promise((resolve, reject) => {
    let err = "";
    child.stderr.on("data", (d) => (err += d.toString()));
    child.on("close", (code) => {
      if (code !== 0) {
        return reject(new Error(`gs failed code=${code}`));
      }
      resolve();
    });
    child.on("error", reject);
  });

  let timeoutId = null;
  const timeoutPromise =
    effectiveTimeout > 0 && Number.isFinite(effectiveTimeout)
      ? new Promise((_, rej) => {
          timeoutId = setTimeout(() => {
            killProcessTree(child);
            console.warn("[gs] pass timeout, falling back to split");
            rej(Object.assign(new Error("GS timeout"), { code: "GS_TIMEOUT" }));
          }, effectiveTimeout);
        })
      : null;

  try {
    if (timeoutPromise) {
      await Promise.race([
        promise.finally(() => {
          if (timeoutId != null) clearTimeout(timeoutId);
        }),
        timeoutPromise,
      ]);
    } else {
      await promise;
    }
    const b = safeStatSize(outPdf) ?? 0;
    return b > 0;
  } catch (e) {
    if (e?.code === "GS_TIMEOUT") {
      // already logged in timeout callback
    }
    return false;
  }
}

const GS_HEARTBEAT_MS = 2500;

async function runGsWithHeartbeat({ jobId, label, runFn }) {
  let failStreak = 0;
  const timer = setInterval(() => {
    updateJob(jobId, {
      stage: "compress",
      detail: label || "Ghostscript",
      heartbeat: Date.now(),
      updated_at: nowIso(),
    })
      .then(() => {
        failStreak = 0;
      })
      .catch(() => {
        failStreak++;
        if (failStreak >= 5) clearInterval(timer);
      });
  }, GS_HEARTBEAT_MS);
  try {
    return await runFn();
  } finally {
    clearInterval(timer);
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
  minParts = 1,
  maxParts = 10,
}) {
  const b = Math.max(0, Number(bytes || 0));
  const p = Math.max(1, Number(pages || 1));
  const t = Math.max(256 * 1024, Number(targetBytes || SYSTEM_PART_BYTES));
  const maxP = Math.max(1, Math.min(Number(maxParts || 10), p));

  const ideal = Math.ceil(b / t);
  return Math.max(1, Math.min(maxP, ideal));
}

// Max 2 Ghostscript passes: normal (quality-first), rescue (if needed). No multi-pass loops.
// Compatibility signature retained; behavior locked to 2 passes.
function buildHardFitPassPlan({ bytes, pages, forceFast } = {}) {
  return [
    { dpi: 110, jpegQ: 50, pdfSettings: "/ebook" }, // normal
    { dpi: 85, jpegQ: 45, pdfSettings: "/screen" }, // rescue
  ];
}

function heavyTailPasses() {
  // Extra ladder for QUEUE_HEAVY jobs (controlled quality drop).
  return [
    { dpi: 60, jpegQ: 24, pdfSettings: "/screen" },
    { dpi: 52, jpegQ: 20, pdfSettings: "/screen" },
    { dpi: 46, jpegQ: 16, pdfSettings: "/screen" },
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
  heavyLane = false,
}) {
  const effectiveSoftStopMs = heavyLane
    ? Math.max(UX_SOFTSTOP_MS, 300_000)
    : UX_SOFTSTOP_MS;
  const elapsed = () => Date.now() - startedAtMs;
  const softStop = () => elapsed() >= effectiveSoftStopMs;

  const partsDir = path.join(wd, "parts");
  fs.mkdirSync(partsDir, { recursive: true });
  const preInBytes = safeStatSize(inPdf) ?? 0;
  const T = {};
  tmark(T, "start");
  const qpdfWarnings = [];

  await updateJob(jobId, {
    stage: "PREFLIGHT",
    progress: 8,
    split_progress: 2,
    updated_at: nowIso(),
  });

  // Hard input cap: refuse only when raw PDF is over 500MB.
  if (preInBytes > SYSTEM_DEFAULT_MAX_INPUT_BYTES) {
    const err = new Error(
      "Over 500MB. Please use Compress tool or Manual split.",
    );
    err.code = "REFUSED_TOO_LARGE";
    throw err;
  }

  // qpdf pre-normalize when input <= 25MB; hard timeout kills process, fall back to inPdf on timeout/failure.
  let workPdf = inPdf;
  if (preInBytes <= QPDF_PRE_NORMALIZE_MAX_BYTES) {
    const qpdfPdf = path.join(wd, `.__qpdf_${rand6()}.pdf`);
    const { promise, child } = qpdfFastRecompress({
      inPdf,
      outPdf: qpdfPdf,
    });
    let timeoutId = null;
    const timeoutPromise = new Promise((_, rej) => {
      timeoutId = setTimeout(() => {
        killProcessTree(child);
        console.warn("[qpdf] killed due to timeout");
        rej(
          Object.assign(new Error("timeout"), {
            code: "QPDF_PRE_NORMALIZE_TIMEOUT",
          }),
        );
      }, QPDF_PRE_NORMALIZE_TIMEOUT_MS);
    });
    try {
      const result = await Promise.race([promise, timeoutPromise]);
      if (result?.ok) workPdf = qpdfPdf;
      if (result?.warning) qpdfWarnings.push(result.warning);
    } catch (e) {
      // workPdf stays inPdf on timeout or qpdf failure; pipeline continues
    } finally {
      if (timeoutId != null) clearTimeout(timeoutId);
    }
  }
  tmark(T, "qpdf");

  const prePages = await qpdfPages(workPdf, expiresAtIso, qpdfWarnings);
  const preBytes = safeStatSize(workPdf) ?? safeStatSize(inPdf) ?? 0;
  let afterCompressBytes = preBytes;
  let anyShrink = false;

  const partCapBytes = SYSTEM_PART_BYTES;
  const maxPartsGoal = SYSTEM_TARGET_PARTS_GOAL;
  const totalCapBytes = SYSTEM_TOTAL_CAP_BYTES;
  const isHeavyInput = preBytes >= 200 * 1024 * 1024;
  const heavyFloorBytes = isHeavyInput
    ? Math.max(30 * 1024 * 1024, HEAVY_FLOOR_BYTES)
    : 0;
  // Single source of truth for policy: <200MB => 5 parts, 200–500MB => 10 parts.
  const effectiveMaxParts = preInBytes < 200 * 1024 * 1024 ? 5 : 10;
  // Zone A: fewest-files-first, minimum damage; accept any valid fit (no total-size floor rejection).
  // Zone B: aggressive compression, <=effectiveMaxParts (5 or 10); uses total cap and quality floor.
  const effectiveTotalCap = isHeavyInput ? totalCapBytes : preBytes;
  const effectiveFloorBytes = isHeavyInput ? heavyFloorBytes : 0; // Zone B only; Zone A never uses

  console.log("[POLICY_LOCK]", {
    jobId,
    isHeavyInput,
    effectiveMaxParts,
    effectiveTotalCapMb:
      Math.round((effectiveTotalCap / 1024 / 1024) * 100) / 100,
    ...(isHeavyInput && {
      effectiveFloorMb:
        Math.round((effectiveFloorBytes / 1024 / 1024) * 100) / 100,
    }),
    ...(!isHeavyInput && { zone: "A_fewest_files_first" }),
  });

  function estimateEffectiveParts(totalBytes, maxParts) {
    return estimatePartsForTarget({
      bytes: totalBytes,
      targetBytes: partCapBytes,
      maxParts,
      pages: prePages,
    });
  }
  console.log("[POLICY_PART_ESTIMATOR]", {
    jobId,
    isHeavyInput,
    effectiveMaxParts,
  });
  let estimatedParts = estimateEffectiveParts(
    afterCompressBytes,
    effectiveMaxParts,
  );

  // Zone B only: reject when candidate fits cap/parts but is below quality floor.
  // Zone A: never reject on total size; acceptance = part-count fit + per-part limit.
  function shouldRejectOverShrunkCandidate({
    isHeavyInput,
    candidateBytes,
    effectiveCapBytes,
    effectiveMaxParts,
    floorBytes,
  }) {
    if (candidateBytes <= 0) return false;
    if (!isHeavyInput) return false; // Zone A: no floor-based rejection
    if (candidateBytes >= floorBytes) return false;
    const fitsCap = candidateBytes <= effectiveCapBytes;
    const fitsParts =
      estimateEffectiveParts(candidateBytes, effectiveMaxParts) <=
      effectiveMaxParts;
    return fitsCap && fitsParts;
  }

  let bestFitPath = null;
  let bestFitBytes = 0;
  let bestFitParts = Infinity;
  let bestFitStepLabel = null;

  // Zone B only: above-floor preference (never used by Zone A).
  let bestFitFloorPath = null;
  let bestFitFloorBytes = 0;
  let bestFitFloorStepLabel = null;

  function maybeAcceptBestFit(candidatePath, bytes, label) {
    if (bytes <= 0) return false;
    if (bytes > effectiveTotalCap) return false;
    const estParts = estimateEffectiveParts(bytes, effectiveMaxParts);
    if (estParts > effectiveMaxParts) return false;

    if (
      shouldRejectOverShrunkCandidate({
        isHeavyInput,
        candidateBytes: bytes,
        effectiveCapBytes: effectiveTotalCap,
        effectiveMaxParts: effectiveMaxParts,
        floorBytes: effectiveFloorBytes,
      })
    ) {
      console.log("[CANDIDATE_REJECT_OVER_SHRINK]", {
        jobId,
        label,
        candidateMb: Math.round(bytesToMb(bytes) * 100) / 100,
        floorMb: Math.round(bytesToMb(effectiveFloorBytes) * 100) / 100,
        effectiveCapMb: Math.round(bytesToMb(effectiveTotalCap) * 100) / 100,
        isHeavyInput,
      });
      return false;
    }

    if (isHeavyInput) {
      if (bytes > bestFitBytes) {
        bestFitPath = candidatePath;
        bestFitBytes = bytes;
        bestFitStepLabel = label;
      }
      if (
        effectiveFloorBytes > 0 &&
        bytes >= effectiveFloorBytes &&
        bytes > bestFitFloorBytes
      ) {
        bestFitFloorPath = candidatePath;
        bestFitFloorBytes = bytes;
        bestFitFloorStepLabel = label;
      }
      if (bytes === bestFitBytes || bytes === bestFitFloorBytes) {
        console.log("[CANDIDATE]", {
          jobId,
          label,
          bytesMb: Math.round(bytesToMb(bytes) * 100) / 100,
          estParts,
          tier: bytes >= effectiveFloorBytes ? "ABOVE_FLOOR" : "UNDER_FLOOR",
        });
      }
    } else {
      // Zone A: fewest parts first, then highest bytes. No floor-tier logic.
      if (
        estParts < bestFitParts ||
        (estParts === bestFitParts && bytes > bestFitBytes)
      ) {
        bestFitPath = candidatePath;
        bestFitBytes = bytes;
        bestFitParts = estParts;
        bestFitStepLabel = label;
        console.log("[CANDIDATE]", {
          jobId,
          label,
          bytesMb: Math.round(bytesToMb(bytes) * 100) / 100,
          estParts,
        });
      }
    }
    return true;
  }

  maybeAcceptBestFit(workPdf, preBytes, "initial");
  console.log("[DEFAULT_FEASIBILITY]", {
    jobId,
    step: "initial",
    estimatedParts,
    targetPartBytes: partCapBytes,
    maxPartsGoal,
    bytes: afterCompressBytes,
  });

  // Zone A: stepped 1-file attempt — gentle levels 1→2→3, stop on first viable result
  const under200Mb = preInBytes < 200 * 1024 * 1024;
  if (
    under200Mb &&
    !softStop() &&
    afterCompressBytes > partCapBytes &&
    preBytes <= SYSTEM_DEFAULT_MAX_INPUT_BYTES
  ) {
    const gentleSteps = [
      { max_side: 1800, jpeg_q: 50, gentle_level: 1 },
      { max_side: 1600, jpeg_q: 45, gentle_level: 2 },
      { max_side: 1400, jpeg_q: 42, gentle_level: 3 },
    ];
    for (const step of gentleSteps) {
      try {
        const pyExe = pickPythonExe();
        const tmp1 = path.join(
          wd,
          `.__pyre_1FILE_L${step.gentle_level}_${rand6()}.pdf`,
        );
        const pyArgs = [
          path.join(__dirname, "tools", "selective_recompress.py"),
          workPdf,
          tmp1,
          "--target_mb",
          "9",
          "--top_k",
          "300",
          "--min_stream_kb",
          "50",
          "--max_side",
          String(step.max_side),
          "--jpeg_q",
          String(step.jpeg_q),
          "--passes",
          "1",
          "--gentle",
          "--gentle_level",
          String(step.gentle_level),
        ];
        const tPy = boundedTimeout(120_000, expiresAtIso, 20_000, 120_000);
        await runCmd(pyExe, pyArgs, tPy, {}, [0]);
        const after1 = safeStatSize(tmp1) ?? 0;
        // Zone A: accept when fits 1 part; no total-size floor rejection
        if (after1 > 0 && after1 <= partCapBytes) {
          workPdf = tmp1;
          afterCompressBytes = after1;
          estimatedParts = 1;
          anyShrink = true;
          console.log("[ZONE_A_1FILE_SUCCESS]", {
            jobId,
            level: step.gentle_level,
            bytesMb: Math.round(bytesToMb(after1) * 100) / 100,
          });
          break;
        }
        safeUnlink(tmp1);
      } catch (e) {
        console.warn(
          "[ZONE_A_1FILE] level",
          step.gentle_level,
          "failed:",
          e?.message || e,
        );
      }
    }
  }

  // Python-based selective image recompress lane (quality-aware):
  // Zone A: gentle profile. Zone B: MILD then STRONG; prefer above HEAVY_FLOOR_BYTES.
  const needPython =
    (estimatedParts > effectiveMaxParts || preBytes > effectiveTotalCap) &&
    preBytes <= SYSTEM_DEFAULT_MAX_INPUT_BYTES &&
    !softStop();
  if (needPython) {
    try {
      // Zone A: target 9MB (1 file). Zone B: target total cap.
      const targetMb = isHeavyInput
        ? Math.max(1, Math.ceil(effectiveTotalCap / (1024 * 1024)))
        : 9;
      console.log("[PY_TARGET_MB]", { jobId, isHeavyInput, targetMb });
      const pyExe = pickPythonExe();

      const runPyVariant = async (variantLabel, extraArgs) => {
        const tmpRe = path.join(wd, `.__pyre_${variantLabel}_${rand6()}.pdf`);
        const pyArgs = [
          path.join(__dirname, "tools", "selective_recompress.py"),
          workPdf,
          tmpRe,
          "--target_mb",
          String(targetMb),
          ...extraArgs,
        ];

        const tPy = boundedTimeout(180_000, expiresAtIso, 30_000, 180_000);
        const pyRes = await runCmd(pyExe, pyArgs, tPy, {}, [0]);

        let summary = null;
        try {
          summary = JSON.parse(pyRes.out.trim() || "{}");
        } catch {
          summary = null;
        }

        const beforeB = safeStatSize(workPdf) ?? preBytes;
        const afterB = safeStatSize(tmpRe) ?? beforeB;

        // Zone B only: reject over-shrunk unless we still cannot fit.
        const wouldStillNeedFit =
          estimateEffectiveParts(afterB, effectiveMaxParts) >
            effectiveMaxParts || afterB > effectiveTotalCap;
        const overShrunk =
          isHeavyInput && afterB > 0 && afterB < effectiveFloorBytes;

        const acceptable =
          afterB > 0 && afterB < beforeB && (!overShrunk || wouldStillNeedFit);

        console.log("[PY_VARIANT]", {
          jobId,
          variant: variantLabel,
          beforeMb: Math.round(bytesToMb(beforeB) * 100) / 100,
          afterMb: Math.round(bytesToMb(afterB) * 100) / 100,
          images_touched: summary?.images_touched ?? null,
          passes_run: summary?.passes_run ?? null,
          overShrunk,
          acceptable,
        });
        if (variantLabel === "GENTLE" && (summary?.images_touched ?? 0) === 0) {
          console.log("[PY_GENTLE_NOOP]", {
            jobId,
            inputBytes: beforeB,
            note: "min_stream_kb/top_k may still skip images",
          });
        }

        const accepted = maybeAcceptBestFit(
          tmpRe,
          afterB,
          `python_${variantLabel}`,
        );
        if (acceptable && accepted) {
          workPdf = tmpRe;
          afterCompressBytes = afterB;
          estimatedParts = estimateEffectiveParts(
            afterCompressBytes,
            effectiveMaxParts,
          );
          anyShrink = true;
          console.log("[DEFAULT_FEASIBILITY]", {
            jobId,
            step: `python_${variantLabel}`,
            estimatedParts,
            targetPartBytes: partCapBytes,
            maxPartsGoal,
            bytes: afterCompressBytes,
          });
          return true;
        }

        safeUnlink(tmpRe);
        return false;
      };

      if (isHeavyInput) {
        // 1) MILD: keep better readability (prefer)
        await runPyVariant("MILD", [
          "--top_k",
          "160",
          "--min_stream_kb",
          "220",
          "--max_side",
          "1600",
          "--jpeg_q",
          "44",
          "--passes",
          "2",
          "--force",
        ]);

        // 2) STRONG: only if still infeasible
        if (
          estimateEffectiveParts(afterCompressBytes, effectiveMaxParts) >
            effectiveMaxParts ||
          afterCompressBytes > effectiveTotalCap
        ) {
          await runPyVariant("STRONG", [
            "--top_k",
            "220",
            "--min_stream_kb",
            "200",
            "--max_side",
            "1100",
            "--jpeg_q",
            "30",
            "--passes",
            "3",
            "--force",
          ]);
        }
      } else {
        // Zone A: GENTLE lane - quality-first, minimum necessary compression
        await runPyVariant("GENTLE", [
          "--top_k",
          "400",
          "--min_stream_kb",
          "20",
          "--max_side",
          "2000",
          "--jpeg_q",
          "55",
          "--passes",
          "1",
          "--gentle",
        ]);
      }
    } catch (e) {
      console.warn("[PY_RECOMPRESS] failed:", e?.message || e);
    }
  }

  // Achieved stage ladder for DEFAULT (labels/progress; targetA = ideal total bytes)
  const targetA = effectiveMaxParts * SYSTEM_PART_BYTES;
  const targetB = Math.floor(targetA * 1.6);
  const targetC = Math.floor(targetA * 2.5);
  let achievedStage = "D_RESCUE_SPLIT_ONLY";
  const recomputeAchievedStage = () => {
    if (afterCompressBytes <= targetA) {
      achievedStage = "A_TARGET_MET";
    } else if (anyShrink && afterCompressBytes <= targetB) {
      achievedStage = "B_TARGET_RELAXED";
    } else if (anyShrink && afterCompressBytes <= targetC) {
      achievedStage = "C_TARGET_RELAXED";
    } else if (anyShrink) {
      achievedStage = "C_TARGET_RELAXED";
    } else {
      achievedStage = "D_RESCUE_SPLIT_ONLY";
    }
  };
  const aggressiveLane = preInBytes >= 200 * 1024 * 1024; // >=200MB input triggers AGGRESSIVE_DEFAULT_LANE

  // Ensure total size is improved toward the ladder targets before split; AGGRESSIVE_DEFAULT_LANE
  // uses more passes and longer timeout for >=200MB inputs, but we still proceed best-effort.
  const COMPRESS_PASSES = aggressiveLane
    ? [
        {
          label: "High (200/70)",
          dpi: 200,
          jpegQ: 70,
          pdfSettings: "/printer",
        },
        { label: "Med (150/60)", dpi: 150, jpegQ: 60, pdfSettings: "/ebook" },
        { label: "Low (120/50)", dpi: 120, jpegQ: 50, pdfSettings: "/screen" },
        {
          label: "Lower (110/45)",
          dpi: 110,
          jpegQ: 45,
          pdfSettings: "/screen",
        },
        { label: "Floor (96/40)", dpi: 96, jpegQ: 40, pdfSettings: "/screen" },
      ]
    : [
        {
          label: "High (200/70)",
          dpi: 200,
          jpegQ: 70,
          pdfSettings: "/printer",
        },
        { label: "Med (150/60)", dpi: 150, jpegQ: 60, pdfSettings: "/ebook" },
        { label: "Low (120/50)", dpi: 120, jpegQ: 50, pdfSettings: "/screen" },
      ];
  const COMPRESS_PASS_TIMEOUT_MS = aggressiveLane ? 180_000 : 120_000;

  // Run compression ladder only if we don't already fit.
  if (
    (estimatedParts > effectiveMaxParts ||
      afterCompressBytes > effectiveTotalCap) &&
    !softStop()
  ) {
    for (const pass of COMPRESS_PASSES) {
      if (softStop()) break;
      estimatedParts = estimateEffectiveParts(
        afterCompressBytes,
        effectiveMaxParts,
      );
      if (
        estimatedParts <= effectiveMaxParts &&
        afterCompressBytes <= effectiveTotalCap
      ) {
        break;
      }
      console.log("[DEFAULT_FEASIBILITY]", {
        jobId,
        step: `gs_pass_start_${pass.label}`,
        estimatedParts,
        targetPartBytes: partCapBytes,
        maxPartsGoal,
        bytes: afterCompressBytes,
      });
      await updateJob(jobId, {
        stage: "COMPRESS_HARDFIT",
        progress: 22,
        split_progress: 12,
        updated_at: nowIso(),
      });
      const outPath = path.join(
        wd,
        `.__target_fit_${pass.label.replace(/\s+/g, "_")}_${rand6()}.pdf`,
      );
      const ok = await runGsWithHeartbeat({
        jobId,
        label: `compress: ${pass.label}`,
        runFn: () =>
          gsCompressOnce({
            inPdf: workPdf,
            outPdf: outPath,
            mode: "PRESET",
            dpi: pass.dpi,
            jpegQ: pass.jpegQ,
            pdfSettings: pass.pdfSettings,
            expiresAtIso,
            timeoutMs: COMPRESS_PASS_TIMEOUT_MS,
          }),
      });
      if (ok) {
        const outBytes = safeStatSize(outPath) ?? 0;
        if (outBytes > 0) {
          const accepted = maybeAcceptBestFit(
            outPath,
            outBytes,
            `gs_${pass.label}`,
          );
          if (accepted && outBytes < afterCompressBytes) {
            workPdf = outPath;
            afterCompressBytes = outBytes;
            anyShrink = true;
            estimatedParts = estimateEffectiveParts(
              afterCompressBytes,
              effectiveMaxParts,
            );
            console.log("[DEFAULT_FEASIBILITY]", {
              jobId,
              step: `gs_pass_done_${pass.label}`,
              estimatedParts,
              targetPartBytes: partCapBytes,
              maxPartsGoal,
              bytes: afterCompressBytes,
            });
            if (
              estimatedParts <= effectiveMaxParts &&
              afterCompressBytes <= effectiveTotalCap
            )
              break;
          } else if (!accepted) {
            safeUnlink(outPath);
          }
        } else {
          safeUnlink(outPath);
        }
      } else {
        safeUnlink(outPath);
      }
    }

    tmark(T, "compress");
  }

  // Last-resort nuclear pass only if still over cap/parts.
  const needNuclear =
    !softStop() &&
    (estimateEffectiveParts(afterCompressBytes, effectiveMaxParts) >
      effectiveMaxParts ||
      afterCompressBytes > effectiveTotalCap);
  const allowNuclear = isHeavyInput; // only heavy inputs (>=200MB) may run nuclear
  if (needNuclear && allowNuclear) {
    const nuclearOut = path.join(wd, `.__target_fit_NUCLEAR_${rand6()}.pdf`);
    const okNuclear = await runGsWithHeartbeat({
      jobId,
      label: "compress: nuclear",
      runFn: () =>
        gsCompressOnce({
          inPdf: workPdf,
          outPdf: nuclearOut,
          mode: "NUCLEAR",
          expiresAtIso,
          timeoutMs: COMPRESS_PASS_TIMEOUT_MS,
        }),
    });
    if (okNuclear) {
      const outBytes = safeStatSize(nuclearOut) ?? 0;
      if (outBytes > 0) {
        const accepted = maybeAcceptBestFit(nuclearOut, outBytes, "nuclear");
        if (accepted && outBytes < afterCompressBytes) {
          workPdf = nuclearOut;
          afterCompressBytes = outBytes;
          anyShrink = true;
          estimatedParts = estimateEffectiveParts(
            afterCompressBytes,
            effectiveMaxParts,
          );
          console.log("[DEFAULT_FEASIBILITY]", {
            jobId,
            step: "gs_nuclear_done",
            estimatedParts,
            targetPartBytes: partCapBytes,
            maxPartsGoal,
            bytes: afterCompressBytes,
          });
        } else if (!accepted) {
          safeUnlink(nuclearOut);
        }
      } else {
        safeUnlink(nuclearOut);
      }
    } else {
      safeUnlink(nuclearOut);
    }
  } else if (needNuclear && !allowNuclear) {
    console.log("[NUCLEAR_SKIP_UNDER200]", {
      jobId,
      inputBytes: preBytes,
      reason: "quality_guard",
    });
  }

  let appliedBestFit = false;
  const currentFits =
    afterCompressBytes <= effectiveTotalCap &&
    estimateEffectiveParts(afterCompressBytes, effectiveMaxParts) <=
      effectiveMaxParts;
  const useFallback = !currentFits && bestFitPath && bestFitBytes > 0;

  let finalBestPath, finalBestBytes, finalBestLabel, tier, tierReason;
  if (isHeavyInput) {
    const preferFloor = bestFitFloorPath != null;
    finalBestPath = preferFloor
      ? bestFitFloorPath
      : useFallback
        ? bestFitPath
        : workPdf;
    finalBestBytes = preferFloor
      ? bestFitFloorBytes
      : useFallback
        ? bestFitBytes
        : afterCompressBytes;
    finalBestLabel = preferFloor
      ? bestFitFloorStepLabel
      : useFallback
        ? bestFitStepLabel
        : null;
    tier = preferFloor ? "PREFERRED" : useFallback ? "FALLBACK" : "NONE";
    tierReason = preferFloor
      ? "best_quality_candidate"
      : useFallback
        ? "fit_candidate"
        : currentFits
          ? "current_fits"
          : "no_acceptable_candidate";
  } else {
    // Zone A: fewest parts first, no floor-tier preference
    finalBestPath = useFallback ? bestFitPath : workPdf;
    finalBestBytes = useFallback ? bestFitBytes : afterCompressBytes;
    finalBestLabel = useFallback ? bestFitStepLabel : null;
    tier = useFallback ? "FALLBACK" : "NONE";
    tierReason = useFallback
      ? "fit_candidate"
      : currentFits
        ? "current_fits"
        : "no_acceptable_candidate";
  }

  if (
    finalBestPath &&
    (finalBestPath !== workPdf || finalBestBytes !== afterCompressBytes)
  ) {
    workPdf = finalBestPath;
    afterCompressBytes = finalBestBytes;
    appliedBestFit = true;
    console.log("[BEST_FIT_SELECTED]", {
      jobId,
      label: finalBestLabel,
      tier,
      bytesMb: Math.round(bytesToMb(finalBestBytes) * 100) / 100,
      estParts: estimateEffectiveParts(finalBestBytes, effectiveMaxParts),
    });
  }
  recomputeAchievedStage();

  await updateJob(jobId, {
    stage: "SPLIT",
    progress: 55,
    split_progress: 40,
    updated_at: nowIso(),
  });

  // DEFAULT split sizing (best-effort, ≤ effectiveMaxParts parts):
  // Single source for part-size budget; never allow missing/invalid targetBytes.
  const targetBytes =
    Number.isFinite(SYSTEM_PART_BYTES) && SYSTEM_PART_BYTES > 0
      ? SYSTEM_PART_BYTES
      : 9 * 1024 * 1024;
  const maxPartsForSplit = effectiveMaxParts;

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

  // HARD: each part <= targetBytes (≈9MB).
  const splitForSystem = async (pdfPath, opts = {}) => {
    const partBytes = opts.targetBytes ?? targetBytes;
    const effectiveMaxParts = opts.maxParts ?? maxPartsForSplit;
    wipePartsDir();
    const pages0 = await qpdfPages(pdfPath, expiresAtIso, qpdfWarnings);
    const bytes0 = safeStatSize(pdfPath) ?? 0;

    if (bytes0 <= partBytes) {
      const singleName = "goodPDF-1(1).pdf";
      const singlePath = path.join(partsDir, singleName);
      fs.copyFileSync(pdfPath, singlePath);
      const b = safeStatSize(singlePath) ?? 0;
      return {
        partFiles: [singlePath],
        partMeta: [
          {
            name: singleName,
            bytes: b,
            sizeMb: Math.round(bytesToMb(b) * 10) / 10,
            startPageIndex: 1,
            endPageIndex: pages0,
          },
        ],
        _fit: true,
        _parts: 1,
      };
    }

    const maxPartsForFile = Math.min(effectiveMaxParts, pages0);
    let desiredParts = choosePartCountBySize({
      bytes: bytes0,
      pages: pages0,
      targetBytes: partBytes,
      minParts: 1,
      maxParts: maxPartsForFile,
    });
    const desiredPartsBefore = desiredParts;
    const minPartsNeeded = Math.ceil(bytes0 / partBytes);
    const minPartsNeededClamped = Math.min(
      effectiveMaxParts,
      Math.max(2, minPartsNeeded),
    );
    if (opts.under200Mb) {
      desiredParts = Math.min(desiredParts, minPartsNeededClamped);
    }
    console.log("[SPLIT_BUCKET]", {
      jobId,
      bytesMb: Math.round(bytesToMb(bytes0) * 100) / 100,
      targetMb: Math.round((partBytes / (1024 * 1024)) * 100) / 100,
      desiredPartsBefore,
      desiredPartsAfter: desiredParts,
    });
    // Zone A: try 2 -> 3 -> 4 -> 5; accept first where each part <= 9MB (no total-size floor).
    if (opts.under200Mb) {
      const tryParts = [2, 3, 4, 5];
      const maxPartsToTry = Math.min(effectiveMaxParts, pages0);
      let pageBytes = null;
      try {
        const probeDir = path.join(wd, ".__probe");
        pageBytes = await estimatePageBytes(
          { inPdf: pdfPath, pages: pages0, probeDir, expiresAtIso },
          qpdfWarnings,
        );
        if (!Array.isArray(pageBytes) || pageBytes.length !== pages0)
          pageBytes = null;
      } catch (_) {
        pageBytes = null;
      }
      for (const p of tryParts) {
        if (p > maxPartsToTry) continue;
        wipePartsDir();
        const rangesTry =
          pageBytes && pageBytes.length === pages0
            ? buildRangesFromPageBytes({ pageBytes, parts: p })
            : buildRangesByEstimatedSize({
                bytes: bytes0,
                pages: pages0,
                targetBytes: partBytes,
                maxParts: effectiveMaxParts,
                parts: p,
              });
        const resTry = await splitSinglePass(
          { inPdf: pdfPath, partsDir, ranges: rangesTry, expiresAtIso },
          qpdfWarnings,
        );
        const maxPartBytesTry = maxPartBytes(resTry.partMeta);
        const totalBytesTry = (resTry.partMeta || []).reduce(
          (s, m) => s + Number(m?.bytes || 0),
          0,
        );
        const maxPartMb =
          Math.round((maxPartBytesTry / 1024 / 1024) * 100) / 100;
        const totalOutMb =
          Math.round((totalBytesTry / 1024 / 1024) * 100) / 100;
        console.log("[UNDER200_SPLIT_TRY]", {
          jobId,
          parts: p,
          maxPartMb,
          totalOutMb,
        });
        if (maxPartBytesTry <= partBytes) {
          console.log("[UNDER200_SPLIT_SUCCESS]", {
            jobId,
            parts: p,
            totalOutMb,
            maxPartMb,
          });
          return { ...resTry, _fit: true, _parts: p };
        }
      }
    }

    const minParts = Math.min(
      maxPartsForFile,
      Math.max(
        desiredParts,
        estimatePartsForTarget({
          bytes: bytes0,
          targetBytes: partBytes,
          maxParts: maxPartsForFile,
          pages: pages0,
        }),
      ),
    );

    let best = null;
    for (let parts = minParts; parts <= maxPartsForFile; parts++) {
      wipePartsDir();
      const ranges = buildRangesByEstimatedSize({
        bytes: bytes0,
        pages: pages0,
        targetBytes: partBytes,
        maxParts: maxPartsForFile,
        parts,
      });
      const res = await splitSinglePass(
        {
          inPdf: pdfPath,
          partsDir,
          ranges,
          expiresAtIso,
        },
        qpdfWarnings,
      );

      const peak = maxPartBytes(res.partMeta);
      if (!best || peak < best.peak) best = { res, peak, parts };
      if (peak <= partBytes) return { ...res, _fit: true, _parts: parts };
    }

    return {
      ...(best?.res || { partFiles: [], partMeta: [] }),
      _fit: false,
      _parts: best?.parts ?? maxPartsForFile,
    };
  };

  // Strict feasibility check: uses effectiveMaxParts (5).
  estimatedParts = estimateEffectiveParts(
    afterCompressBytes,
    effectiveMaxParts,
  );
  console.log("[DEFAULT_FEASIBILITY]", {
    jobId,
    step: "pre_split",
    estimatedParts,
    targetPartBytes: SYSTEM_PART_BYTES,
    maxPartsGoal: effectiveMaxParts,
    bytes: afterCompressBytes,
  });

  let res;
  let rescueSplitOnly = false;
  let rescueReason = null;

  if (estimatedParts <= effectiveMaxParts) {
    res = await splitForSystem(workPdf, {
      targetBytes: SYSTEM_PART_BYTES,
      maxParts: effectiveMaxParts,
      under200Mb,
    });
  } else {
    rescueSplitOnly = true;
    rescueReason = {
      estimatedParts,
      targetPartBytes: SYSTEM_PART_BYTES,
      maxPartsGoal: effectiveMaxParts,
      finalBytes: afterCompressBytes,
    };
    console.warn("[DEFAULT_FEASIBILITY]", {
      jobId,
      step: "rescue_split_only_triggered",
      ...rescueReason,
    });

    // Rescue: cap at effectiveMaxParts; do not silently produce many parts.
    res = await splitForSystem(workPdf, {
      targetBytes: SYSTEM_PART_BYTES,
      maxParts: Math.min(effectiveMaxParts, prePages),
    });
  }
  tmark(T, "split");

  const partsCount = (res.partMeta && res.partMeta.length) || 0;
  if (achievedStage !== "A_TARGET_MET") {
    const stageMsg = `Best effort delivered (${achievedStage}).`;
    res.warningMessage = res.warningMessage
      ? `${stageMsg}\n${res.warningMessage}`
      : stageMsg;
  }
  if (appliedBestFit) {
    const bestFitMsg = "Best-fit quality selection applied.";
    res.warningMessage = res.warningMessage
      ? `${res.warningMessage}\n${bestFitMsg}`
      : bestFitMsg;
  }
  if (preInBytes >= 200 * 1024 * 1024) {
    const qualityMsg =
      effectiveMaxParts <= 5
        ? "Large PDF: strong compression may be applied. Output up to 5 parts near 9MB each."
        : "Large PDF: strong compression may be applied. Output up to 10 parts near 9MB each.";
    res.warningMessage = res.warningMessage
      ? `${qualityMsg}\n${res.warningMessage}`
      : qualityMsg;
  }
  if (partsCount >= effectiveMaxParts) {
    const partsMsg =
      effectiveMaxParts <= 5
        ? "Reached max of 5 parts. Use Manual for finer splitting or Compress tool."
        : "Reached max of 10 parts. Use Manual for finer splitting or Compress tool.";
    res.warningMessage = res.warningMessage
      ? `${partsMsg}\n${res.warningMessage}`
      : partsMsg;
  }
  if (qpdfWarnings.length) {
    const qpdfMsg = qpdfWarnings.join("\n").slice(0, 2000).trim();
    res.warningMessage = res.warningMessage
      ? `${res.warningMessage}\n${qpdfMsg}`
      : qpdfMsg;
  }

  const zoneA = preInBytes < 200 * 1024 * 1024;

  // Zone-aware part-fit:
  // - Zone A (<200MB): prefer repartitioning (splitOversizeSafe) before gentle rescue.
  // - Zone B (>=200MB): retain existing aggressive hard-fit behavior.
  if (maxPartBytes(res.partMeta) > targetBytes) {
    if (zoneA && !softStop()) {
      console.log("[ZONE_A_REPARTITION]", {
        jobId,
        inputMb: Math.round(bytesToMb(preInBytes) * 100) / 100,
        estimatedParts,
        maxPartsGoal: effectiveMaxParts,
        targetMb: Math.round(bytesToMb(targetBytes) * 100) / 100,
      });
      await updateJob(jobId, {
        stage: "OVERSIZE_SAFE_SPLIT",
        progress: 70,
        split_progress: 55,
        updated_at: nowIso(),
      });
      res = await splitOversizeSafe(
        {
          inPdf: workPdf,
          partsDir,
          targetBytes,
          expiresAtIso,
          maxParts: effectiveMaxParts,
        },
        qpdfWarnings,
      );
    }

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
        maxPasses: heavyLane ? HEAVY_EXTRA_SHRINK_PASSES : 2,
        onProgress: async ({
          pass,
          passCount,
          done,
          total,
          maxPartBytes: mpb,
        }) => {
          const localPct = total > 0 ? done / total : 0;
          const pct = Math.round(68 + ((pass - 1 + localPct) / passCount) * 18);
          await updateJob(jobId, {
            stage: zoneA ? "ZONE_A_PART_RESCUE" : "PART_FIT_9MB",
            progress: Math.min(95, Math.max(68, pct)),
            split_progress: Math.min(95, Math.max(52, pct)),
            max_part_mb: Math.round(bytesToMb(mpb) * 100) / 100,
            updated_at: nowIso(),
          });
        },
        zoneA,
      });
    }

    if (!zoneA && maxPartBytes(res.partMeta) > targetBytes && !softStop()) {
      console.log("[ZONE_B_HARDFIT]", {
        jobId,
        inputMb: Math.round(bytesToMb(preInBytes) * 100) / 100,
        targetMb: Math.round(bytesToMb(targetBytes) * 100) / 100,
        maxPartsGoal: effectiveMaxParts,
      });
      await updateJob(jobId, {
        stage: "OVERSIZE_SAFE_SPLIT",
        progress: 86,
        split_progress: 78,
        updated_at: nowIso(),
      });
      res = await splitOversizeSafe(
        {
          inPdf: workPdf,
          partsDir,
          targetBytes,
          expiresAtIso,
          maxParts: effectiveMaxParts,
        },
        qpdfWarnings,
      );
      if (maxPartBytes(res.partMeta) > targetBytes) {
        res = await shrinkOversizePartsAggressive({
          res,
          targetBytes,
          expiresAtIso,
          maxPasses: heavyLane ? HEAVY_EXTRA_SHRINK_PASSES : 2,
        });
      }
    }

    if (maxPartBytes(res.partMeta) > targetBytes) {
      console.warn(
        "[system-fit] hard-fit warning: some parts still exceed 9MB after final fallback",
      );
    }
  }

  tmark(T, "end");
  const qpdfMs = td(T, "start", "qpdf");
  const compressMs = T.compress != null ? td(T, "qpdf", "compress") : 0;
  const splitStart = T.compress != null ? "compress" : "qpdf";
  const splitMs = td(T, splitStart, "split");
  const totalMs = td(T, "start", "end");
  const partsN = (res.partMeta && res.partMeta.length) || 0;
  const bytesOut = (res.partMeta || []).reduce(
    (s, m) => s + (Number(m?.bytes) || 0),
    0,
  );

  // Zone A total-output sanity guard: avoid absurdly tiny results for <200MB inputs.
  if (zoneA && bytesOut > 0) {
    const requiredParts = Math.max(
      1,
      Math.min(partsN || estimatedParts || 1, 5),
    );
    const zoneAMinTotalBytes = Math.max(
      Math.floor(preInBytes * 0.35),
      requiredParts * 5 * 1024 * 1024,
    );
    if (
      bytesOut < zoneAMinTotalBytes &&
      bestFitPath &&
      bestFitBytes > bytesOut
    ) {
      console.log("[ZONE_A_REJECT_OVER_SHRINK]", {
        jobId,
        inputMb: Math.round(bytesToMb(preInBytes) * 100) / 100,
        bytesOutMb: Math.round(bytesToMb(bytesOut) * 100) / 100,
        minTotalMb: Math.round(bytesToMb(zoneAMinTotalBytes) * 100) / 100,
        bestFitMb: Math.round(bytesToMb(bestFitBytes) * 100) / 100,
        bestFitStep: bestFitStepLabel,
        parts: partsN,
      });
      try {
        const resBetter = await splitForSystem(bestFitPath, {
          targetBytes: SYSTEM_PART_BYTES,
          maxParts: effectiveMaxParts,
          under200Mb,
        });
        const betterBytesOut = (resBetter.partMeta || []).reduce(
          (s, m) => s + (Number(m?.bytes) || 0),
          0,
        );
        const betterMaxPart = maxPartBytes(resBetter.partMeta);
        if (
          betterBytesOut > bytesOut &&
          betterBytesOut >= zoneAMinTotalBytes &&
          betterMaxPart <= targetBytes
        ) {
          console.log("[ZONE_A_ACCEPT_FINAL]", {
            jobId,
            inputMb: Math.round(bytesToMb(preInBytes) * 100) / 100,
            bytesOutMb: Math.round(bytesToMb(betterBytesOut) * 100) / 100,
            maxPartMb: Math.round(bytesToMb(betterMaxPart) * 100) / 100,
            parts: (resBetter.partMeta && resBetter.partMeta.length) || 0,
          });
          res = resBetter;
        }
      } catch (e) {
        console.warn("[ZONE_A_ACCEPT_FINAL] re-split from bestFit failed:", e);
      }
    } else {
      console.log("[ZONE_A_GUARD]", {
        jobId,
        inputMb: Math.round(bytesToMb(preInBytes) * 100) / 100,
        bytesOutMb: Math.round(bytesToMb(bytesOut) * 100) / 100,
        parts: partsN,
      });
    }
  }
  console.log("[DEFAULT_STAGE]", {
    jobId,
    zone: zoneA ? "A" : "B",
    afterCompressBytes,
    achievedStage,
    aggressiveLane: zoneA ? "fewest-files-first" : aggressiveLane,
    inputBytes: preInBytes,
    parts: partsN,
  });

  console.log("[DEFAULT_POLICY]", {
    jobId,
    zone: zoneA ? "A" : "B",
    inputBytes: preInBytes,
    afterCompressBytes,
    parts: partsN,
    maxPartsDefault: SYSTEM_MAX_PARTS_DEFAULT,
    ...(!zoneA && { targetTotalCap: SYSTEM_TOTAL_CAP_BYTES }),
  });

  console.log(
    `[timing] qpdf=${qpdfMs}ms compress=${compressMs}ms split=${splitMs}ms total=${totalMs}ms parts=${partsN} bytes_in=${preInBytes} bytes_out=${bytesOut}`,
  );

  if (rescueSplitOnly) {
    res.rescueSplitOnly = true;
    res.rescueReason = rescueReason;
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

function ingestCachePath(jobId) {
  return path.join(
    os.tmpdir(),
    "goodpdf_ingest",
    String(jobId || ""),
    "input.pdf",
  );
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
  const queuedStage = String(job?.stage || "QUEUE").toUpperCase();
  const heavyHint =
    String(job?.error_code || "").toUpperCase() === "HEAVY_HINT";
  const claimStage = queuedStage === "QUEUE_HEAVY" ? "QUEUE_HEAVY" : "QUEUE";
  const claimed = await claimJob(jobId, claimStage);
  timing.claim_ms = Date.now() - tClaim;
  if (!claimed) return;

  const startedAtMs = Date.now();
  const wd = tmpDir(jobId);
  let hbTimer = null;

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
    hbTimer = setInterval(() => {
      heartbeatJob(jobId, "PROCESSING").catch(() => {});
    }, HEARTBEAT_MS);

    await updateJob(jobId, {
      stage: "DOWNLOAD",
      progress: 5,
      split_progress: 0,
      expires_at: expiresAtIso,
      delete_at: expiresAtIso,
      updated_at: nowIso(),
    });
    const tDownload = Date.now();
    const cachedIn = ingestCachePath(jobId);
    if (ENABLE_LOCAL_INGEST_FAST_PATH && safeExists(cachedIn)) {
      try {
        fs.copyFileSync(cachedIn, inPdf);
      } catch {
        await downloadFromR2(inputKey, inPdf);
      }
    } else {
      await downloadFromR2(inputKey, inPdf);
    }
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

    const systemFit = true;
    // Hard per-part cap for DEFAULT mode (systemFit); defaults to 9MB.
    let targetBytes = SYSTEM_PART_BYTES;

    let res;
    const tProcess = Date.now();
    await updateJob(jobId, {
      stage: "DEFAULT",
      progress: 6,
      split_progress: 1,
      updated_at: nowIso(),
    });

    if (USE_PDF_WORKER) {
      const partTargetBytes =
        Number.isFinite(SYSTEM_PART_BYTES) && SYSTEM_PART_BYTES > 0
          ? SYSTEM_PART_BYTES
          : 9 * 1024 * 1024;
      targetBytes = partTargetBytes;
      try {
        const pdfWorkerUrl = new URL("../worker/dist/index.js", import.meta.url)
          .href;
        const { processPdf } = await import(pdfWorkerUrl);
        const pdfRes = await processPdf(inPdf, partsDir, {
          timeoutMs: Math.max(
            60_000,
            boundedTimeout(TIMEOUT_GS_MS, expiresAtIso, 20_000, 90_000) * 2,
          ),
          targetBytes: partTargetBytes,
        });
        const policyMsg =
          pdfRes.policyMaxParts <= 5
            ? "Designed to fit within 5 parts."
            : "Designed to fit within 10 parts.";
        const strategyNote =
          pdfRes.strategyUsed && pdfRes.strategyUsed !== "balanced"
            ? ` Strategy: ${pdfRes.strategyUsed}.`
            : "";
        res = {
          partFiles: pdfRes.parts,
          partMeta: pdfRes.parts.map((p) => ({
            name: path.basename(p),
            bytes: safeStatSize(p) ?? 0,
            sizeMb: Math.round(bytesToMb(safeStatSize(p) ?? 0) * 10) / 10,
          })),
          warningMessage:
            pdfRes.fitStatus === "best_effort"
              ? `${policyMsg} Best-effort delivery (some parts may exceed target).${strategyNote}`
              : pdfRes.usedFallback
                ? `${policyMsg} Raster fallback was used to meet policy limits.${strategyNote}`
                : strategyNote
                  ? `${policyMsg}${strategyNote}`
                  : null,
        };
      } catch (pdfWorkerErr) {
        const stage = "processPdf";
        const errMsg = String(pdfWorkerErr?.message || pdfWorkerErr);
        console.warn(
          `[PDF_WORKER] ${stage} failed, falling back to processDefaultFast:`,
          errMsg.slice(0, 500),
        );
        try {
          const heavyLane =
            heavyHint || queuedStage === "QUEUE_HEAVY" || IS_HEAVY_PROFILE;
          res = await processDefaultFast({
            jobId,
            inPdf,
            wd,
            expiresAtIso,
            startedAtMs,
            heavyLane,
          });
        } catch (fallbackErr) {
          console.error(
            "[PDF_WORKER] processDefaultFast fallback also failed:",
            String(fallbackErr?.message || fallbackErr).slice(0, 500),
          );
          throw fallbackErr;
        }
      }
    } else {
      const heavyLane =
        heavyHint || queuedStage === "QUEUE_HEAVY" || IS_HEAVY_PROFILE;
      res = await processDefaultFast({
        jobId,
        inPdf,
        wd,
        expiresAtIso,
        startedAtMs,
        heavyLane,
      });
    }
    timing.process_ms = Date.now() - tProcess;

    // ---------- FINAL ACCEPTANCE GATE (per-part hard cap) ----------
    let partBytes = (res.partMeta || [])
      .map((p) => p.bytes)
      .filter((x) => typeof x === "number" && Number.isFinite(x));
    let totalPartsBytes = partBytes.length
      ? partBytes.reduce((a, b) => a + b, 0)
      : null;
    let maxPartB = partBytes.length ? Math.max(...partBytes) : null;
    const zone = inBytes < 200 * 1024 * 1024 ? "A" : "B";
    const targetMb = Math.round(bytesToMb(targetBytes) * 100) / 100;
    let maxPartMb =
      typeof maxPartB === "number"
        ? Math.round(bytesToMb(maxPartB) * 100) / 100
        : null;
    let totalOutMb =
      typeof totalPartsBytes === "number"
        ? Math.round(bytesToMb(totalPartsBytes) * 100) / 100
        : null;

    let hardCapMet = !systemFit || maxPartB == null || maxPartB <= targetBytes;
    let dynamicExpansionApplied = false;

    console.log("[FINAL_ACCEPTANCE_CHECK]", {
      jobId,
      zone,
      systemFit,
      parts: res.partFiles?.length || 0,
      totalOutMb,
      maxPartMb,
      targetMb,
      hardCapMet,
      maxPartsAllowed: zone === "A" ? SYSTEM_MAX_PARTS_DEFAULT : null,
    });

    // Dynamic part scaling: Zone B only. When default max parts (10) cannot
    // satisfy the per-part limit, allow a one-time expansion up to the hard cap
    // (SYSTEM_MAX_PARTS_HARD_CAP = 25). Zone A stays fewest-files-first (1–5 parts)
    // with no expansion; if cap cannot be met, job fails cleanly.
    if (!hardCapMet && systemFit && zone === "B") {
      const defaultMaxParts =
        inBytes < 200 * 1024 * 1024 ? SYSTEM_MAX_PARTS_DEFAULT : 10;
      const requiredPartsRaw =
        targetBytes > 0 ? Math.ceil(inBytes / targetBytes) : defaultMaxParts;
      const requiredParts = Math.max(
        1,
        Math.min(
          SYSTEM_MAX_PARTS_HARD_CAP,
          Number.isFinite(requiredPartsRaw)
            ? requiredPartsRaw
            : defaultMaxParts,
        ),
      );

      if (requiredParts > defaultMaxParts) {
        console.log("[DYNAMIC_PART_EXPANSION]", {
          jobId,
          inputMb: Math.round(bytesToMb(inBytes) * 100) / 100,
          targetMb,
          defaultMaxParts,
          requiredParts,
          hardCapMax: SYSTEM_MAX_PARTS_HARD_CAP,
        });
        try {
          // Re-split from original input PDF using a higher part cap.
          safeRm(partsDir);
          fs.mkdirSync(partsDir, { recursive: true });
          const dynWarnings = [];
          const dynRes = await splitOversizeSafe(
            {
              inPdf,
              partsDir,
              targetBytes,
              expiresAtIso,
              maxParts: requiredParts,
            },
            dynWarnings,
          );
          res = dynRes;
          partBytes = (res.partMeta || [])
            .map((p) => p.bytes)
            .filter((x) => typeof x === "number" && Number.isFinite(x));
          totalPartsBytes = partBytes.length
            ? partBytes.reduce((a, b) => a + b, 0)
            : null;
          maxPartB = partBytes.length ? Math.max(...partBytes) : null;
          maxPartMb =
            typeof maxPartB === "number"
              ? Math.round(bytesToMb(maxPartB) * 100) / 100
              : null;
          totalOutMb =
            typeof totalPartsBytes === "number"
              ? Math.round(bytesToMb(totalPartsBytes) * 100) / 100
              : null;
          hardCapMet =
            !systemFit || maxPartB == null || maxPartB <= targetBytes;
          if (hardCapMet) dynamicExpansionApplied = true;
        } catch (e) {
          console.warn(
            "[DYNAMIC_PART_EXPANSION] failed, keeping original result:",
            String(e?.message || e).slice(0, 300),
          );
        }
      }
    }

    // Zone A only: when fewest-files-first (1–5 parts) fails, run quality-preserving
    // compression ladder toward ~43–45MB (stop at <=45MB), then exact 5-part split.
    if (!hardCapMet && systemFit && zone === "A") {
      try {
        const compressedPdf = await runZoneACompressionLadder(
          inPdf,
          wd,
          expiresAtIso,
          jobId,
        );
        if (compressedPdf) {
          safeRm(partsDir);
          fs.mkdirSync(partsDir, { recursive: true });
          const zoneAWarnings = [];
          const pages = await qpdfPages(
            compressedPdf,
            expiresAtIso,
            zoneAWarnings,
          );
          const compressedBytes = safeStatSize(compressedPdf) ?? 0;
          const ranges = buildRangesByEstimatedSize({
            bytes: compressedBytes,
            pages,
            targetBytes,
            maxParts: 5,
            parts: 5,
          });
          const zoneARes = await splitSinglePass(
            {
              inPdf: compressedPdf,
              partsDir,
              ranges,
              expiresAtIso,
            },
            zoneAWarnings,
          );
          safeUnlink(compressedPdf);
          res = zoneARes;
          partBytes = (res.partMeta || [])
            .map((p) => p.bytes)
            .filter((x) => typeof x === "number" && Number.isFinite(x));
          totalPartsBytes = partBytes.length
            ? partBytes.reduce((a, b) => a + b, 0)
            : null;
          maxPartB = partBytes.length ? Math.max(...partBytes) : null;
          maxPartMb =
            typeof maxPartB === "number"
              ? Math.round(bytesToMb(maxPartB) * 100) / 100
              : null;
          totalOutMb =
            typeof totalPartsBytes === "number"
              ? Math.round(bytesToMb(totalPartsBytes) * 100) / 100
              : null;
          hardCapMet =
            !systemFit || maxPartB == null || maxPartB <= targetBytes;
          console.log("[ZONE_A_AFTER_COMPRESS_SPLIT]", {
            jobId,
            totalOutMb,
            maxPartMb,
            hardCapMet,
          });
        }
      } catch (e) {
        const fullMsg = String(e?.message ?? e);
        console.warn(
          "[ZONE_A_COMPRESS_LADDER] failed (full stderr/out):",
          fullMsg,
        );
      }
    }

    const finalPartsCount = res.partFiles?.length || 0;
    const finalZoneAMaxPartsMet =
      zone !== "A" || finalPartsCount <= SYSTEM_MAX_PARTS_DEFAULT;

    if (!hardCapMet || !finalZoneAMaxPartsMet) {
      const errorText =
        `Could not fit all parts within the ${SYSTEM_PART_MB}MB per-part limit. ` +
        `Max part was ${maxPartMb ?? "unknown"}MB across ${finalPartsCount} parts. ` +
        "Use Manual mode or stronger compression if you need fewer or smaller parts.";

      console.log("[FINAL_ACCEPT_REJECT]", {
        jobId,
        zone,
        parts: finalPartsCount,
        totalOutMb,
        maxPartMb,
        targetMb,
        finalStatus: "FAILED",
        maxPartsAllowed: zone === "A" ? SYSTEM_MAX_PARTS_DEFAULT : null,
        zoneAMaxPartsMet: finalZoneAMaxPartsMet,
        rejectReason: !hardCapMet ? "part_over_target_mb" : "too_many_parts",
      });
      console.log("[SPLIT_TARGET_NOT_MET]", {
        jobId,
        zone,
        parts: finalPartsCount,
        totalOutMb,
        maxPartMb,
        targetMb,
      });

      // For FAILED final state, strip success-ish lines from warning_text so
      // messaging stays truthful (no "Best effort delivered" or "Reached max..." etc.).
      let failureWarning = res.warningMessage || null;
      if (failureWarning) {
        const bannedPhrases = [
          "Best effort delivered",
          "Reached max of 5 parts",
          "Reached max of 10 parts",
          "Dynamic expansion applied to preserve the 9MB per-part limit",
        ];
        const cleanedLines = String(failureWarning)
          .split("\n")
          .filter((ln) => !bannedPhrases.some((phrase) => ln.includes(phrase)));
        failureWarning = cleanedLines.length ? cleanedLines.join("\n") : null;
      }

      await updateJob(jobId, {
        status: "FAILED",
        stage: "FAILED",
        progress: 100,
        split_progress: 100,
        output_zip_path: null,
        zip_path: null,
        ttl_minutes: ttlMinutes,
        expires_at: expiresAtIso,
        delete_at: expiresAtIso,
        parts_count: finalPartsCount || null,
        parts_json: res.partMeta || null,
        total_parts_bytes: totalPartsBytes,
        output_zip_bytes: null,
        target_mb: SYSTEM_PART_MB,
        max_part_mb: maxPartMb,
        warning_text: failureWarning,
        error_text: errorText,
        error_code: "SPLIT_TARGET_NOT_MET",
        claimed_by: null,
        claimed_at: null,
        updated_at: nowIso(),
      });

      // Do NOT zip/upload on hard-cap failure; end cleanly.
      timing.finalize_ms = Date.now() - tProcess;
      timing.total_ms = Date.now() - jobStartMs;
      return;
    }

    console.log("[FINAL_ACCEPT_SUCCESS]", {
      jobId,
      zone,
      parts: res.partFiles?.length || 0,
      totalOutMb,
      maxPartMb,
      targetMb,
      finalStatus: "DONE",
      maxPartsAllowed: zone === "A" ? SYSTEM_MAX_PARTS_DEFAULT : null,
      zoneAMaxPartsMet: finalZoneAMaxPartsMet,
    });

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
    const warningText =
      systemFit && res.warningMessage ? res.warningMessage : null;

    const tFinalize = Date.now();
    const rescueInfo =
      res && res.rescueSplitOnly ? res.rescueReason || null : null;
    const policyMaxPartsFromInput = inBytes < 200 * 1024 * 1024 ? 5 : 10;
    const maxPartsGoal = rescueInfo?.maxPartsGoal ?? policyMaxPartsFromInput;
    const rescueMsg =
      rescueInfo && systemFit && !dynamicExpansionApplied
        ? `Could not reach <=${maxPartsGoal} parts at ${SYSTEM_PART_MB}MB target.\n` +
          `Estimated parts needed: ${rescueInfo.estimatedParts ?? "unknown"}.\n` +
          `Use Manual mode or stronger compression if you need fewer parts.`
        : null;
    const expansionNote = dynamicExpansionApplied
      ? "Dynamic expansion applied to preserve the 9MB per-part limit."
      : null;
    const finalWarningText = expansionNote
      ? expansionNote
      : rescueMsg && warningText
        ? `${rescueMsg}\n${warningText}`
        : rescueMsg || warningText;

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
      warning_text: finalWarningText,
      error_text: null,
      error_code: null,
      claimed_by: null,
      claimed_at: null,
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
    const code = e?.code ? String(e.code) : "WORKER_ERROR";
    timing.total_ms = Date.now() - jobStartMs;
    console.error("[JOB_TIMING_FAILED]", {
      jobId,
      ...timing,
      error: msg.slice(0, 200),
    });

    const corruptCodes = [
      "INPUT_EMPTY",
      "INVALID_PDF_SIGNATURE",
      "NPAGES_FAILED",
    ];
    const isRefusedCorrupt = corruptCodes.includes(code);
    const isRefusedImageHeavy = code === "REFUSED_IMAGE_HEAVY";
    const refusalCode = isRefusedCorrupt
      ? "REFUSED_CORRUPT_PDF"
      : isRefusedImageHeavy
        ? "REFUSED_IMAGE_HEAVY"
        : code;
    const refusalText = isRefusedCorrupt
      ? "PDF is corrupt or invalid."
      : isRefusedImageHeavy
        ? "PDF is too image-heavy to process."
        : msg.slice(0, 800);

    await updateJob(jobId, {
      status: "FAILED",
      stage: "FAILED",
      progress: 100,
      split_progress: 100,
      error_code: refusalCode,
      error_text: refusalText,
      claimed_by: null,
      claimed_at: null,
      updated_at: nowIso(),
    });
  } finally {
    if (hbTimer) {
      try {
        clearInterval(hbTimer);
      } catch {}
      hbTimer = null;
    }
    try {
      if (ENABLE_LOCAL_INGEST_FAST_PATH) {
        safeRm(path.dirname(ingestCachePath(jobId)));
      }
    } catch {}
    safeRm(wd);
  }
}

// ----------------------
// Main loop
// ----------------------
async function main() {
  console.log("goodpdf worker started", {
    WORKER_ID,
    WORKER_PROFILE,
    QUEUE_STAGE,
    ACCEPTED_QUEUE_STAGES,
    NORMAL_CAN_PROCESS_HEAVY,
    POLL_MS,
    CONCURRENCY,
    QUEUE_FETCH_LIMIT,
    QUEUE_NEWEST_FIRST,
    SPLIT_PAR,
    R2_BUCKET_IN,
    R2_BUCKET_OUT,
    DEFAULT_TTL_MINUTES,
    DO_CLEANUP,
    CLEANUP_EVERY_MS,
    STALE_RECOVERY_EVERY_MS,
    CLAIM_STALE_MS,
    HEARTBEAT_MS,
    MAX_STALE_RECOVERY_BATCH,
    ENABLE_LOCAL_INGEST_FAST_PATH,
    UX_CAP_MS,
    UX_SOFTSTOP_MS,
    SYSTEM_PART_MB,
    SYSTEM_MAX_PARTS,
    SYSTEM_TURBO_TRIGGER_MB,
    SYSTEM_TURBO_PAGE_TRIGGER,
    SYSTEM_FORCE_5_FROM_MB,
    HEAVY_TARGET_MARGIN,
    HEAVY_EXTRA_SHRINK_PASSES,
  });

  // Print critical env sanity (never print secrets)
  console.log("[ENV] SUPABASE_URL =", SUPABASE_URL);
  console.log(
    "[ENV] SUPABASE_SERVICE_ROLE_KEY present =",
    !!SUPABASE_SERVICE_ROLE_KEY,
  );
  console.log("[ENV] R2_ENDPOINT present =", !!R2_ENDPOINT);

  let lastCleanupAt = 0;
  let lastStaleRecoveryAt = 0;
  let pollTick = 0;
  let lastIdlePollLogAt = 0;
  let lastErrorLogAt = 0;
  let lastLoggedIdleSleepMs = 0;
  let idleSleepMs = POLL_MS;
  let idleStartedAtMs = null;

  function getIdleSinceMs() {
    if (idleStartedAtMs === null) return 0;
    return Date.now() - idleStartedAtMs;
  }

  while (true) {
    try {
      const now = Date.now();
      if (DO_CLEANUP && now - lastCleanupAt >= CLEANUP_EVERY_MS) {
        lastCleanupAt = now;
        cleanupExpiredOutputs().catch((err) => {
          console.error("[cleanup] failed:", err?.message || err);
        });
      }
      if (now - lastStaleRecoveryAt >= STALE_RECOVERY_EVERY_MS) {
        lastStaleRecoveryAt = now;
        requeueStaleProcessingJobs()
          .then((n) => {
            if (n > 0) console.log("[stale-recovery] requeued =", n);
          })
          .catch((err) => {
            console.error("[stale-recovery] failed:", err?.message || err);
          });
      }

      const jobs = await fetchQueue(QUEUE_FETCH_LIMIT);
      pollTick++;

      if (!jobs.length) {
        const nowMs = Date.now();
        if (idleStartedAtMs === null) idleStartedAtMs = nowMs;
        const idleSinceMs = getIdleSinceMs();
        if (WORKER_IDLE_EXIT_MS > 0 && idleSinceMs >= WORKER_IDLE_EXIT_MS) {
          console.log(
            "[POLL] idle exit: no jobs for",
            WORKER_IDLE_EXIT_MS,
            "ms",
          );
          process.exit(0);
        }
        const throttleOk = nowMs - lastIdlePollLogAt >= POLL_IDLE_LOG_EVERY_MS;
        const backoffChanged = idleSleepMs !== lastLoggedIdleSleepMs;
        if (throttleOk || backoffChanged) {
          lastIdlePollLogAt = nowMs;
          lastLoggedIdleSleepMs = idleSleepMs;
          console.log(
            "[POLL] idle tick =",
            pollTick,
            "jobs = 0",
            "next poll in",
            idleSleepMs,
            "ms",
          );
        }
        const jitter = 0.9 + Math.random() * 0.2;
        const sleepMs = Math.round(idleSleepMs * jitter);
        await sleep(sleepMs);
        idleSleepMs = Math.min(idleSleepMs * 2, MAX_IDLE_BACKOFF_MS);
        continue;
      }

      idleStartedAtMs = null;
      idleSleepMs = POLL_MS;
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
    } catch (err) {
      console.error("[POLL] loop error:", err?.stack || err?.message || err);
      await new Promise((r) => setTimeout(r, 5000));
      continue;
    }
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
