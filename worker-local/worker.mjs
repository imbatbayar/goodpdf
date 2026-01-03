// worker-local/worker.mjs  (GOODPDF v2 — SPLIT ONLY, NO schema read, safe update)
import "dotenv/config";
import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { spawn } from "node:child_process";
import { createClient } from "@supabase/supabase-js";
import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  DeleteObjectsCommand,
  HeadObjectCommand,
} from "@aws-sdk/client-s3";

// ----------------------
// ENV
// ----------------------
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

// R2 (S3-compatible)
const R2_ENDPOINT = process.env.R2_ENDPOINT;
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID;
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY;
const R2_BUCKET_IN = process.env.R2_BUCKET_IN || "goodpdf-in";
const R2_BUCKET_OUT = process.env.R2_BUCKET_OUT || "goodpdf-out";

// tools
const QPDF_EXE = process.env.QPDF_EXE || "qpdf";
const SEVEN_Z_EXE = process.env.SEVEN_Z_EXE || "7z";

// worker runtime
const POLL_MS = Number(process.env.POLL_MS || 2000);
const CONCURRENCY = Math.max(1, Number(process.env.CONCURRENCY || 1));

// privacy TTL (DONE → expires_at)
const OUTPUT_TTL_MINUTES = Number(process.env.OUTPUT_TTL_MINUTES || 10);

// cleanup loop
const CLEANUP_EVERY_MS = Number(process.env.CLEANUP_EVERY_MS || 30000);
const DO_CLEANUP = String(process.env.DO_CLEANUP || "1") === "1";

// hard timeouts
const TIMEOUT_QPDF_MS = Number(process.env.TIMEOUT_QPDF_MS || 180_000);
const TIMEOUT_7Z_MS = Number(process.env.TIMEOUT_7Z_MS || 180_000);

// Worker identity
const WORKER_ID = `${os.hostname()}_${process.pid}`;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error(
    "Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY in worker-local/.env"
  );
  process.exit(1);
}
if (!R2_ENDPOINT || !R2_ACCESS_KEY_ID || !R2_SECRET_ACCESS_KEY) {
  console.error(
    "Missing R2_ENDPOINT / R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY in worker-local/.env"
  );
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

const r2 = new S3Client({
  region: "auto",
  endpoint: R2_ENDPOINT,
  credentials: {
    accessKeyId: R2_ACCESS_KEY_ID,
    secretAccessKey: R2_SECRET_ACCESS_KEY,
  },
  forcePathStyle: true,
});

// ----------------------
// Helpers
// ----------------------
function killProcessTree(pid) {
  if (!pid) return;
  if (process.platform === "win32") {
    try {
      spawn("taskkill", ["/PID", String(pid), "/T", "/F"], { stdio: "ignore" });
    } catch {}
    return;
  }
  try {
    process.kill(-pid, "SIGKILL");
  } catch {
    try {
      process.kill(pid, "SIGKILL");
    } catch {}
  }
}

function run(cmd, args, opts = {}) {
  const timeoutMs = Number(opts.timeoutMs || 0);

  return new Promise((resolve, reject) => {
    const p = spawn(cmd, args, {
      stdio: "pipe",
      windowsHide: true,
      detached: process.platform !== "win32",
      ...opts,
    });

    let out = "";
    let err = "";

    let timer = null;
    if (timeoutMs > 0) {
      timer = setTimeout(() => {
        killProcessTree(p.pid);
        reject(
          new Error(
            `Command timeout after ${timeoutMs}ms: ${cmd} ${args.join(" ")}`
          )
        );
      }, timeoutMs);
    }

    p.stdout.on("data", (d) => (out += d.toString()));
    p.stderr.on("data", (d) => (err += d.toString()));

    p.on("error", (e) => {
      if (timer) clearTimeout(timer);
      reject(e);
    });

    p.on("close", (code) => {
      if (timer) clearTimeout(timer);
      if (code === 0) return resolve({ out, err });
      reject(
        new Error(`Command failed: ${cmd} ${args.join(" ")}\n${err || out}`)
      );
    });
  });
}

function tmpDir(jobId) {
  const d = path.join(os.tmpdir(), "goodpdf", jobId);
  fs.mkdirSync(d, { recursive: true });
  return d;
}

function nowIso() {
  return new Date().toISOString();
}

function addMinutesIso(m) {
  return new Date(Date.now() + m * 60 * 1000).toISOString();
}

function clampPct(n, fallback = 0) {
  const x = Number(n);
  if (!Number.isFinite(x)) return fallback;
  return Math.max(0, Math.min(100, Math.round(x)));
}

function bytesToMb1(bytes) {
  const mb = Number(bytes) / (1024 * 1024);
  return Math.round(mb * 10) / 10;
}

function sanitizeBaseName(name) {
  const base = String(name || "file")
    .replace(/\.[^/.]+$/, "")
    .trim()
    .slice(0, 80);
  const clean = base
    .replace(/[<>:"/\\|?*\x00-\x1F]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  return clean || "file";
}

// ----------------------
// ✅ Safe update: багана байхгүй бол автоматаар хасаад үргэлжлүүлнэ
// ----------------------
function pickMissingColumnName(msg) {
  // Postgres: column "xxx" of relation "jobs" does not exist
  const m = String(msg || "").match(
    /column \"([a-zA-Z0-9_]+)\" .* does not exist/i
  );
  return m?.[1] || null;
}

async function updateJobSafe(jobId, patch) {
  // patch empty бол update хийхгүй
  if (!patch || Object.keys(patch).length === 0) return;

  let p = { ...patch };
  for (let i = 0; i < 6; i++) {
    const { error } = await supabase.from("jobs").update(p).eq("id", jobId);
    if (!error) return;

    const col = pickMissingColumnName(error.message);
    if (col && Object.prototype.hasOwnProperty.call(p, col)) {
      // тухайн багана DB дээр байхгүй тул patch-ээс хасаад дахин оролдоно
      delete p[col];
      continue;
    }
    throw error;
  }
}

async function updateStage(jobId, patch) {
  const p = { ...(patch || {}) };
  if (p.progress != null) p.progress = clampPct(p.progress);
  if (p.split_progress != null) p.split_progress = clampPct(p.split_progress);
  if (p.compress_progress != null)
    p.compress_progress = clampPct(p.compress_progress);
  await updateJobSafe(jobId, p);
}

// ----------------------
// R2 helpers
// ----------------------
async function streamToFile(readable, outPath) {
  await fsp.mkdir(path.dirname(outPath), { recursive: true });
  const w = fs.createWriteStream(outPath);
  await new Promise((resolve, reject) => {
    readable.pipe(w);
    readable.on("error", reject);
    w.on("error", reject);
    w.on("finish", resolve);
  });
}

async function r2DownloadToFile(bucket, key, localPath) {
  const res = await r2.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
  if (!res?.Body) throw new Error(`R2 GetObject empty body: ${bucket}/${key}`);
  await streamToFile(res.Body, localPath);
}

async function r2UploadFile(
  bucket,
  key,
  localPath,
  contentType = "application/octet-stream"
) {
  const body = fs.createReadStream(localPath);
  await r2.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: body,
      ContentType: contentType,
    })
  );
}

async function r2Exists(bucket, key) {
  try {
    await r2.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
    return true;
  } catch {
    return false;
  }
}

async function r2DeleteMany(bucket, keys) {
  const unique = [...new Set((keys || []).filter(Boolean))];
  if (unique.length === 0) return;
  await r2.send(
    new DeleteObjectsCommand({
      Bucket: bucket,
      Delete: { Objects: unique.map((Key) => ({ Key })), Quiet: true },
    })
  );
}

// ----------------------
// PDF helpers (split-only)
// ----------------------
async function getFileSizeBytes(p) {
  const st = await fsp.stat(p);
  return st.size;
}

async function getTotalPages(pdfPath) {
  const { out } = await run(QPDF_EXE, ["--show-npages", pdfPath], {
    timeoutMs: TIMEOUT_QPDF_MS,
  });
  const n = Number(String(out).trim());
  return Number.isFinite(n) && n > 0 ? n : 0;
}

async function renamePartsToStandard(rawParts, partsDir) {
  const meta = [];
  const renamedPaths = [];

  for (let i = 0; i < rawParts.length; i++) {
    const idx = i + 1;
    const stdName = `goodpdf-${String(idx).padStart(2, "0")}.pdf`;
    const stdPath = path.join(partsDir, stdName);

    try {
      await fsp.unlink(stdPath);
    } catch {}
    await fsp.rename(rawParts[i], stdPath);

    const bytes = await getFileSizeBytes(stdPath);
    const sizeMb = bytesToMb1(bytes);

    meta.push({ name: stdName, bytes, sizeMb });
    renamedPaths.push(stdPath);
  }

  return { renamedPaths, meta };
}

async function splitPdfByMbTarget(inputPdf, splitMb, outDir, onProgress) {
  if (!splitMb || splitMb <= 0) {
    const out = path.join(outDir, "part_001.pdf");
    await fsp.copyFile(inputPdf, out);
    if (onProgress) await onProgress(100);
    return [out];
  }

  const totalPages = await getTotalPages(inputPdf);
  if (!totalPages) {
    const out = path.join(outDir, "part_001.pdf");
    await fsp.copyFile(inputPdf, out);
    if (onProgress) await onProgress(100);
    return [out];
  }

  const targetBytes = Math.max(
    256 * 1024,
    Math.floor(splitMb * 1024 * 1024 * 0.97)
  );

  const totalBytes = await getFileSizeBytes(inputPdf);
  const bytesPerPage = Math.max(1024, Math.floor(totalBytes / totalPages));
  let estPages = Math.max(1, Math.floor(targetBytes / bytesPerPage));
  estPages = Math.min(estPages, totalPages);

  async function makePart(start, end, partIndex) {
    const name = `part_${String(partIndex).padStart(3, "0")}.pdf`;
    const outPath = path.join(outDir, name);

    await run(
      QPDF_EXE,
      [inputPdf, "--pages", ".", `${start}-${end}`, "--", outPath],
      {
        timeoutMs: TIMEOUT_QPDF_MS,
      }
    );

    const sz = await getFileSizeBytes(outPath);
    return { outPath, size: sz, endPage: end };
  }

  const parts = [];
  let partIndex = 1;
  let start = 1;

  while (start <= totalPages) {
    let end = Math.min(totalPages, start + estPages - 1);
    let best = await makePart(start, end, partIndex);

    if (best.size > targetBytes && end > start) {
      let lo = start;
      let hi = end;

      while (lo < hi) {
        const mid = Math.floor((lo + hi) / 2);
        const trialEnd = Math.max(start, mid);
        const trial = await makePart(start, trialEnd, partIndex);

        if (trial.size > targetBytes && trialEnd > start) hi = trialEnd - 1;
        else {
          best = trial;
          lo = trialEnd + 1;
        }
      }
    } else {
      while (end < totalPages) {
        const trialEnd = Math.min(
          totalPages,
          end + Math.max(1, Math.floor(estPages / 3))
        );
        const trial = await makePart(start, trialEnd, partIndex);
        if (trial.size <= targetBytes) {
          end = trialEnd;
          best = trial;
        } else break;
      }
    }

    parts.push(best.outPath);
    partIndex++;
    start = end + 1;

    if (onProgress) {
      const pct = clampPct((best.endPage / totalPages) * 100, 0);
      await onProgress(Math.min(99, pct));
    }
  }

  if (onProgress) await onProgress(100);
  return parts;
}

async function zipOutputs(files, zipPath) {
  await run(SEVEN_Z_EXE, ["a", "-tzip", zipPath, ...files], {
    timeoutMs: TIMEOUT_7Z_MS,
  });
}

// ----------------------
// Queue (DB) — select MINIMAL fields only
// ----------------------
async function fetchQueue(limit = 1) {
  const { data, error } = await supabase
    .from("jobs")
    .select(
      "id,input_path,split_mb,status,created_at,file_name,file_size_bytes,claimed_by"
    )
    .in("status", ["QUEUED", "UPLOADED"])
    .is("claimed_by", null)
    .order("created_at", { ascending: false })
    .limit(limit);

  if (error) throw error;
  return data || [];
}

async function claimJob(jobId) {
  // claim-ийг хамгийн жижиг patch-аар хийе (schema зөрчил багасна)
  const patch = {
    status: "PROCESSING",
    stage: "QUEUE",
    progress: 1,
    claimed_by: WORKER_ID,
    claimed_at: nowIso(),
    error_text: null,
  };

  // зөвхөн QUEUED дээр claim хийж lock үүсгэнэ
  const { data, error } = await supabase
    .from("jobs")
    .update(patch)
    .eq("id", jobId)
    .eq("status", "QUEUED")
    .select("id")
    .maybeSingle();

  if (error) return false;
  return !!data;
}

// ----------------------
// Cleanup — schema байхгүй багана байвал автоматаар алгасна
// ----------------------
let CLEANUP_DISABLED = false;

async function cleanupExpiredJobs(limit = 25) {
  if (CLEANUP_DISABLED) return 0;
  const now = nowIso();

  // expires_at / cleaned_at / output_zip_path байхгүй байж болно → алдахад cleanup-г унтраана
  const { data: jobs, error } = await supabase
    .from("jobs")
    .select("id,input_path,output_zip_path,expires_at,status,cleaned_at")
    .is("cleaned_at", null)
    .not("expires_at", "is", null)
    .lt("expires_at", now)
    .eq("status", "DONE")
    .order("expires_at", { ascending: true })
    .limit(limit);

  if (error) {
    console.warn("[cleanup] disabled:", error.message);
    CLEANUP_DISABLED = true;
    return 0;
  }

  if (!jobs || jobs.length === 0) return 0;

  for (const j of jobs) {
    try {
      const inputKey = j.input_path || null;
      const outKey = j.output_zip_path || null;

      if (inputKey) await r2DeleteMany(R2_BUCKET_IN, [inputKey]);
      if (outKey) await r2DeleteMany(R2_BUCKET_OUT, [outKey]);

      await updateJobSafe(j.id, {
        status: "CLEANED",
        stage: "CLEANUP",
        cleaned_at: nowIso(),
        input_path: null,
        output_zip_path: null,
      });

      console.log(`[cleanup] cleaned job=${j.id}`);
    } catch (e) {
      console.error("[cleanup] failed job=", j?.id, e?.message || e);
    }
  }

  return jobs.length;
}

// ----------------------
// Job processing (R2 in/out) — SPLIT ONLY
// ----------------------
function normalizeInputKey(job) {
  // canonical: {jobId}/input.pdf
  return `${job.id}/input.pdf`.replace(/^\/+/, "");
}
function outputZipKeyFor(job) {
  return `${job.id}/goodpdf.zip`;
}

async function processOneJob(job) {
  const jobId = job.id;

  const claimed = await claimJob(jobId);
  if (!claimed) return;

  const wd = tmpDir(jobId);
  const inPdf = path.join(wd, "input.pdf");
  const partsDir = path.join(wd, "parts");
  fs.mkdirSync(partsDir, { recursive: true });

  const inputKey = normalizeInputKey(job);
  const outZipKey = outputZipKeyFor(job);

  try {
    await updateStage(jobId, { stage: "DOWNLOAD", progress: 5 });

    const exists = await r2Exists(R2_BUCKET_IN, inputKey);
    if (!exists)
      throw new Error(`Input not found in R2: ${R2_BUCKET_IN}/${inputKey}`);

    await r2DownloadToFile(R2_BUCKET_IN, inputKey, inPdf);

    const originalBytes = await getFileSizeBytes(inPdf);
    await updateJobSafe(jobId, {
      file_size_bytes: originalBytes,
      input_path: inputKey,
    });

    await updateStage(jobId, { progress: 15 });

    // ---- SPLIT ----
    const splitMb = Number(job.split_mb || 0);
    await updateStage(jobId, {
      stage: "SPLIT",
      progress: 20,
      split_progress: 0,
    });

    const rawParts = await splitPdfByMbTarget(
      inPdf,
      splitMb,
      partsDir,
      async (pct) => {
        const overall = 20 + clampPct(pct, 0) * 0.65; // 20..85
        await updateStage(jobId, { split_progress: pct, progress: overall });
      }
    );

    const { renamedPaths: parts, meta: partsMeta } =
      await renamePartsToStandard(rawParts, partsDir);

    const partsCount = partsMeta.length;
    const totalPartsBytes = partsMeta.reduce(
      (s, x) => s + Number(x.bytes || 0),
      0
    );
    const maxPartMb =
      partsMeta.length > 0
        ? Math.max(...partsMeta.map((x) => Number(x.sizeMb || 0)))
        : null;

    // optional metadata — байхгүй багана бол автоматаар хасагдана
    await updateJobSafe(jobId, {
      parts_count: partsCount,
      parts_json: partsMeta,
      total_parts_bytes: totalPartsBytes,
      max_part_mb: maxPartMb,
    });

    await updateStage(jobId, { progress: 85, split_progress: 100 });

    // ---- ZIP ----
    await updateStage(jobId, { stage: "ZIP", progress: 88 });

    const baseName = sanitizeBaseName(job.file_name || "file");
    const zipLocal = path.join(wd, `goodPDF - ${baseName}.zip`);

    await zipOutputs(parts, zipLocal);

    const zipBytes = await getFileSizeBytes(zipLocal);
    await updateJobSafe(jobId, { output_zip_bytes: zipBytes });

    await updateStage(jobId, { stage: "UPLOAD_OUT", progress: 94 });

    await r2UploadFile(R2_BUCKET_OUT, outZipKey, zipLocal, "application/zip");

    // ---- DONE ----
    await updateJobSafe(jobId, {
      status: "DONE",
      stage: "DONE",
      progress: 100,
      output_zip_path: outZipKey,
      done_at: nowIso(),
      expires_at: addMinutesIso(OUTPUT_TTL_MINUTES),
      error_text: null,
      split_progress: 100,
    });

    console.log("[DONE]", jobId, {
      input: `${R2_BUCKET_IN}/${inputKey}`,
      out: `${R2_BUCKET_OUT}/${outZipKey}`,
      splitMb,
      parts: partsCount,
      maxPartMb,
      zipBytes,
      ttlMin: OUTPUT_TTL_MINUTES,
    });
  } catch (e) {
    const msg = e?.stack || e?.message || String(e);
    console.error("FAILED job", jobId, msg);

    await updateJobSafe(jobId, {
      status: "FAILED",
      stage: "FAILED",
      progress: 0,
      error_text: String(msg).slice(0, 1800),
      done_at: nowIso(),
    });
  } finally {
    try {
      fs.rmSync(wd, { recursive: true, force: true });
    } catch {}
  }
}

// ----------------------
// Main loop
// ----------------------
async function main() {
  console.log("goodpdf local worker started (SPLIT ONLY)", {
    WORKER_ID,
    POLL_MS,
    CONCURRENCY,
    R2_BUCKET_IN,
    R2_BUCKET_OUT,
    OUTPUT_TTL_MINUTES,
    DO_CLEANUP,
    CLEANUP_EVERY_MS,
    TIMEOUT_QPDF_MS,
    TIMEOUT_7Z_MS,
  });

  let lastCleanupAt = 0;

  while (true) {
    try {
      const nowMs = Date.now();
      if (DO_CLEANUP && nowMs - lastCleanupAt > CLEANUP_EVERY_MS) {
        lastCleanupAt = nowMs;
        await cleanupExpiredJobs(25);
      }

      const jobs = await fetchQueue(CONCURRENCY);

      if (jobs.length === 0) {
        await new Promise((r) => setTimeout(r, POLL_MS));
        continue;
      }

      await Promise.all(
        jobs.map(async (j) => {
          console.log(
            "Picked job",
            j.id,
            "status=",
            j.status,
            "splitMb=",
            j.split_mb
          );
          await processOneJob(j);
        })
      );
    } catch (e) {
      console.error("Loop error:", e?.message || e);
      await new Promise((r) => setTimeout(r, POLL_MS));
    }
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
