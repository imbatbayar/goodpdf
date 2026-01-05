// worker-local/worker.mjs — GOODPDF (Split-only) Production Worker
// Supabase schema (from jobs_rows.csv):
// - output_zip_path, zip_path, ttl_minutes, delete_at, cleaned_at
// - parts_json, parts_count, total_parts_bytes, output_zip_bytes
// - stage, progress, split_progress, error_text, error_code, claimed_by, claimed_at

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

// ----------------------
// ENV
// ----------------------
const WORKER_ID =
  process.env.WORKER_ID || `worker_${crypto.randomBytes(3).toString("hex")}`;

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const R2_ENDPOINT = process.env.R2_ENDPOINT;
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID;
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY;

const R2_BUCKET_IN =
  process.env.R2_BUCKET_IN || process.env.R2_BUCKET || "goodpdf-in";
const R2_BUCKET_OUT =
  process.env.R2_BUCKET_OUT || process.env.R2_BUCKET || "goodpdf-out";

const POLL_MS = Number(process.env.POLL_MS || 2000);
const CONCURRENCY = Math.max(1, Number(process.env.CONCURRENCY || 1));

// TTL fallback (minutes) if job.ttl_minutes is null
const DEFAULT_TTL_MINUTES = Math.max(
  1,
  Number(
    process.env.DEFAULT_TTL_MINUTES || process.env.OUTPUT_TTL_MINUTES || 30
  )
);

const DO_CLEANUP =
  String(process.env.DO_CLEANUP || "true").toLowerCase() !== "false";
const CLEANUP_EVERY_MS = Math.max(
  10_000,
  Number(process.env.CLEANUP_EVERY_MS || 30_000)
);

const TIMEOUT_QPDF_MS = Math.max(
  10_000,
  Number(process.env.TIMEOUT_QPDF_MS || 240_000)
);
const TIMEOUT_7Z_MS = Math.max(
  10_000,
  Number(process.env.TIMEOUT_7Z_MS || 240_000)
);

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}
if (!R2_ENDPOINT || !R2_ACCESS_KEY_ID || !R2_SECRET_ACCESS_KEY) {
  console.error(
    "Missing R2 env vars (R2_ENDPOINT/R2_ACCESS_KEY_ID/R2_SECRET_ACCESS_KEY)"
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
function tmpDir(jobId) {
  const d = path.join(os.tmpdir(), `goodpdf_${jobId}`);
  fs.mkdirSync(d, { recursive: true });
  return d;
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
function safeStatSize(p) {
  try {
    if (!safeExists(p)) return null;
    const st = fs.statSync(p);
    if (!st || !Number.isFinite(st.size)) return null;
    return st.size;
  } catch {
    return null;
  }
}
function bytesToMb(n) {
  return n / (1024 * 1024);
}
function mbToBytes(mb) {
  return mb * 1024 * 1024;
}
function rand6() {
  return crypto.randomBytes(3).toString("hex");
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

function runCmd(cmd, args, timeoutMs, spawnOpts = {}) {
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
      if (killed)
        return reject(new Error(`${cmd} timeout after ${timeoutMs}ms`));
      if (code !== 0)
        return reject(new Error(`${cmd} failed code=${code}\n${err || out}`));
      resolve({ out, err });
    });
  });
}


// ----------------------
// Supabase helpers
// ----------------------
async function updateJob(jobId, patch) {
  const { error } = await supabase.from("jobs").update(patch).eq("id", jobId);
  if (error) throw error;
}

/**
 * Worker processes only QUEUED jobs (user pressed Start).
 */
async function fetchQueue(limit = 1) {
  const { data, error } = await supabase
    .from("jobs")
    .select(
      "id,input_path,split_mb,status,created_at,file_name,file_size_bytes,claimed_by,ttl_minutes"
    )
    .in("status", ["QUEUED"])
    .is("claimed_by", null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
  return data || [];
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
    .select("id")
    .maybeSingle();

  if (error) return false;
  return !!data;
}

// ----------------------
// R2 helpers
// ----------------------
function normalizeInputKey(job) {
  if (job.input_path) return String(job.input_path).replace(/^\/+/, "");
  return `${job.id}/input.pdf`.replace(/^\/+/, "");
}
function outputZipKeyFor(job) {
  return `${job.id}/goodpdf.zip`;
}

async function downloadFromR2(key, outPath) {
  const res = await r2.send(
    new GetObjectCommand({ Bucket: R2_BUCKET_IN, Key: key })
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
    })
  );
}

// ----------------------
// Cleanup (delete_at + output_zip_path)
// ----------------------
async function cleanupExpiredOutputs() {
  if (!DO_CLEANUP) return 0;

  const { data, error } = await supabase
    .from("jobs")
    .select("id,output_zip_path,zip_path,delete_at,cleaned_at,status")
    .is("cleaned_at", null)
    .not("delete_at", "is", null)
    .lt("delete_at", nowIso())
    .limit(50);

  if (error) throw error;

  const rows = data || [];
  let cleaned = 0;

  for (const r of rows) {
    const key =
      (r.output_zip_path && String(r.output_zip_path)) ||
      (r.zip_path && String(r.zip_path)) ||
      null;

    if (key) {
      try {
        await r2.send(
          new DeleteObjectCommand({ Bucket: R2_BUCKET_OUT, Key: key })
        );
      } catch {
        // best effort
      }
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
    } catch {
      // best effort
    }
  }

  return cleaned;
}

// ----------------------
// SPLIT: Adaptive range packing (never-crash)
// ----------------------
async function splitPdfGreedy({ inPdf, partsDir, limitMb }) {
  const limitBytes = mbToBytes(limitMb);

  const { out: npagesOut } = await runCmd(
    "qpdf",
    ["--show-npages", inPdf],
    TIMEOUT_QPDF_MS
  );
  const totalPages = Number(String(npagesOut).trim());
  if (!Number.isFinite(totalPages) || totalPages <= 0) {
    throw new Error("Could not read page count");
  }

  function partTmpName(partNo) {
    return path.join(
      partsDir,
      `.__part_${String(partNo).padStart(3, "0")}_${rand6()}.pdf`
    );
  }
  function candTmpName(startPage, endPage) {
    return path.join(
      partsDir,
      `.__cand_${startPage}_${endPage}_${rand6()}.pdf`
    );
  }

  async function tryBuildRange(startPage, endPage, outPath) {
    try {
      await runCmd(
        "qpdf",
        ["--empty", "--pages", inPdf, `${startPage}-${endPage}`, "--", outPath],
        TIMEOUT_QPDF_MS
      );
    } catch {
      return null;
    }
    return safeStatSize(outPath);
  }

  const tempParts = []; // {tmpPath, bytes, startPage, endPage}
  let start = 1;
  let partNo = 1;
  let prevSpan = 12;

  while (start <= totalPages) {
    // build single page (must be monotonic baseline)
    const oneTmp = partTmpName(partNo);
    let oneBytes = await tryBuildRange(start, start, oneTmp);

    // hard fail: even single page cannot be built after retry
    if (oneBytes == null) {
      safeUnlink(oneTmp);
      const oneTmp2 = partTmpName(partNo);
      oneBytes = await tryBuildRange(start, start, oneTmp2);
      if (oneBytes == null) {
        safeUnlink(oneTmp2);
        throw Object.assign(new Error("QPDF_RANGE_BUILD_FAILED"), {
          code: "QPDF_RANGE_BUILD_FAILED",
          startPage: start,
          endPage: start,
        });
      }
      tempParts.push({
        tmpPath: oneTmp2,
        bytes: oneBytes,
        startPage: start,
        endPage: start,
      });
      partNo++;
      start++;
      prevSpan = 1;
      continue;
    }

    // single page exceeds target => still output it (page-aligned rule)
    if (oneBytes > limitBytes) {
      tempParts.push({
        tmpPath: oneTmp,
        bytes: oneBytes,
        startPage: start,
        endPage: start,
      });
      partNo++;
      start++;
      prevSpan = 1;
      continue;
    }

    // exponential expand
    let goodEnd = start;
    let goodBytes = oneBytes;

    let step = Math.max(1, prevSpan);
    let probeEnd = Math.min(totalPages, start + step - 1);
    let foundTooBig = false;

    while (true) {
      const cand = candTmpName(start, probeEnd);
      const candBytes = await tryBuildRange(start, probeEnd, cand);

      if (candBytes != null && candBytes <= limitBytes) {
        goodEnd = probeEnd;
        goodBytes = candBytes;
        safeUnlink(cand);

        if (probeEnd >= totalPages) break;
        step *= 2;
        probeEnd = Math.min(totalPages, goodEnd + step);
        continue;
      }

      safeUnlink(cand);
      foundTooBig = true;
      break;
    }

    if (goodEnd === start) {
      // keep single page
      tempParts.push({
        tmpPath: oneTmp,
        bytes: oneBytes,
        startPage: start,
        endPage: start,
      });
      partNo++;
      start++;
      prevSpan = 1;
      continue;
    }

    // we extended; remove single tmp
    safeUnlink(oneTmp);

    // binary refine if we hit tooBig
    if (foundTooBig) {
      const tooBigEnd = probeEnd;
      if (tooBigEnd > goodEnd + 1) {
        let lo = goodEnd + 1;
        let hi = tooBigEnd - 1;
        while (lo <= hi) {
          const mid = Math.floor((lo + hi) / 2);
          const cand = candTmpName(start, mid);
          const candBytes = await tryBuildRange(start, mid, cand);

          if (candBytes != null && candBytes <= limitBytes) {
            goodEnd = mid;
            goodBytes = candBytes;
            safeUnlink(cand);
            lo = mid + 1;
          } else {
            safeUnlink(cand);
            hi = mid - 1;
          }
        }
      }
    }

    // build final [start..goodEnd] with backoff safety
    let finalStart = start;
    let finalEnd = goodEnd;
    let finalTmp = partTmpName(partNo);
    let finalBytes = await tryBuildRange(finalStart, finalEnd, finalTmp);

    // backoff if build failed or oversize
    let guard = 0;
    while (
      (finalBytes == null || finalBytes > limitBytes) &&
      finalEnd > finalStart
    ) {
      safeUnlink(finalTmp);
      finalEnd--;
      finalTmp = partTmpName(partNo);
      finalBytes = await tryBuildRange(finalStart, finalEnd, finalTmp);
      guard++;
      if (guard > 200) break;
    }

    if (finalBytes == null) {
      safeUnlink(finalTmp);
      throw Object.assign(new Error("QPDF_FINAL_BUILD_FAILED"), {
        code: "QPDF_FINAL_BUILD_FAILED",
        startPage: finalStart,
        endPage: finalEnd,
      });
    }

    // last resort: if still oversize (should be rare), fallback to single page
    if (finalBytes > limitBytes && finalEnd > finalStart) {
      safeUnlink(finalTmp);
      finalEnd = finalStart;
      finalTmp = partTmpName(partNo);
      const b = await tryBuildRange(finalStart, finalEnd, finalTmp);
      if (b == null) {
        safeUnlink(finalTmp);
        throw Object.assign(new Error("QPDF_SINGLE_REBUILD_FAILED"), {
          code: "QPDF_SINGLE_REBUILD_FAILED",
          startPage: finalStart,
          endPage: finalEnd,
        });
      }
      finalBytes = b;
    }

    tempParts.push({
      tmpPath: finalTmp,
      bytes: finalBytes,
      startPage: finalStart,
      endPage: finalEnd,
    });

    prevSpan = Math.max(1, finalEnd - finalStart + 1);
    partNo++;
    start = finalEnd + 1;
  }

  // rename to canonical goodPDF-<TOTAL>(<i>).pdf
  const totalParts = tempParts.length;
  const partFiles = [];
  const partMeta = [];

  for (let idx = 0; idx < tempParts.length; idx++) {
    const p = tempParts[idx];
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
      startPageIndex: p.startPage,
      endPageIndex: p.endPage,
    });
  }

  // cleanup leftovers
  try {
    for (const f of fs.readdirSync(partsDir)) {
      if (f.startsWith(".__cand_") || f.startsWith(".__part_")) {
        safeUnlink(path.join(partsDir, f));
      }
    }
  } catch {}

  return { partFiles, partMeta };
}

async function zipParts(partsDir, outZipPath) {
  // ✅ Wildcard ашиглахгүй — бодит PDF жагсаалтыг 7z-д өгнө (хоосон ZIP асуудлыг бүр мөсөн хаана)
  const pdfs = fs
    .readdirSync(partsDir)
    .filter((f) => f.toLowerCase().endsWith(".pdf"))
    .sort();

  console.log("[ZIP] partsDir =", partsDir);
  console.log("[ZIP] pdf count =", pdfs.length);
  if (pdfs.length) console.log("[ZIP] sample =", pdfs.slice(0, 5));

  if (pdfs.length === 0) {
    throw Object.assign(new Error("NO_PART_PDFS_TO_ZIP"), {
      code: "NO_PART_PDFS_TO_ZIP",
      partsDir,
    });
  }

  // outZipPath абсолют байж болно; cwd=partsDir үед файлуудыг нэрээр нь нэмнэ
  await runCmd("7z", ["a", "-tzip", outZipPath, ...pdfs], TIMEOUT_7Z_MS, {
    cwd: partsDir,
  });
}


// ----------------------
// Process one job
// ----------------------
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
    await updateJob(jobId, {
      stage: "DOWNLOAD",
      progress: 5,
      split_progress: 0,
      updated_at: nowIso(),
    });
    await downloadFromR2(inputKey, inPdf);

    const splitMb = Number(job.split_mb || 0);
    if (!Number.isFinite(splitMb) || splitMb <= 0) {
      throw Object.assign(new Error("split_mb missing/invalid"), {
        code: "SPLIT_MB_INVALID",
      });
    }

    await updateJob(jobId, {
      stage: "SPLIT",
      progress: 20,
      split_progress: 10,
      updated_at: nowIso(),
    });

    const { partFiles, partMeta } = await splitPdfGreedy({
      inPdf,
      partsDir,
      limitMb: splitMb,
    });

    await updateJob(jobId, {
      stage: "ZIP",
      progress: 70,
      split_progress: 90,
      updated_at: nowIso(),
    });

    const outZip = path.join(wd, "goodpdf.zip");
    await zipParts(partsDir, outZip);

    await updateJob(jobId, {
      stage: "UPLOAD_OUT",
      progress: 85,
      split_progress: 95,
      updated_at: nowIso(),
    });

    await uploadToR2(outZipKey, outZip, "application/zip");

    // compute stats
    const outZipBytes = safeStatSize(outZip) ?? null;
    const partBytes = partMeta
      .map((p) => p.bytes)
      .filter((x) => typeof x === "number" && Number.isFinite(x));
    const totalPartsBytes = partBytes.length
      ? partBytes.reduce((a, b) => a + b, 0)
      : null;
    const maxPartBytes = partBytes.length ? Math.max(...partBytes) : null;

    const ttl = Number(job.ttl_minutes || 0);
    const ttlMinutes =
      Number.isFinite(ttl) && ttl > 0 ? ttl : DEFAULT_TTL_MINUTES;
    const deleteAtIso = new Date(
      Date.now() + ttlMinutes * 60_000
    ).toISOString();

    // ✅ THIS is the key fix for "Ready but not downloading":
    // output_zip_path (and zip_path for compatibility) must be filled.
    const donePatch = {
      status: "DONE",
      stage: "DONE",
      progress: 100,
      split_progress: 100,

      output_zip_path: outZipKey,
      zip_path: outZipKey, // legacy compatibility

      ttl_minutes: ttlMinutes,
      delete_at: deleteAtIso,

      parts_count: partFiles.length,
      parts_json: partMeta,
      total_parts_bytes: totalPartsBytes,
      output_zip_bytes: outZipBytes,

      target_mb: splitMb,
      max_part_mb:
        typeof maxPartBytes === "number"
          ? Math.round(bytesToMb(maxPartBytes) * 100) / 100
          : null,

      error_text: null,
      error_code: null,
      updated_at: nowIso(),
    };

    await updateJob(jobId, donePatch);
  } catch (e) {
    const msg = String(e?.message || e);
    const stack = e?.stack ? String(e.stack) : msg;

    await updateJob(jobId, {
      status: "FAILED",
      stage: "FAILED",
      progress: 100,
      split_progress: 100,
      error_code: e?.code ? String(e.code) : "WORKER_ERROR",
      error_text: stack.slice(0, 1800),
      updated_at: nowIso(),
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
  console.log("goodpdf worker started", {
    WORKER_ID,
    POLL_MS,
    CONCURRENCY,
    R2_BUCKET_IN,
    R2_BUCKET_OUT,
    DEFAULT_TTL_MINUTES,
    DO_CLEANUP,
    CLEANUP_EVERY_MS,
  });

  let lastCleanupAt = 0;

  while (true) {
    try {
      const now = Date.now();
      if (DO_CLEANUP && now - lastCleanupAt >= CLEANUP_EVERY_MS) {
        lastCleanupAt = now;
        try {
          const cleaned = await cleanupExpiredOutputs();
          if (cleaned > 0)
            console.log("Cleanup removed", cleaned, "expired outputs");
        } catch (e) {
          console.log("Cleanup error (ignored):", e?.message || e);
        }
      }

      const jobs = await fetchQueue(CONCURRENCY);

      if (!jobs.length) {
        await sleep(POLL_MS);
        continue;
      }

      await Promise.all(jobs.map(processOneJob));
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
