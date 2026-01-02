import "dotenv/config";
import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import crypto from "node:crypto";
import { spawn } from "node:child_process";
import { createClient } from "@supabase/supabase-js";

// ----------------------
// ENV
// ----------------------
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const GS_EXE = process.env.GS_EXE || "gswin64c";
const QPDF_EXE = process.env.QPDF_EXE || "qpdf";
const SEVEN_Z_EXE = process.env.SEVEN_Z_EXE || "7z";

const POLL_MS = Number(process.env.POLL_MS || 2000);
const CONCURRENCY = Math.max(1, Number(process.env.CONCURRENCY || 1));

// Buckets
const BUCKET_IN = process.env.BUCKET_IN || "job-input";
const BUCKET_OUT = process.env.BUCKET_OUT || "jobs-output";

// Job TTL + stale хамгаалалт
const OUTPUT_TTL_MINUTES = Number(process.env.OUTPUT_TTL_MINUTES || 60); // ✅ DONE болсон output хэдэн минутын дараа УСТАХ вэ
const STALE_PROCESSING_MINUTES = Number(
  process.env.STALE_PROCESSING_MINUTES || 15
);

// ✅ Auto-cleanup loop (expired болсон job-уудын input/output-г storage-оос арилгана)
const CLEANUP_EVERY_MS = Number(process.env.CLEANUP_EVERY_MS || 30000); // 30s тутам шалгана
const DO_CLEANUP = String(process.env.DO_CLEANUP || "1") === "1"; // 0 бол унтраана

// Worker identity
const WORKER_ID = `${os.hostname()}_${process.pid}`;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error(
    "Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY in worker-local/.env"
  );
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

// ----------------------
// Helpers
// ----------------------
function run(cmd, args, opts = {}) {
  return new Promise((resolve, reject) => {
    const p = spawn(cmd, args, { stdio: "pipe", ...opts });
    let out = "";
    let err = "";
    p.stdout.on("data", (d) => (out += d.toString()));
    p.stderr.on("data", (d) => (err += d.toString()));
    p.on("error", reject);
    p.on("close", (code) => {
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

function sha() {
  return crypto.randomBytes(8).toString("hex");
}

function nowIso() {
  return new Date().toISOString();
}

function addMinutesIso(m) {
  return new Date(Date.now() + m * 60 * 1000).toISOString();
}

function clampInt(n, lo, hi) {
  const x = Number(n);
  if (!Number.isFinite(x)) return lo;
  return Math.max(lo, Math.min(hi, Math.floor(x)));
}

async function updateJob(jobId, patch) {
  const { error } = await supabase.from("jobs").update(patch).eq("id", jobId);
  if (error) throw error;
}

async function downloadToFile(bucket, remotePath, localPath) {
  const { data, error } = await supabase.storage
    .from(bucket)
    .download(remotePath);
  if (error) throw error;
  const ab = await data.arrayBuffer();
  await fsp.writeFile(localPath, Buffer.from(ab));
}

async function uploadFile(
  bucket,
  remotePath,
  localPath,
  contentType = "application/octet-stream"
) {
  const buf = await fsp.readFile(localPath);
  const { error } = await supabase.storage
    .from(bucket)
    .upload(remotePath, buf, { contentType, upsert: true });
  if (error) throw error;
}

async function getFileSizeBytes(p) {
  const st = await fsp.stat(p);
  return st.size;
}

async function getTotalPages(pdfPath) {
  const { out } = await run(QPDF_EXE, ["--show-npages", pdfPath]);
  const n = Number(String(out).trim());
  return Number.isFinite(n) && n > 0 ? n : 0;
}

// ----------------------
// Queue
// ----------------------
async function fetchQueue(limit = 1) {
  const { data, error } = await supabase
    .from("jobs")
    .select("id,user_id,input_path,quality,split_mb,status,progress,created_at")
    .in("status", ["UPLOADED"])
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
  return data || [];
}

async function claimJob(jobId) {
  const { data, error } = await supabase
    .from("jobs")
    .update({
      status: "PROCESSING",
      progress: 1,
      processing_started_at: nowIso(),
      error_text: null,
      claimed_by: WORKER_ID,
      claimed_at: nowIso(),
    })
    .eq("id", jobId)
    .eq("status", "UPLOADED")
    .select("id")
    .maybeSingle();

  if (error) throw error;
  return !!data;
}

async function requeueStaleProcessing(minutes) {
  const cutoff = new Date(Date.now() - minutes * 60 * 1000).toISOString();
  const { error } = await supabase
    .from("jobs")
    .update({
      status: "UPLOADED",
      progress: 0,
      error_text: "Requeued: stale PROCESSING",
      processing_started_at: null,
      claimed_by: null,
      claimed_at: null,
    })
    .eq("status", "PROCESSING")
    .lt("processing_started_at", cutoff);

  if (error) throw error;
}

// ----------------------
// ✅ Auto Cleanup (expires_at өнгөрмөгц input+output storage-оос устгана)
// ----------------------
async function deleteFromBucket(bucket, filePath) {
  if (!filePath) return;
  try {
    const { error } = await supabase.storage.from(bucket).remove([filePath]);
    if (error) throw error;
  } catch {
    // best-effort delete
  }
}

async function markCleaned(jobId, extra = {}) {
  // Зарим үед job_status enum-д CLEANED байхгүй байж магадгүй.
  // Тийм үед status update нь алдаа өгнө. Тэгвэл fallback: status-г оролдохгүй, зөвхөн cleaned_at + path-уудыг null болгоно.
  const basePatch = {
    status: "CLEANED",
    cleaned_at: nowIso(),
    input_path: null,
    output_zip_path: null,
    zip_path: null,
    ...extra,
  };

  const { error: err1 } = await supabase
    .from("jobs")
    .update(basePatch)
    .eq("id", jobId);

  if (!err1) return;

  const msg = String(err1.message || err1);
  const looksLikeEnum =
    msg.toLowerCase().includes("invalid input value") &&
    msg.toLowerCase().includes("enum");

  if (!looksLikeEnum) {
    // өөр алдаа бол throw
    throw err1;
  }

  // fallback: status-г оролдохгүй
  const fallbackPatch = {
    cleaned_at: nowIso(),
    input_path: null,
    output_zip_path: null,
    zip_path: null,
    error_text: "CLEANED (enum missing)",
    ...extra,
  };

  const { error: err2 } = await supabase
    .from("jobs")
    .update(fallbackPatch)
    .eq("id", jobId);
  if (err2) throw err2;
}

async function cleanupExpiredJobs(limit = 25) {
  const now = nowIso();

  const { data: jobs, error } = await supabase
    .from("jobs")
    .select("id,input_path,output_zip_path,zip_path,expires_at,status")
    .eq("status", "DONE")
    .lt("expires_at", now)
    .order("expires_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!jobs || jobs.length === 0) return 0;

  for (const j of jobs) {
    try {
      const outPath = j.output_zip_path || j.zip_path || null;

      // storage delete
      await deleteFromBucket(BUCKET_IN, j.input_path);
      await deleteFromBucket(BUCKET_OUT, outPath);

      // db mark
      await markCleaned(j.id);

      console.log(`[cleanup] cleaned job=${j.id}`);
    } catch (e) {
      console.error("[cleanup] failed job=", j?.id, e?.message || e);
    }
  }

  return jobs.length;
}

// ----------------------
// Core pipeline
// ----------------------
function mapQualityToGsPreset(q) {
  if (!q || q === "GOOD") return "/ebook";
  if (q === "MAX") return "/printer";
  if (q === "ORIGINAL") return null;
  if (q === "low") return "/screen";
  if (q === "high") return "/printer";
  return "/ebook";
}

async function compressPdfWithGhostscript(inputPdf, outputPdf, qualityMode) {
  const preset = mapQualityToGsPreset(qualityMode);
  if (!preset) {
    await fsp.copyFile(inputPdf, outputPdf);
    return;
  }

  await run(GS_EXE, [
    "-sDEVICE=pdfwrite",
    "-dCompatibilityLevel=1.4",
    "-dPDFSETTINGS=" + preset,
    "-dNOPAUSE",
    "-dQUIET",
    "-dBATCH",
    `-sOutputFile=${outputPdf}`,
    inputPdf,
  ]);
}

async function splitPdfByMbTarget(inputPdf, splitMb, outDir) {
  if (!splitMb || splitMb <= 0) {
    const out = path.join(outDir, "part_001.pdf");
    await fsp.copyFile(inputPdf, out);
    return [out];
  }

  const totalPages = await getTotalPages(inputPdf);
  if (!totalPages) {
    const out = path.join(outDir, "part_001.pdf");
    await fsp.copyFile(inputPdf, out);
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

    await run(QPDF_EXE, [
      inputPdf,
      "--pages",
      ".",
      `${start}-${end}`,
      "--",
      outPath,
    ]);
    const sz = await getFileSizeBytes(outPath);
    return { outPath, size: sz };
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

        if (trial.size > targetBytes && trialEnd > start) {
          hi = trialEnd - 1;
        } else {
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
  }

  return parts;
}

async function zipOutputs(files, zipPath) {
  await run(SEVEN_Z_EXE, ["a", "-tzip", zipPath, ...files]);
}

async function setProgressSafe(jobId, p) {
  const val = clampInt(p, 0, 100);
  await updateJob(jobId, { progress: val });
}

async function processOneJob(job) {
  const jobId = job.id;

  if (!job.input_path) {
    await updateJob(jobId, {
      status: "FAILED",
      progress: 0,
      error_text: "Missing input_path (upload not completed or DB mismatch)",
      done_at: nowIso(),
    });
    return;
  }

  const claimed = await claimJob(jobId);
  if (!claimed) return;

  const userId = job.user_id || "dev";
  const wd = tmpDir(jobId);
  const inPdf = path.join(wd, "input.pdf");
  const compressedPdf = path.join(wd, "compressed.pdf");
  const partsDir = path.join(wd, "parts");
  fs.mkdirSync(partsDir, { recursive: true });

  try {
    await setProgressSafe(jobId, 5);

    await downloadToFile(BUCKET_IN, job.input_path, inPdf);
    await setProgressSafe(jobId, 15);

    const quality = job.quality || "GOOD";
    await compressPdfWithGhostscript(inPdf, compressedPdf, quality);
    await setProgressSafe(jobId, 55);

    const splitMb = Number(job.split_mb || 0);
    const parts = await splitPdfByMbTarget(compressedPdf, splitMb, partsDir);
    await setProgressSafe(jobId, 80);

    const zipLocal = path.join(wd, `out_${sha()}.zip`);
    await zipOutputs(parts, zipLocal);
    await setProgressSafe(jobId, 90);

    const outKey = `${userId}/${jobId}/out.zip`;
    await uploadFile(BUCKET_OUT, outKey, zipLocal, "application/zip");

    await updateJob(jobId, {
      status: "DONE",
      progress: 100,
      output_zip_path: outKey,
      zip_path: outKey,
      done_at: nowIso(),
      expires_at: addMinutesIso(OUTPUT_TTL_MINUTES),
      error_text: null,
    });
  } catch (e) {
    const msg = e?.stack || e?.message || String(e);
    console.error("FAILED job", jobId, msg);
    await updateJob(jobId, {
      status: "FAILED",
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
  console.log("goodpdf local worker started", {
    WORKER_ID,
    POLL_MS,
    CONCURRENCY,
    BUCKET_IN,
    BUCKET_OUT,
    OUTPUT_TTL_MINUTES,
    STALE_PROCESSING_MINUTES,
    DO_CLEANUP,
    CLEANUP_EVERY_MS,
  });

  let lastCleanupAt = 0;

  while (true) {
    try {
      await requeueStaleProcessing(STALE_PROCESSING_MINUTES);

      // ✅ 10 минут өнгөрсөн DONE job-уудыг бүрэн арилгана (input+output)
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
            j.split_mb,
            "quality=",
            j.quality
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
