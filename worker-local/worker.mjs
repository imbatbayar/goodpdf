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
const CONCURRENCY = Number(process.env.CONCURRENCY || 1);

// Buckets (prod-доо нэг мөр болгоё)
const BUCKET_IN = process.env.BUCKET_IN || "job-input";
const BUCKET_OUT = process.env.BUCKET_OUT || "jobs-output";

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
    .upload(remotePath, buf, {
      contentType,
      upsert: true,
    });
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
/**
 * ✅ Зорилгод нийцсэн queue:
 * - зөвхөн UPLOADED (upload бүрэн дууссан)
 */
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

/**
 * ✅ Atomic claim:
 * - status=UPLOADED байгаа үед л PROCESSING болгож “би авлаа” гэж баталгаажуулна.
 * - ингэснээр давхар worker нэг job-г зэрэг авахгүй.
 */
async function claimJob(jobId) {
  const { data, error } = await supabase
    .from("jobs")
    .update({
      status: "PROCESSING",
      progress: 1,
      processing_started_at: nowIso(),
      error_text: null,
    })
    .eq("id", jobId)
    .eq("status", "UPLOADED")
    .select("id")
    .maybeSingle();

  if (error) throw error;
  return !!data;
}

// ----------------------
// Core pipeline
// ----------------------
function mapQualityToGsPreset(q) {
  // DB/UI: ORIGINAL | GOOD | MAX
  if (!q || q === "GOOD") return "/ebook";
  if (q === "MAX") return "/printer";
  if (q === "ORIGINAL") return null; // recompress хийхгүй
  // backward-compat (хуучин утгууд байвал)
  if (q === "low") return "/screen";
  if (q === "high") return "/printer";
  return "/ebook";
}

async function compressPdfWithGhostscript(inputPdf, outputPdf, qualityMode) {
  const preset = mapQualityToGsPreset(qualityMode);

  // ORIGINAL = copy (чанар алдагдуулахгүй)
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

/**
 * ✅ MB target split (adaptive)
 * - part бүр targetMB-ээс доош оруулах гэж оролдоно (headroom-той)
 * - PDF бүр өөр тул “яг таг” гэж амлахгүй, гэхдээ практикт 10MB барина.
 */
async function splitPdfByMbTarget(inputPdf, splitMb, outDir) {
  // splitMb<=0 => нэг файл
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

  // Headroom: PDF overhead + zip overhead-ийг бодоод 3% үлдээе
  const targetBytes = Math.max(
    256 * 1024,
    Math.floor(splitMb * 1024 * 1024 * 0.97)
  );

  // Эхний estimate: bytes/page
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

    // 1) эхний үүсгэлт
    let best = await makePart(start, end, partIndex);

    // 2) Хэтэрвэл end-г багасгана (binary-ish shrink)
    if (best.size > targetBytes && end > start) {
      let lo = start;
      let hi = end;
      let chosen = end;

      while (lo < hi) {
        const mid = Math.floor((lo + hi) / 2);

        // mid хүртэл жижигрүүлж туршина
        const trialEnd = Math.max(start, mid);
        const trial = await makePart(start, trialEnd, partIndex);

        if (trial.size > targetBytes && trialEnd > start) {
          // дахиад багасгана
          hi = trialEnd - 1;
        } else {
          // багтлаа => өсгөх боломж байна
          chosen = trialEnd;
          best = trial;
          lo = trialEnd + 1;
        }
      }
    } else {
      // 3) Дутуу жижиг байвал бага зэрэг өсгөж болно (greedy expand)
      while (end < totalPages) {
        const trialEnd = Math.min(
          totalPages,
          end + Math.max(1, Math.floor(estPages / 3))
        );
        const trial = await makePart(start, trialEnd, partIndex);
        if (trial.size <= targetBytes) {
          end = trialEnd;
          best = trial;
        } else {
          break;
        }
      }
    }

    parts.push(best.outPath);
    partIndex++;
    start = end + 1;
  }

  return parts;
}

async function zipOutputs(files, zipPath) {
  // 7z a -tzip out.zip file1 file2 ...
  await run(SEVEN_Z_EXE, ["a", "-tzip", zipPath, ...files]);
}

async function processOneJob(job) {
  const jobId = job.id;

  // ✅ input_path байхгүй бол энэ нь upload/DB mismatch
  if (!job.input_path) {
    await updateJob(jobId, {
      status: "FAILED",
      progress: 0,
      error_text: "Missing input_path (upload not completed or DB mismatch)",
    });
    return;
  }

  // ✅ Atomic claim (UPLOADED -> PROCESSING)
  const claimed = await claimJob(jobId);
  if (!claimed) {
    // өөр worker аль хэдийн авсан
    return;
  }

  const userId = job.user_id || "dev";
  const wd = tmpDir(jobId);
  const inPdf = path.join(wd, "input.pdf");
  const compressedPdf = path.join(wd, "compressed.pdf");
  const partsDir = path.join(wd, "parts");
  fs.mkdirSync(partsDir, { recursive: true });

  try {
    await updateJob(jobId, { progress: 5 });

    // 1) download input
    await downloadToFile(BUCKET_IN, job.input_path, inPdf);
    await updateJob(jobId, { progress: 15 });

    // 2) compress (ORIGINAL|GOOD|MAX)
    const quality = job.quality || "GOOD";
    await compressPdfWithGhostscript(inPdf, compressedPdf, quality);
    await updateJob(jobId, { progress: 55 });

    // 3) split by target MB
    const splitMb = Number(job.split_mb || 0);
    const parts = await splitPdfByMbTarget(compressedPdf, splitMb, partsDir);
    await updateJob(jobId, { progress: 80 });

    // 4) zip
    const zipLocal = path.join(wd, `out_${sha()}.zip`);
    await zipOutputs(parts, zipLocal);
    await updateJob(jobId, { progress: 90 });

    // 5) upload output zip
    const outKey = `${userId}/${jobId}/out.zip`;
    await uploadFile(BUCKET_OUT, outKey, zipLocal, "application/zip");

    // 6) DONE (DB талдаа download/status эндээс уншина)
    await updateJob(jobId, {
      status: "DONE",
      progress: 100,
      output_zip_path: outKey,
      zip_path: outKey, // backward compat
      done_at: nowIso(),
      expires_at: null, // TTL-г дараагийн алхамд тохируулна
      error_text: null,
    });
  } catch (e) {
    const msg = e?.stack || e?.message || String(e);
    console.error("FAILED job", jobId, msg);
    await updateJob(jobId, {
      status: "FAILED",
      progress: 0,
      error_text: msg.slice(0, 1800),
    });
  } finally {
    // cleanup best-effort
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
    POLL_MS,
    CONCURRENCY,
    BUCKET_IN,
    BUCKET_OUT,
  });

  while (true) {
    try {
      const jobs = await fetchQueue(CONCURRENCY);

      if (jobs.length === 0) {
        await new Promise((r) => setTimeout(r, POLL_MS));
        continue;
      }

      for (const j of jobs) {
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
      }
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
