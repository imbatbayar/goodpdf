// worker-local/worker.mjs — GOODPDF Worker (LOCKED v2.3: 10-min cap + SYSTEM policy fix (near-9MB, page-continuity)
//
// DOWNLOAD -> PREFLIGHT_1 -> (QPDF_FAST_COMPRESS) -> (GS_OPTIONAL) -> PREFLIGHT_2 -> SPLIT -> ZIP -> UPLOAD -> DONE
//
// ✅ Hard UX rule: do not keep user waiting > ~2 minutes (best-effort).
//    If we approach the cap, we SKIP heavy compression and proceed to split+zip.
// ✅ Ghostscript crashes are handled: fallback to qpdf-fast or skip and continue.
// ✅ No split retry loops, no per-page extraction loops.

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
const DEFAULT_TTL_MINUTES = 10;

const DO_CLEANUP =
  String(process.env.DO_CLEANUP || "true").toLowerCase() !== "false";
const CLEANUP_EVERY_MS = Math.max(
  10_000,
  Number(process.env.CLEANUP_EVERY_MS || 30_000),
);

// Binaries
const GS_EXE = process.env.GS_EXE || "gs";
const QPDF_EXE = process.env.QPDF_EXE || "qpdf";

// Timeouts (still bounded by expires_at)
const TIMEOUT_QPDF_MS = Math.max(
  10_000,
  Number(process.env.TIMEOUT_QPDF_MS || 240_000),
);
const TIMEOUT_GS_MS = Math.max(
  10_000,
  Number(process.env.TIMEOUT_GS_MS || 240_000),
);
const TIMEOUT_7Z_MS = Math.max(
  10_000,
  Number(process.env.TIMEOUT_7Z_MS || 240_000),
);

// UX cap: best-effort "do not keep user waiting > 2 min"
const UX_CAP_MS = Math.max(60_000, Number(process.env.UX_CAP_MS || 600_000));
const UX_SOFTSTOP_MS = Math.max(
  50_000,
  Math.min(UX_CAP_MS - 10_000, Number(process.env.UX_SOFTSTOP_MS || 540_000)),
);

// Preprocess mode: ALWAYS | OFF (OFF = split only)

// SYSTEM default policy (System-fit):
// - Always split to <=5 files, each <=9MB (for government portals etc.)
// - If input > 100MB, we may FORCE RASTER rebuild for predictable sizing.
// - We aim to minimize number of parts (1..5) while keeping each <=9MB.
const SYSTEM_FORCE_RASTER_MIN_MB = Number(
  process.env.SYSTEM_FORCE_RASTER_MIN_MB || 100,
);
const SYSTEM_MAX_PARTS = Number(process.env.SYSTEM_MAX_PARTS || 5);
const SYSTEM_PART_MB = Number(process.env.SYSTEM_PART_MB || 9);
const SYSTEM_PART_BYTES = SYSTEM_PART_MB * 1024 * 1024;
// Headroom to reduce chance of any single part crossing the hard 9MB cap after overhead.
const SYSTEM_TOTAL_CAP_BYTES = Math.floor(
  SYSTEM_PART_BYTES * SYSTEM_MAX_PARTS - 512 * 1024,
);
const PREPROCESS_MODE = String(
  process.env.PREPROCESS_MODE || "ALWAYS",
).toUpperCase();

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
function remainingMsFromExpiresAt(expiresAtIso) {
  const t = Date.parse(expiresAtIso || "");
  if (!Number.isFinite(t)) return null;
  return t - Date.now();
}
function boundedTimeout(
  defaultMs,
  expiresAtIso,
  floorMs = 20_000,
  reserveMs = 60_000,
) {
  const remaining = remainingMsFromExpiresAt(expiresAtIso);
  if (remaining == null) return defaultMs;
  const maxAllowed = remaining - reserveMs;
  return Math.max(floorMs, Math.min(defaultMs, maxAllowed));
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
      if (killed) {
        const e = new Error(`${cmd} timeout after ${timeoutMs}ms`);
        e.code = "CMD_TIMEOUT";
        return reject(e);
      }
      if (code !== 0) {
        const e = new Error(`${cmd} failed code=${code}\n${err || out}`);
        e.code = "CMD_FAILED";
        return reject(e);
      }
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

async function fetchQueue(limit = 1) {
  const { data, error } = await supabase
    .from("jobs")
    .select(
      "id,input_path,split_mb,status,created_at,file_name,file_size_bytes,claimed_by,ttl_minutes,expires_at",
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

  const { data, error } = await supabase
    .from("jobs")
    .select(
      "id,input_path,output_zip_path,zip_path,expires_at,cleaned_at,status",
    )
    .is("cleaned_at", null)
    .not("expires_at", "is", null)
    .lt("expires_at", nowIso())
    .limit(50);

  if (error) throw error;

  const rows = data || [];
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
// PREFLIGHT #1 — FAST-ONLY (no qpdf --json)
// ----------------------
async function preflight1_fastOnly(inPdf, expiresAtIso) {
  const inBytes = safeStatSize(inPdf) ?? 0;

  const tQ = Math.min(
    boundedTimeout(TIMEOUT_QPDF_MS, expiresAtIso, 10_000, 60_000),
    25_000,
  );

  const { out: npagesOut } = await runCmd(
    QPDF_EXE,
    ["--show-npages", inPdf],
    tQ,
  );
  const pages = Number(String(npagesOut).trim());
  if (!Number.isFinite(pages) || pages <= 0) {
    const e = new Error("Could not read page count");
    e.code = "NPAGES_FAILED";
    throw e;
  }

  const avgBytesPerPage = inBytes / Math.max(1, pages);

  // Simple, fast heuristic to decide whether we should prefer FAST tiers later.
  // (No job-mode logic belongs in preflight.)
  let profile = "QUALITY";
  if (inBytes >= 50 * 1024 * 1024 || avgBytesPerPage >= 0.8 * 1024 * 1024) {
    profile = "FAST";
  }

  return { profile, pages, inBytes };
}

// ----------------------
// Heuristic: detect scan-heavy PDFs (image-dominant)
// Best-effort classifier used only to pick a safer GS profile.
// ----------------------
function isLikelyScanPdf({ pages, bytes }) {
  const p = Math.max(1, Number(pages || 0));
  const b = Math.max(0, Number(bytes || 0));
  const avg = b / p;
  // Typical text PDFs: ~20KB–300KB/page; scan PDFs: ~600KB–3MB+/page.
  // Conservative threshold to avoid misclassifying text PDFs.
  return p >= 2 && avg >= 700 * 1024;
}

// ----------------------
// QPDF fast recompress (very fast, stable)
// ----------------------
async function qpdfFastRecompress({ inPdf, outPdf, expiresAtIso }) {
  const tQ = boundedTimeout(TIMEOUT_QPDF_MS, expiresAtIso, 20_000, 60_000);
  // keep it conservative and compatible
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
// Ghostscript optional (may be slow/crash; always fallback)
// ----------------------
function gsArgsFastStable({ inPdf, outPdf, profile, forcePdfSettings = null }) {
  // NOTE: no ColorConversionStrategy / ProcessColorModel props -> avoids rangecheck putdeviceprops
  // IMPORTANT: /screen is forbidden (destroys legibility). We use /ebook as safe default.
  const pdfSettings = forcePdfSettings || "/ebook";

  return [
    "-q",
    "-dSAFER",
    "-dBATCH",
    "-dNOPAUSE",
    "-sDEVICE=pdfwrite",
    "-dCompatibilityLevel=1.4",
    `-dPDFSETTINGS=${pdfSettings}`,
    "-dDetectDuplicateImages=true",
    "-dAutoRotatePages=/None",
    "-dCompressFonts=true",
    "-dSubsetFonts=true",
    "-dEmbedAllFonts=true",
    `-sOutputFile=${outPdf}`,
    inPdf,
  ];
}

function gsArgsScanLegible({
  inPdf,
  outPdf,
  colorDpi = 200,
  monoDpi = 300,
  jpegQ = 75,
}) {
  // Scan-friendly: keep text legible while shrinking images.
  // Floors: color/gray >=170 dpi, mono >=300 dpi, JPEGQ >=70.
  const cd = Math.max(170, Math.min(240, Math.floor(colorDpi)));
  const md = Math.max(300, Math.min(600, Math.floor(monoDpi)));
  const jq = Math.max(70, Math.min(85, Math.floor(jpegQ)));

  return [
    "-q",
    "-dSAFER",
    "-dBATCH",
    "-dNOPAUSE",
    "-sDEVICE=pdfwrite",
    "-dCompatibilityLevel=1.4",
    // Baseline + explicit overrides for scan.
    "-dPDFSETTINGS=/ebook",
    "-dDetectDuplicateImages=true",
    "-dAutoRotatePages=/None",
    "-dCompressFonts=true",
    "-dSubsetFonts=true",
    "-dEmbedAllFonts=true",

    // Image handling
    "-dDownsampleColorImages=true",
    "-dDownsampleGrayImages=true",
    "-dDownsampleMonoImages=true",
    "-dColorImageDownsampleType=/Bicubic",
    "-dGrayImageDownsampleType=/Bicubic",
    "-dMonoImageDownsampleType=/Subsample",
    `-dColorImageResolution=${cd}`,
    `-dGrayImageResolution=${cd}`,
    `-dMonoImageResolution=${md}`,
    "-dAutoFilterColorImages=false",
    "-dAutoFilterGrayImages=false",
    "-dColorImageFilter=/DCTEncode",
    "-dGrayImageFilter=/DCTEncode",
    `-dJPEGQ=${jq}`,

    `-sOutputFile=${outPdf}`,
    inPdf,
  ];
}

// Scan tier (aggressive). Used for System-fit and/or when we must hit <=5×9MB.
function gsArgsScanTier({
  inPdf,
  outPdf,
  colorDpi = 150,
  monoDpi = 240,
  jpegQ = 60,
  pdfSettings = "/ebook",
}) {
  const cd = Math.max(90, Math.min(240, Math.floor(colorDpi)));
  const md = Math.max(180, Math.min(600, Math.floor(monoDpi)));
  const jq = Math.max(30, Math.min(85, Math.floor(jpegQ)));

  // We avoid the GS built-in /screen profile unless asked; pdfSettings is explicit per tier.
  return [
    "-q",
    "-dSAFER",
    "-dBATCH",
    "-dNOPAUSE",
    "-sDEVICE=pdfwrite",
    "-dCompatibilityLevel=1.4",
    "-dAutoRotatePages=/None",
    "-dDetectDuplicateImages=true",
    "-dDownsampleColorImages=true",
    "-dDownsampleGrayImages=true",
    "-dDownsampleMonoImages=true",
    "-dColorImageDownsampleType=/Bicubic",
    "-dGrayImageDownsampleType=/Bicubic",
    "-dMonoImageDownsampleType=/Bicubic",
    `-dColorImageResolution=${cd}`,
    `-dGrayImageResolution=${cd}`,
    `-dMonoImageResolution=${md}`,
    `-dJPEGQ=${jq}`,
    "-dSubsetFonts=true",
    "-dEmbedAllFonts=true",
    "-dCompressFonts=true",
    "-dCompressPages=true",
    "-dUseFlateCompression=true",
    `-dPDFSETTINGS=${pdfSettings}`,
    `-sOutputFile=${outPdf}`,
    inPdf,
  ];
}

// System raster profiles (high → low). We pick the first that fits under cap to keep maximum readability.
const SYSTEM_RASTER_PROFILES = [
  { name: "R1", dpi: 200, jpegQ: 82 },
  { name: "R2", dpi: 180, jpegQ: 78 },
  { name: "R3", dpi: 170, jpegQ: 74 },
  { name: "R4", dpi: 160, jpegQ: 70 },
  { name: "R5", dpi: 150, jpegQ: 65 },
  { name: "R6", dpi: 140, jpegQ: 60 },
  { name: "R7", dpi: 130, jpegQ: 55 },
  { name: "R8", dpi: 120, jpegQ: 50 },
  { name: "R9", dpi: 110, jpegQ: 45 },
];

function sortFilesNumericSuffix(files) {
  return files.slice().sort((a, b) => {
    const ra = a.match(/(\d+)(?=\.[^.]+$)/);
    const rb = b.match(/(\d+)(?=\.[^.]+$)/);
    const na = ra ? Number(ra[1]) : 0;
    const nb = rb ? Number(rb[1]) : 0;
    return na - nb;
  });
}

async function gsRenderToJpegs({ inPdf, outDir, dpi, jpegQ, expiresAtIso }) {
  fs.mkdirSync(outDir, { recursive: true });
  try {
    for (const f of fs.readdirSync(outDir)) safeUnlink(path.join(outDir, f));
  } catch {}

  const tGS = boundedTimeout(TIMEOUT_GS_MS, expiresAtIso, 20_000, 120_000);
  const outPattern = path.join(outDir, "page_%06d.jpg");
  const args = [
    "-q",
    "-dSAFER",
    "-dBATCH",
    "-dNOPAUSE",
    "-sDEVICE=jpeg",
    `-dJPEGQ=${Math.max(25, Math.min(85, Math.floor(Number(jpegQ || 60))))}`,
    `-r${Math.max(72, Math.min(300, Math.floor(Number(dpi || 150))))}`,
    `-sOutputFile=${outPattern}`,
    inPdf,
  ];
  await runCmd(GS_EXE, args, tGS);

  const jpgs = sortFilesNumericSuffix(
    fs
      .readdirSync(outDir)
      .filter((f) => f.toLowerCase().endsWith(".jpg"))
      .map((f) => path.join(outDir, f)),
  );
  if (!jpgs.length) {
    const e = new Error("RASTER_RENDER_NO_JPEGS");
    e.code = "RASTER_RENDER_NO_JPEGS";
    throw e;
  }
  return jpgs;
}

async function gsBuildPdfFromImages({ images, outPdf, expiresAtIso, wd }) {
  const tGS = boundedTimeout(TIMEOUT_GS_MS, expiresAtIso, 20_000, 180_000);

  // Use response file (@file) to avoid Windows command length limit.
  const argFile = path.join(wd, `images_${rand6()}.txt`);
  const lines = images
    .map((p) => `"${String(p).replace(/\\/g, "/")}"`)
    .join("\n");
  fs.writeFileSync(argFile, lines, "utf-8");

  const args = [
    "-q",
    "-dSAFER",
    "-dBATCH",
    "-dNOPAUSE",
    "-sDEVICE=pdfwrite",
    "-dCompatibilityLevel=1.4",
    "-dAutoRotatePages=/None",
    "-dDetectDuplicateImages=true",
    "-dPDFFitPage",
    `-sOutputFile=${outPdf}`,
    `@${argFile}`,
  ];
  await runCmd(GS_EXE, args, tGS);
  safeUnlink(argFile);

  const outBytes = safeStatSize(outPdf) ?? 0;
  if (outBytes <= 0) {
    const e = new Error("RASTER_BUILD_FAILED");
    e.code = "RASTER_BUILD_FAILED";
    throw e;
  }
  return outBytes;
}

async function systemForceRasterToCap({ inPdf, wd, capBytes, expiresAtIso }) {
  const rasterDir = path.join(wd, "raster");
  let bestPdf = null;
  let bestBytes = 0;

  for (const prof of SYSTEM_RASTER_PROFILES) {
    const jpgDir = path.join(rasterDir, prof.name);
    const images = await gsRenderToJpegs({
      inPdf,
      outDir: jpgDir,
      dpi: prof.dpi,
      jpegQ: prof.jpegQ,
      expiresAtIso,
    });

    const outPdf = path.join(wd, `rebuilt_${prof.name}.pdf`);
    const outBytes = await gsBuildPdfFromImages({
      images,
      outPdf,
      expiresAtIso,
      wd,
    });

    if (outBytes <= capBytes) {
      bestPdf = outPdf;
      bestBytes = outBytes;
      break; // first fit = best quality under cap
    }

    // keep smallest as fallback
    if (!bestPdf || outBytes < bestBytes) {
      bestPdf = outPdf;
      bestBytes = outBytes;
    }
  }

  return { pdfPath: bestPdf, bytes: bestBytes };
}

async function gsOptionalCompress({
  inPdf,
  outPdf,
  profile,
  expiresAtIso,
  forcePdfSettings = null,
  scanOptions = null,
  maxMsOverride = null,
}) {
  let tGS = boundedTimeout(TIMEOUT_GS_MS, expiresAtIso, 20_000, 60_000);
  if (typeof maxMsOverride === "number" && Number.isFinite(maxMsOverride)) {
    tGS = Math.max(10_000, Math.min(tGS, Math.floor(maxMsOverride)));
  }
  const args =
    forcePdfSettings === "SCAN_LEGIBLE"
      ? gsArgsScanLegible({ inPdf, outPdf, ...(scanOptions || {}) })
      : forcePdfSettings === "SCAN_TIER"
        ? gsArgsScanTier({ inPdf, outPdf, ...(scanOptions || {}) })
        : gsArgsFastStable({ inPdf, outPdf, profile, forcePdfSettings });
  await runCmd(GS_EXE, args, tGS);
  const outBytes = safeStatSize(outPdf) ?? 0;
  return outBytes > 0;
}

// ----------------------
// PREFLIGHT #2 — Split planning (O(1))
// ----------------------
function planRangesByAvg({
  totalPages,
  totalBytes,
  targetBytes,
  mode,
  maxPartsAim = null,
}) {
  // Plan by *desired part count* from bytes/target ("near 9MB").
  // Never inflate pages/part just to satisfy "<=5 parts" if size doesn't allow it.
  const fillRatio = mode === "MANUAL" ? 0.9 : 0.92;
  const thresholdBytes = Math.max(
    256 * 1024,
    Math.floor(targetBytes * fillRatio),
  );

  const b = Math.max(0, Number(totalBytes || 0));
  const p = Math.max(1, Number(totalPages || 0));

  const desiredBySize = Math.max(1, Math.ceil(b / thresholdBytes));

  const aim =
    maxPartsAim && Number.isFinite(maxPartsAim)
      ? Math.max(1, Math.floor(maxPartsAim))
      : null;
  const canFitAim = aim ? b <= aim * thresholdBytes * 1.05 : false;
  const desiredParts = canFitAim ? Math.min(desiredBySize, aim) : desiredBySize;

  const pagesPerPart = Math.max(1, Math.ceil(p / desiredParts));

  const ranges = [];
  let start = 1;
  while (start <= p) {
    const end = Math.min(p, start + pagesPerPart - 1);
    ranges.push({ start, end });
    start = end + 1;
  }
  return ranges;
}

// ----------------------
// SPLIT — single pass extraction (one qpdf per range)
// ----------------------
async function splitSinglePass({ inPdf, partsDir, ranges, expiresAtIso }) {
  const tEach = boundedTimeout(TIMEOUT_QPDF_MS, expiresAtIso, 15_000, 60_000);
  const tmpParts = [];

  const tmpName = (i, s, e) =>
    path.join(
      partsDir,
      `.__range_${String(i + 1).padStart(3, "0")}_${s}_${e}_${rand6()}.pdf`,
    );

  for (let i = 0; i < ranges.length; i++) {
    const { start, end } = ranges[i];
    const outPath = tmpName(i, start, end);

    await runCmd(
      QPDF_EXE,
      ["--empty", "--pages", inPdf, `${start}-${end}`, "--", outPath],
      tEach,
    );

    const b = safeStatSize(outPath) ?? 0;
    tmpParts.push({ tmpPath: outPath, start, end, bytes: b });
  }

  const totalParts = tmpParts.length;
  const partFiles = [];
  const partMeta = [];

  for (let idx = 0; idx < tmpParts.length; idx++) {
    const p = tmpParts[idx];
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

  // cleanup
  try {
    for (const f of fs.readdirSync(partsDir)) {
      if (f.startsWith(".__range_")) safeUnlink(path.join(partsDir, f));
    }
  } catch {}

  return { partFiles, partMeta };
}

// ----------------------
// SYSTEM (100MB+) — Chunk compress to <=5×<=9MB parts (speed path)
// Strategy: split into <=5 page-chunks, compress each chunk to ~9MB with 1-pass ratio tuning
// + up to 1 fallback, then (optional) merge adjacent small parts to reduce file count.
// ----------------------
function buildSystemChunkRanges(totalPages) {
  const p = Math.max(1, Number(totalPages || 0));
  const chunkSize = Math.max(1, Math.ceil(p / SYSTEM_MAX_PARTS));
  const ranges = [];
  for (let start = 1; start <= p; start += chunkSize) {
    const end = Math.min(p, start + chunkSize - 1);
    ranges.push({ start, end });
  }
  return ranges; // length <= SYSTEM_MAX_PARTS
}

async function extractRangePdf({ inPdf, outPdf, start, end, expiresAtIso }) {
  const tQ = boundedTimeout(TIMEOUT_QPDF_MS, expiresAtIso, 15_000, 60_000);
  await runCmd(
    QPDF_EXE,
    ["--empty", "--pages", inPdf, `${start}-${end}`, "--", outPdf],
    tQ,
  );
  const b = safeStatSize(outPdf) ?? 0;
  if (b <= 0) {
    const e = new Error("CHUNK_EXTRACT_FAILED");
    e.code = "CHUNK_EXTRACT_FAILED";
    throw e;
  }
  return b;
}

async function systemChunkCompressToParts({
  inPdf,
  partsDir,
  totalPages,
  expiresAtIso,
  wd,
}) {
  const CAP = SYSTEM_PART_BYTES;
  const TARGET = Math.max(256 * 1024, CAP - 180 * 1024); // keep headroom; stay near 9MB
  const ranges = buildSystemChunkRanges(totalPages);
  const tmpOuts = [];

  const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));

  function pickParamsFromRatio(ratio) {
    // image size ~ dpi^2  => dpi ≈ baseDpi * sqrt(ratio)
    const baseDpi = 150; // speed-first, still readable
    const baseMono = 220;
    const baseQ = 68;

    const r = clamp(ratio, 0.02, 1.0);
    const dpi = clamp(Math.round(baseDpi * Math.sqrt(r)), 72, baseDpi);
    const mono = clamp(Math.round(baseMono * Math.sqrt(r)), 170, baseMono);
    const q = clamp(Math.round(baseQ * (0.55 + 0.45 * r)), 32, baseQ);

    // Avoid /screen unless we must; /ebook is more stable for readability.
    const pdfSettings = dpi >= 110 ? "/ebook" : "/screen";
    return { colorDpi: dpi, monoDpi: mono, jpegQ: q, pdfSettings };
  }

  async function compressOneChunk(extractedPdf, outPrefix) {
    const inB = safeStatSize(extractedPdf) ?? 0;
    if (inB <= 0) {
      const e = new Error("CHUNK_EXTRACT_EMPTY");
      e.code = "CHUNK_EXTRACT_EMPTY";
      throw e;
    }

    const ratio = TARGET / Math.max(1, inB);
    let params = pickParamsFromRatio(ratio);

    let bestUnder = null;

    const tryOnce = async (tag, p, maxMs) => {
      const outPdf = path.join(wd, `${outPrefix}_${tag}.pdf`);
      try {
        const ok = await gsOptionalCompress({
          inPdf: extractedPdf,
          outPdf,
          profile: "FAST",
          expiresAtIso,
          forcePdfSettings: "SCAN_TIER",
          maxMsOverride: maxMs,
          scanOptions: {
            colorDpi: p.colorDpi,
            monoDpi: p.monoDpi,
            jpegQ: p.jpegQ,
            pdfSettings: p.pdfSettings,
          },
        });
        const outB = ok ? (safeStatSize(outPdf) ?? 0) : 0;
        if (outB > 0 && outB <= CAP) {
          if (!bestUnder || outB > bestUnder.bytes)
            bestUnder = { pdf: outPdf, bytes: outB };
        }
        return outB;
      } catch {
        return 0;
      }
    };

    const b1 = await tryOnce("P1", params, 35_000);

    if (!bestUnder && b1 > CAP) {
      // One fast fallback only: nudge down based on feedback
      const ratio2 = CAP / Math.max(1, b1);
      params = pickParamsFromRatio(ratio * ratio2);
      params.colorDpi = clamp(Math.floor(params.colorDpi * 0.9), 72, 150);
      params.monoDpi = clamp(Math.floor(params.monoDpi * 0.9), 170, 220);
      params.jpegQ = clamp(Math.floor(params.jpegQ - 6), 28, 85);
      await tryOnce("P2", params, 25_000);
    }

    if (!bestUnder) {
      const e = new Error("SYSTEM_PART_OVER_9MB");
      e.code = "SYSTEM_PART_OVER_9MB";
      throw e;
    }

    return bestUnder;
  }

  for (let i = 0; i < ranges.length; i++) {
    const { start, end } = ranges[i];
    const extracted = path.join(
      wd,
      `sys_chunk_${String(i + 1).padStart(2, "0")}_${start}_${end}.pdf`,
    );

    await extractRangePdf({
      inPdf,
      outPdf: extracted,
      start,
      end,
      expiresAtIso,
    });

    const best = await compressOneChunk(
      extracted,
      `sys_chunk_${String(i + 1).padStart(2, "0")}`,
    );

    tmpOuts.push({ tmpPath: best.pdf, bytes: best.bytes, start, end });
  }

  // Write final names into partsDir
  const totalParts = tmpOuts.length;
  const partFiles = [];
  const partMeta = [];

  for (let idx = 0; idx < tmpOuts.length; idx++) {
    const p = tmpOuts[idx];
    const outName = `goodPDF-${totalParts}(${idx + 1}).pdf`;
    const outPath = path.join(partsDir, outName);
    fs.copyFileSync(p.tmpPath, outPath);
    partFiles.push(outPath);
    partMeta.push({
      name: path.basename(outPath),
      bytes: p.bytes,
      sizeMb: Math.round(bytesToMb(p.bytes) * 10) / 10,
      startPageIndex: p.start,
      endPageIndex: p.end,
    });
  }

  return { partFiles, partMeta };
}

// SYSTEM post-pass: merge adjacent parts when possible (fast qpdf concat)
async function systemMergeAdjacentParts({
  partsDir,
  partFiles,
  partMeta,
  expiresAtIso,
  wd,
}) {
  const CAP = SYSTEM_PART_BYTES;
  const MERGE_CAP = Math.max(256 * 1024, CAP - 160 * 1024);
  const tQ = boundedTimeout(TIMEOUT_QPDF_MS, expiresAtIso, 15_000, 60_000);

  const items = partFiles.map((f, i) => ({ file: f, meta: partMeta[i] }));
  const merged = [];

  async function qpdfConcat(aFile, bFile, outFile) {
    await runCmd(
      QPDF_EXE,
      ["--empty", "--pages", aFile, bFile, "--", outFile],
      tQ,
    );
    const b = safeStatSize(outFile) ?? 0;
    if (b <= 0) {
      const e = new Error("QPDF_MERGE_FAILED");
      e.code = "QPDF_MERGE_FAILED";
      throw e;
    }
    return b;
  }

  let i = 0;
  while (i < items.length) {
    let cur = items[i];
    i++;

    while (i < items.length) {
      const nxt = items[i];
      const est = Number(cur?.meta?.bytes || 0) + Number(nxt?.meta?.bytes || 0);
      if (est > MERGE_CAP) break;

      const outTmp = path.join(wd, `.__merge_${rand6()}.pdf`);
      const outBytes = await qpdfConcat(cur.file, nxt.file, outTmp);
      if (outBytes > MERGE_CAP) {
        safeUnlink(outTmp);
        break;
      }

      safeUnlink(cur.file);
      safeUnlink(nxt.file);

      cur = {
        file: outTmp,
        meta: {
          name: path.basename(outTmp),
          bytes: outBytes,
          sizeMb: Math.round(bytesToMb(outBytes) * 10) / 10,
          startPageIndex: cur.meta.startPageIndex,
          endPageIndex: nxt.meta.endPageIndex,
        },
      };
      i++;
    }

    merged.push(cur);
  }

  if (merged.length === items.length) return { partFiles, partMeta };

  // wipe existing PDFs in partsDir
  try {
    for (const f of fs.readdirSync(partsDir)) {
      if (f.toLowerCase().endsWith(".pdf")) safeUnlink(path.join(partsDir, f));
    }
  } catch {}

  const totalParts = merged.length;
  const outFiles = [];
  const outMeta = [];

  for (let idx = 0; idx < merged.length; idx++) {
    const m = merged[idx];
    const outName = `goodPDF-${totalParts}(${idx + 1}).pdf`;
    const outPath = path.join(partsDir, outName);

    try {
      fs.renameSync(m.file, outPath);
    } catch {
      fs.copyFileSync(m.file, outPath);
      safeUnlink(m.file);
    }

    const b = safeStatSize(outPath) ?? Number(m?.meta?.bytes || 0);
    outFiles.push(outPath);
    outMeta.push({
      name: path.basename(outPath),
      bytes: b,
      sizeMb: Math.round(bytesToMb(b) * 10) / 10,
      startPageIndex: m.meta.startPageIndex,
      endPageIndex: m.meta.endPageIndex,
    });
  }

  if (outMeta.some((p) => Number(p.bytes || 0) > CAP)) {
    const e = new Error("SYSTEM_MERGE_OVER_CAP");
    e.code = "SYSTEM_MERGE_OVER_CAP";
    throw e;
  }

  return { partFiles: outFiles, partMeta: outMeta };
}

// ----------------------
// ZIP
// ----------------------
async function zipParts(partsDir, outZipPath, timeoutMs) {
  const pdfs = fs
    .readdirSync(partsDir)
    .filter((f) => f.toLowerCase().endsWith(".pdf"))
    .sort();
  if (pdfs.length === 0) {
    const e = new Error("NO_PART_PDFS_TO_ZIP");
    e.code = "NO_PART_PDFS_TO_ZIP";
    throw e;
  }
  await runCmd(
    SEVEN_Z_EXE,
    ["a", "-tzip", "-mx=0", "-mmt=on", outZipPath, ...pdfs],
    timeoutMs,
    { cwd: partsDir },
  );
}

// ----------------------
// Process one job
// ----------------------
function tmpDir(jobId) {
  const d = path.join(os.tmpdir(), `goodpdf_${jobId}`);
  fs.mkdirSync(d, { recursive: true });
  return d;
}

async function processOneJob(job) {
  const jobId = job.id;
  const claimed = await claimJob(jobId);
  if (!claimed) return;

  const startedAt = Date.now();
  const elapsed = () => Date.now() - startedAt;
  const shouldSoftStop = () => elapsed() >= UX_SOFTSTOP_MS;

  const wd = tmpDir(jobId);
  const inPdf = path.join(wd, "input.pdf");
  const qpdfPdf = path.join(wd, "input_qpdf.pdf");
  const gsPdf = path.join(wd, "input_gs.pdf");
  const gsPdf2 = path.join(wd, "input_gs2.pdf");

  const partsDir = path.join(wd, "parts");
  fs.mkdirSync(partsDir, { recursive: true });

  const inputKey = normalizeInputKey(job);
  const outZipKey = outputZipKeyFor(job);

  // Parts list for ZIP (set by either fast system pipeline or normal split)
  let partFiles = null;
  let partMeta = null;

  try {
    await updateJob(jobId, {
      stage: "DOWNLOAD",
      progress: 5,
      split_progress: 0,
      updated_at: nowIso(),
    });
    await downloadFromR2(inputKey, inPdf);

    const inBytes = safeStatSize(inPdf) ?? 0;

    const rawSplitMb = Number(job.split_mb || 0);
    if (!Number.isFinite(rawSplitMb) || rawSplitMb <= 0) {
      const e = new Error("split_mb missing/invalid");
      e.code = "SPLIT_MB_INVALID";
      throw e;
    }

    const systemFit = rawSplitMb >= 490;
    const splitMb = systemFit ? SYSTEM_PART_MB : rawSplitMb;
    let maxPartsAim = systemFit ? SYSTEM_MAX_PARTS : null;
    const limitBytes = mbToBytes(splitMb);

    const forceRaster =
      systemFit && inBytes > SYSTEM_FORCE_RASTER_MIN_MB * 1024 * 1024;

    // PREFLIGHT_1
    await updateJob(jobId, {
      stage: "PREFLIGHT_1",
      progress: 10,
      split_progress: 3,
      updated_at: nowIso(),
    });
    const pf1 = await preflight1_fastOnly(inPdf, job.expires_at);
    const scanLikely = isLikelyScanPdf({ pages: pf1.pages, bytes: inBytes });

    // FAST SYSTEM path (100MB+): produce <=5 parts each <=9MB by per-chunk compression.
    // This avoids whole-document raster rebuild + multi-tier loops (major speed win).
    if (
      systemFit &&
      forceRaster &&
      PREPROCESS_MODE !== "OFF" &&
      !shouldSoftStop()
    ) {
      await updateJob(jobId, {
        stage: "SYS_CHUNK",
        progress: 22,
        split_progress: 8,
        updated_at: nowIso(),
      });

      const res = await systemChunkCompressToParts({
        inPdf,
        partsDir,
        totalPages: pf1.pages,
        expiresAtIso: job.expires_at,
        wd,
      });

      partFiles = res.partFiles;
      partMeta = res.partMeta;

      // Post-pass merge: reduce file count if possible while keeping each <=9MB
      try {
        const merged = await systemMergeAdjacentParts({
          partsDir,
          partFiles,
          partMeta,
          expiresAtIso: job.expires_at,
          wd,
        });
        partFiles = merged.partFiles;
        partMeta = merged.partMeta;
      } catch {
        // ignore merge errors
      }
    }

    if (!partMeta) {
      // COMPRESS (bounded by UX cap)
      await updateJob(jobId, {
        stage: "COMPRESS",
        progress: 18,
        split_progress: 6,
        updated_at: nowIso(),
      });

      let workPdf = inPdf;

      // If user wait cap is near, skip compression entirely
      if (PREPROCESS_MODE === "OFF" || shouldSoftStop()) {
        workPdf = inPdf;
      } else {
        // 1) Always do QPDF fast recompress first (fast + stable)
        try {
          const ok = await qpdfFastRecompress({
            inPdf,
            outPdf: qpdfPdf,
            expiresAtIso: job.expires_at,
          });
          if (ok) workPdf = qpdfPdf;
        } catch {
          workPdf = inPdf; // fallback
        }

        // 2) Optional Ghostscript (only if still within cap)
        // If GS fails (like your screenshot), we ignore and continue with qpdf result.
        if (!shouldSoftStop()) {
          try {
            const beforeBytes = safeStatSize(workPdf) ?? 0;

            // TEXT PDFs: /ebook only (text-safe)
            // SCAN PDFs: legible scan profile (200dpi, JPEGQ>=70) with at most one gentle retry.
            if (!scanLikely) {
              const okGs = await gsOptionalCompress({
                inPdf: workPdf,
                outPdf: gsPdf,
                profile: pf1.profile,
                expiresAtIso: job.expires_at,
                forcePdfSettings: "/ebook",
              });
              if (okGs) workPdf = gsPdf;
            } else {
              // Pass A (legible)
              const okA = await gsOptionalCompress({
                inPdf: workPdf,
                outPdf: gsPdf,
                profile: pf1.profile,
                expiresAtIso: job.expires_at,
                forcePdfSettings: "SCAN_LEGIBLE",
                scanOptions: { colorDpi: 200, monoDpi: 300, jpegQ: 78 },
              });
              let bestPdf = okA ? gsPdf : null;
              let bestBytes = okA ? (safeStatSize(gsPdf) ?? 0) : beforeBytes;

              // If GS didn't help, keep previous.
              const improvedA =
                okA && bestBytes > 0 && bestBytes <= beforeBytes * 0.97;

              // Pass B (gentle) only if we still have time AND pass A helped.
              if (!shouldSoftStop() && improvedA) {
                try {
                  const okB = await gsOptionalCompress({
                    inPdf: gsPdf,
                    outPdf: gsPdf2,
                    profile: pf1.profile,
                    expiresAtIso: job.expires_at,
                    forcePdfSettings: "SCAN_LEGIBLE",
                    scanOptions: { colorDpi: 180, monoDpi: 300, jpegQ: 74 },
                  });
                  const b2 = okB ? (safeStatSize(gsPdf2) ?? 0) : 0;
                  if (okB && b2 > 0 && b2 <= bestBytes * 0.97) {
                    bestPdf = gsPdf2;
                    bestBytes = b2;
                  }
                } catch {
                  // ignore pass B errors
                }
              }

              if (bestPdf && bestBytes > 0) workPdf = bestPdf;
            }
          } catch {
            // ignore GS crash; continue
          }
        }
      }

      // SYSTEM: ensure we can fit into <=5 parts of <=9MB.
      // This requires total <= ~45MB (with headroom). If needed, we compress further and/or force-raster rebuild.
      if (systemFit && !shouldSoftStop()) {
        const capBytes = SYSTEM_TOTAL_CAP_BYTES;

        // If huge input, prefer force-raster (predictable sizing).
        if (forceRaster) {
          try {
            await updateJob(jobId, {
              stage: "RASTER_REBUILD",
              progress: 24,
              split_progress: 8,
              updated_at: nowIso(),
            });
            const rr = await systemForceRasterToCap({
              inPdf: workPdf,
              wd,
              capBytes,
              expiresAtIso: job.expires_at,
            });
            if (rr?.pdfPath && (safeStatSize(rr.pdfPath) ?? 0) > 0)
              workPdf = rr.pdfPath;
          } catch {
            // ignore; fallback to GS
          }
        }

        // If still above cap, try aggressive scan tiers (quality-first) until we fit, then stop.
        try {
          let curBytes = safeStatSize(workPdf) ?? 0;
          if (curBytes > capBytes && !shouldSoftStop()) {
            const tiers = [
              { colorDpi: 170, monoDpi: 260, jpegQ: 75, pdfSettings: "/ebook" },
              { colorDpi: 160, monoDpi: 255, jpegQ: 70, pdfSettings: "/ebook" },
              { colorDpi: 150, monoDpi: 250, jpegQ: 65, pdfSettings: "/ebook" },
              { colorDpi: 140, monoDpi: 245, jpegQ: 60, pdfSettings: "/ebook" },
              { colorDpi: 130, monoDpi: 240, jpegQ: 55, pdfSettings: "/ebook" },
              { colorDpi: 120, monoDpi: 220, jpegQ: 50, pdfSettings: "/ebook" },
              {
                colorDpi: 110,
                monoDpi: 210,
                jpegQ: 45,
                pdfSettings: "/screen",
              },
            ];

            let bestPdf = workPdf;
            let bestBytes = curBytes;

            for (let i = 0; i < tiers.length; i++) {
              if (shouldSoftStop()) break;
              const tier = tiers[i];
              const outPdf = path.join(wd, `input_sys_tier_${i + 1}.pdf`);
              const ok = await gsOptionalCompress({
                inPdf: bestPdf,
                outPdf,
                profile: pf1.profile,
                expiresAtIso: job.expires_at,
                forcePdfSettings: "SCAN_TIER",
                scanOptions: tier,
              });
              const outBytes = ok ? (safeStatSize(outPdf) ?? 0) : 0;
              if (ok && outBytes > 0 && outBytes < bestBytes) {
                bestPdf = outPdf;
                bestBytes = outBytes;
              }
              if (outBytes > 0 && outBytes <= capBytes) break;
            }

            workPdf = bestPdf;
            curBytes = bestBytes;

            // Last resort: if still above cap, force-raster once (even if under 100MB).
            if (curBytes > capBytes && !shouldSoftStop()) {
              try {
                await updateJob(jobId, {
                  stage: "RASTER_REBUILD",
                  progress: 26,
                  split_progress: 10,
                  updated_at: nowIso(),
                });
                const rr2 = await systemForceRasterToCap({
                  inPdf: workPdf,
                  wd,
                  capBytes,
                  expiresAtIso: job.expires_at,
                });
                if (rr2?.pdfPath && (safeStatSize(rr2.pdfPath) ?? 0) > 0)
                  workPdf = rr2.pdfPath;
              } catch {
                // ignore
              }
            }
          }
        } catch {
          // ignore
        }
      }

      // If we are already past soft stop, DO NOT do extra work; go split
      if (shouldSoftStop()) {
        workPdf = inPdf; // ensure fastest path
      }

      // Read pages of workPdf
      const tQ = Math.min(
        boundedTimeout(TIMEOUT_QPDF_MS, job.expires_at, 10_000, 60_000),
        25_000,
      );
      const { out: npagesOut } = await runCmd(
        QPDF_EXE,
        ["--show-npages", workPdf],
        tQ,
      );
      const totalPages = Number(String(npagesOut).trim());
      if (!Number.isFinite(totalPages) || totalPages <= 0) {
        const e = new Error("Could not read page count");
        e.code = "NPAGES_FAILED";
        throw e;
      }

      const workBytes = safeStatSize(workPdf) ?? 0;

      // PREFLIGHT_2
      await updateJob(jobId, {
        stage: "PREFLIGHT_2",
        progress: 30,
        split_progress: 12,
        updated_at: nowIso(),
      });

      const mode = systemFit ? "DEFAULT" : "MANUAL";
      let targetBytes = limitBytes;

      // SYSTEM (<= threshold): we *aim* for <=5 parts only when the current bytes can realistically fit.
      // We NEVER break the near-9MB rule to force 5 parts.
      const effectiveMaxPartsAim = systemFit ? maxPartsAim : null;

      // For system-fit, we must guarantee: <=5 parts AND each <=9MB.
      // We will try a few split plans; if any part exceeds 9MB, we compress a bit more and retry.
      let finalPartFiles = null;
      let finalPartMeta = null;

      const maxSplitAttempts = systemFit ? 5 : 1;
      for (let attempt = 1; attempt <= maxSplitAttempts; attempt++) {
        const curBytesNow = safeStatSize(workPdf) ?? workBytes;

        const ranges = planRangesByAvg({
          totalPages,
          totalBytes: curBytesNow,
          targetBytes,
          mode,
          maxPartsAim: effectiveMaxPartsAim,
        });

        // SPLIT
        await updateJob(jobId, {
          stage: "SPLIT",
          progress: 40,
          split_progress: Math.min(20 + attempt * 2, 30),
          updated_at: nowIso(),
        });

        const { partFiles, partMeta } = await splitSinglePass({
          inPdf: workPdf,
          partsDir,
          ranges,
          expiresAtIso: job.expires_at,
        });

        const tooMany = systemFit && partFiles.length > SYSTEM_MAX_PARTS;
        const overCap =
          systemFit &&
          partMeta.some(
            (p) => Number(p?.bytes || 0) > mbToBytes(SYSTEM_PART_MB),
          );

        if (!systemFit || (!tooMany && !overCap)) {
          finalPartFiles = partFiles;
          finalPartMeta = partMeta;
          break;
        }

        // Retry path (system-fit only): compress a bit more then try again.
        // Clean parts from failed attempt.
        try {
          for (const f of partFiles) safeUnlink(f);
        } catch {}

        if (shouldSoftStop()) break;

        try {
          const tier = [
            { colorDpi: 150, monoDpi: 240, jpegQ: 60, pdfSettings: "/ebook" },
            { colorDpi: 140, monoDpi: 235, jpegQ: 58, pdfSettings: "/ebook" },
            { colorDpi: 130, monoDpi: 230, jpegQ: 55, pdfSettings: "/ebook" },
            { colorDpi: 120, monoDpi: 220, jpegQ: 50, pdfSettings: "/ebook" },
            { colorDpi: 110, monoDpi: 210, jpegQ: 45, pdfSettings: "/screen" },
          ][Math.min(attempt - 1, 4)];

          const outPdf = path.join(wd, `input_sys_split_retry_${attempt}.pdf`);
          const ok = await gsOptionalCompress({
            inPdf: workPdf,
            outPdf,
            profile: pf1.profile,
            expiresAtIso: job.expires_at,
            forcePdfSettings: "SCAN_TIER",
            scanOptions: tier,
          });
          if (ok && (safeStatSize(outPdf) ?? 0) > 0) {
            workPdf = outPdf;
          }
        } catch {
          // ignore and fall through
        }
      }

      if (!finalPartFiles || !finalPartMeta) {
        // If we couldn't satisfy system constraints, last resort: force-raster and one split attempt.
        if (systemFit && !shouldSoftStop()) {
          try {
            await updateJob(jobId, {
              stage: "RASTER_REBUILD",
              progress: 38,
              split_progress: 18,
              updated_at: nowIso(),
            });
            const rr3 = await systemForceRasterToCap({
              inPdf: workPdf,
              wd,
              capBytes: SYSTEM_TOTAL_CAP_BYTES,
              expiresAtIso: job.expires_at,
            });
            if (rr3?.pdfPath && (safeStatSize(rr3.pdfPath) ?? 0) > 0) {
              workPdf = rr3.pdfPath;
            }
          } catch {}
        }

        const ranges = planRangesByAvg({
          totalPages,
          totalBytes: safeStatSize(workPdf) ?? workBytes,
          targetBytes,
          mode,
          maxPartsAim: effectiveMaxPartsAim,
        });

        const res = await splitSinglePass({
          inPdf: workPdf,
          partsDir,
          ranges,
          expiresAtIso: job.expires_at,
        });

        finalPartFiles = res.partFiles;
        finalPartMeta = res.partMeta;
      }

      partFiles = finalPartFiles;
      partMeta = finalPartMeta;

      // SYSTEM post-pass merge (reduce file count if possible without breaking 9MB cap)
      if (
        systemFit &&
        Array.isArray(partFiles) &&
        Array.isArray(partMeta) &&
        partFiles.length > 1
      ) {
        try {
          const merged = await systemMergeAdjacentParts({
            partsDir,
            partFiles,
            partMeta,
            expiresAtIso: job.expires_at,
            wd,
          });
          if (merged?.partFiles?.length) partFiles = merged.partFiles;
          if (merged?.partMeta?.length) partMeta = merged.partMeta;
        } catch {
          // ignore merge errors
        }
      }

      // ZIP
    }

    await updateJob(jobId, {
      stage: "ZIP",
      progress: 70,
      split_progress: 90,
      updated_at: nowIso(),
    });
    const outZip = path.join(wd, "goodpdf.zip");
    const timeoutZip = boundedTimeout(
      TIMEOUT_7Z_MS,
      job.expires_at,
      30_000,
      60_000,
    );
    await zipParts(partsDir, outZip, timeoutZip);

    // UPLOAD
    await updateJob(jobId, {
      stage: "UPLOAD_OUT",
      progress: 85,
      split_progress: 95,
      updated_at: nowIso(),
    });
    await uploadToR2(outZipKey, outZip, "application/zip");

    // stats + expiry
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
      Date.now() + ttlMinutes * 60_000,
    ).toISOString();

    await updateJob(jobId, {
      status: "DONE",
      stage: "DONE",
      progress: 100,
      split_progress: 100,
      output_zip_path: outZipKey,
      zip_path: outZipKey,
      ttl_minutes: ttlMinutes,
      expires_at: deleteAtIso,
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
    });
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
    PREPROCESS_MODE,
    UX_CAP_MS,
    UX_SOFTSTOP_MS,
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
