import { NextResponse } from "next/server";
import { supabaseAdmin } from "@/lib/supabase/admin";
import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { checkRateLimit, rateLimitResponse } from "@/lib/rate-limit";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const revalidate = 0;

const execFileAsync = promisify(execFile);
const QPDF_EXE = process.env.QPDF_EXE || "qpdf";
const R2_ENDPOINT = process.env.R2_ENDPOINT!;
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID!;
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY!;
const R2_BUCKET_IN = process.env.R2_BUCKET_IN || "goodpdf-in";

const r2 = new S3Client({
  region: "auto",
  endpoint: R2_ENDPOINT,
  credentials: {
    accessKeyId: R2_ACCESS_KEY_ID,
    secretAccessKey: R2_SECRET_ACCESS_KEY,
  },
  forcePathStyle: true,
});

function json(ok: boolean, data?: any, error?: string, status = 200) {
  return NextResponse.json(
    { ok, data, error },
    { status, headers: { "cache-control": "no-store" } }
  );
}

async function downloadInputToTemp(inputKey: string) {
  const tmpRoot = await fs.promises.mkdtemp(path.join(os.tmpdir(), "goodpdf_precheck_"));
  const tmpFile = path.join(tmpRoot, "input.pdf");
  const out = fs.createWriteStream(tmpFile);
  const obj = await r2.send(new GetObjectCommand({ Bucket: R2_BUCKET_IN, Key: inputKey }));
  const body = obj.Body as NodeJS.ReadableStream | undefined;
  if (!body) throw new Error("Missing input stream");

  await new Promise<void>((resolve, reject) => {
    body.on("error", reject);
    out.on("error", reject);
    out.on("finish", () => resolve());
    body.pipe(out);
  });

  return { tmpRoot, tmpFile };
}

async function readPagesWithQpdf(pdfPath: string): Promise<number | null> {
  try {
    const { stdout } = await execFileAsync(QPDF_EXE, ["--show-npages", pdfPath], {
      timeout: 25_000,
      windowsHide: true,
    });
    const pages = Number(String(stdout || "").trim());
    if (!Number.isFinite(pages) || pages <= 0) return null;
    return Math.floor(pages);
  } catch {
    return null;
  }
}

type PdfSignals = {
  sampledBytes: number;
  imageRefs: number;
  jpegRefs: number;
  jpxRefs: number;
  softMaskRefs: number;
};

function countMatches(haystack: string, re: RegExp) {
  const m = haystack.match(re);
  return m ? m.length : 0;
}

async function samplePdfSignals(pdfPath: string): Promise<PdfSignals> {
  const st = await fs.promises.stat(pdfPath);
  const size = st.size;
  if (!Number.isFinite(size) || size <= 0) {
    return {
      sampledBytes: 0,
      imageRefs: 0,
      jpegRefs: 0,
      jpxRefs: 0,
      softMaskRefs: 0,
    };
  }

  const SAMPLE_CHUNK = 2 * 1024 * 1024;
  const points = [0, Math.max(0, Math.floor(size / 2) - Math.floor(SAMPLE_CHUNK / 2)), Math.max(0, size - SAMPLE_CHUNK)];
  const fh = await fs.promises.open(pdfPath, "r");
  try {
    let sampledBytes = 0;
    let imageRefs = 0;
    let jpegRefs = 0;
    let jpxRefs = 0;
    let softMaskRefs = 0;

    for (const pos of points) {
      const len = Math.min(SAMPLE_CHUNK, Math.max(0, size - pos));
      if (len <= 0) continue;
      const buf = Buffer.allocUnsafe(len);
      const r = await fh.read(buf, 0, len, pos);
      if (r.bytesRead <= 0) continue;
      sampledBytes += r.bytesRead;
      const text = buf.subarray(0, r.bytesRead).toString("latin1");
      imageRefs += countMatches(text, /\/Subtype\s*\/Image/g);
      jpegRefs += countMatches(text, /\/DCTDecode\b/g);
      jpxRefs += countMatches(text, /\/JPXDecode\b/g);
      softMaskRefs += countMatches(text, /\/SMask\b/g);
    }

    return { sampledBytes, imageRefs, jpegRefs, jpxRefs, softMaskRefs };
  } finally {
    await fh.close();
  }
}

function clampNum(n: number, lo: number, hi: number) {
  return Math.max(lo, Math.min(hi, n));
}

function estimateProfile(
  fileSizeMb: number,
  pages: number | null,
  sig: PdfSignals,
  calibrationFactor: number
) {
  const avgMbPerPage =
    typeof pages === "number" && pages > 0
      ? Math.round((fileSizeMb / pages) * 100) / 100
      : null;

  const sampleMb = sig.sampledBytes > 0 ? sig.sampledBytes / (1024 * 1024) : 0;
  const imgPerSampleMb = sampleMb > 0 ? sig.imageRefs / sampleMb : 0;
  const jpxPerSampleMb = sampleMb > 0 ? sig.jpxRefs / sampleMb : 0;
  const imageDensityScore = Math.min(10, imgPerSampleMb / 2.4);
  const jpxDensityScore = Math.min(10, jpxPerSampleMb * 2.8);
  const avgPerPageScore = avgMbPerPage != null ? Math.min(10, Math.max(0, (avgMbPerPage - 0.35) * 2.6)) : 0;
  const sizeScore = Math.min(10, fileSizeMb / 16);
  const pageScore = pages != null ? Math.min(10, pages / 30) : 2.2;

  // Weighted complexity score tuned for fewer false "heavy" positives.
  const complexity =
    sizeScore * 0.36 +
    avgPerPageScore * 0.24 +
    imageDensityScore * 0.22 +
    jpxDensityScore * 0.12 +
    pageScore * 0.06;

  const estimatedCpuMinRaw = Math.max(
    1,
    Math.round((0.9 + complexity * 0.95 + Math.max(0, sig.softMaskRefs - 4) * 0.08) * 10) / 10
  );
  const estimatedCpuMin = Math.max(1, Math.round(estimatedCpuMinRaw * calibrationFactor * 10) / 10);

  let tokenCost: 1 | 2 | 3 = 1;
  let mode: "NORMAL" | "HEAVY" | "EXTREME" = "NORMAL";
  let etaMinLow = Math.max(2, Math.floor(estimatedCpuMin * 0.9));
  let etaMinHigh = Math.max(3, Math.ceil(estimatedCpuMin * 1.6));
  const reason: string[] = [];

  if (estimatedCpuMin >= 6) {
    tokenCost = 2;
    mode = "HEAVY";
    etaMinLow = Math.max(etaMinLow, 5);
    etaMinHigh = Math.max(etaMinHigh, 9);
  }
  if (
    estimatedCpuMin >= 10 ||
    (avgMbPerPage != null && avgMbPerPage >= 1.6 && (sig.jpxRefs >= 2 || imgPerSampleMb >= 3))
  ) {
    tokenCost = 3;
    mode = "EXTREME";
    etaMinLow = Math.max(etaMinLow, 8);
    etaMinHigh = Math.max(etaMinHigh, 14);
  }

  // Guardrails: avoid undercharging obvious heavy-image workloads.
  if (
    etaMinHigh >= 8 ||
    (avgMbPerPage != null && avgMbPerPage >= 1.0) ||
    imgPerSampleMb >= 2.5
  ) {
    tokenCost = Math.max(tokenCost, 2) as 1 | 2 | 3;
    if (tokenCost >= 2) mode = "HEAVY";
    etaMinLow = Math.max(etaMinLow, 5);
    etaMinHigh = Math.max(etaMinHigh, 9);
  }
  if (fileSizeMb <= 30 && pages != null && pages >= 25 && avgMbPerPage != null && avgMbPerPage <= 1.2) {
    // Small/medium files with moderate page density should stay affordable by default.
    tokenCost = Math.min(tokenCost, 1) as 1 | 2 | 3;
    mode = "NORMAL";
    etaMinLow = Math.min(etaMinLow, 4);
    etaMinHigh = Math.min(etaMinHigh, 8);
  }
  if (
    etaMinHigh >= 12 ||
    (avgMbPerPage != null &&
      avgMbPerPage >= 1.4 &&
      (sig.jpxRefs >= 1 || imgPerSampleMb >= 3.2))
  ) {
    tokenCost = 3;
    mode = "EXTREME";
    etaMinLow = Math.max(etaMinLow, 8);
    etaMinHigh = Math.max(etaMinHigh, 14);
  }

  if (tokenCost === 1) reason.push("Fast path: low complexity (cost-saving)");
  if (fileSizeMb >= 45) reason.push("Large input file");
  if (avgMbPerPage != null && avgMbPerPage >= 0.9) reason.push("High MB per page");
  if (imgPerSampleMb >= 2.5) reason.push("Image-heavy page content");
  if (sig.jpxRefs >= 2) reason.push("High-cost image codec detected");
  if (reason.length === 0) reason.push("Standard optimization path");

  const recommendation =
    tokenCost === 1
      ? "Cost saver: run standard optimization."
      : tokenCost === 2
      ? "Balanced: moderate CPU workload expected."
      : "Quality-safe heavy path recommended for large images.";

  const etaSpan = Math.max(1, etaMinHigh - etaMinLow);
  const baseConfidence =
    pages == null
      ? "MEDIUM"
      : etaSpan <= 2
      ? "HIGH"
      : etaSpan <= 4
      ? "MEDIUM"
      : "LOW";
  const confidence =
    tokenCost === 3 && baseConfidence === "HIGH"
      ? "MEDIUM"
      : baseConfidence;
  const confidenceNote =
    confidence === "HIGH"
      ? "High confidence: pattern matches prior calibrated runs."
      : confidence === "MEDIUM"
      ? "Medium confidence: estimate may shift with content mix."
      : "Low confidence: unusual PDF structure can vary processing time.";

  return {
    tokenCost,
    mode,
    etaMinLow,
    etaMinHigh,
    estimatedCpuMinRaw,
    avgMbPerPage,
    estimatedCpuMin,
    confidence,
    confidenceNote,
    recommendation,
    reason,
  };
}

function safeRmDir(p: string) {
  try {
    fs.rmSync(p, { recursive: true, force: true });
  } catch {}
}

export async function POST(req: Request) {
  let tmpRoot: string | null = null;
  try {
    const rl = checkRateLimit(req, { key: "jobs:precheck", limit: 20, windowMs: 60_000 });
    if (!rl.ok) return rateLimitResponse(rl.retryAfterSec);

    const body = await req.json().catch(() => null);
    const jobId = String(body?.jobId || "").trim();
    const ownerToken = String(req.headers.get("x-owner-token") || "").trim();
    const calibrationRaw = Number(req.headers.get("x-precheck-calibration") || "1");
    const calibrationFactor = Number.isFinite(calibrationRaw)
      ? clampNum(calibrationRaw, 0.65, 2.2)
      : 1;
    if (!jobId) return json(false, null, "Missing jobId", 400);
    if (!ownerToken) return json(false, null, "Forbidden", 403);

    const { data: job, error } = await supabaseAdmin
      .from("jobs")
      .select("id,status,input_path,file_size_bytes")
      .eq("id", jobId)
      .eq("owner_token", ownerToken)
      .maybeSingle();
    if (error) return json(false, null, "Failed to read job", 500);
    if (!job) return json(false, null, "Forbidden", 403);

    const status = String(job.status || "").toUpperCase();
    if (status !== "UPLOADED" && status !== "QUEUED" && status !== "PROCESSING") {
      return json(false, null, `Invalid status for precheck: ${status || "UNKNOWN"}`, 409);
    }

    const inputKey = String(job.input_path || "").trim();
    if (!inputKey) return json(false, null, "Missing input path", 400);

    const sizeBytes = Number(job.file_size_bytes || 0);
    const fileSizeMb = Math.max(0, Math.round((sizeBytes / (1024 * 1024)) * 100) / 100);

    const downloaded = await downloadInputToTemp(inputKey);
    tmpRoot = downloaded.tmpRoot;
    const [pages, sig] = await Promise.all([
      readPagesWithQpdf(downloaded.tmpFile),
      samplePdfSignals(downloaded.tmpFile),
    ]);

    const p = estimateProfile(fileSizeMb, pages, sig, calibrationFactor);
    return json(true, {
      tokenCost: p.tokenCost,
      mode: p.mode,
      etaMinLow: p.etaMinLow,
      etaMinHigh: p.etaMinHigh,
      estimatedCpuMinRaw: p.estimatedCpuMinRaw,
      estimatedCpuMin: p.estimatedCpuMin,
      fileSizeMb,
      pages,
      avgMbPerPage: p.avgMbPerPage,
      calibrationFactor,
      confidence: p.confidence,
      confidenceNote: p.confidenceNote,
      recommendation: p.recommendation,
      reason: p.reason,
    });
  } catch {
    return json(false, null, "Precheck failed", 500);
  } finally {
    if (tmpRoot) safeRmDir(tmpRoot);
  }
}
