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

function estimateProfile(fileSizeMb: number, pages: number | null) {
  const avgMbPerPage =
    typeof pages === "number" && pages > 0
      ? Math.round((fileSizeMb / pages) * 100) / 100
      : null;

  let tokenCost: 1 | 2 | 3 = 1;
  let mode: "NORMAL" | "HEAVY" | "EXTREME" = "NORMAL";
  let etaMinLow = 2;
  let etaMinHigh = 4;
  const reason: string[] = [];

  if (fileSizeMb >= 25) {
    tokenCost = 2;
    mode = "HEAVY";
    etaMinLow = 4;
    etaMinHigh = 8;
    reason.push("Large input size");
  }
  if (avgMbPerPage != null && avgMbPerPage >= 0.8) {
    tokenCost = Math.max(tokenCost, 2) as 1 | 2 | 3;
    if (mode === "NORMAL") mode = "HEAVY";
    etaMinLow = Math.max(etaMinLow, 5);
    etaMinHigh = Math.max(etaMinHigh, 9);
    reason.push("Image-heavy pages detected");
  }
  if (
    fileSizeMb >= 60 ||
    (avgMbPerPage != null && avgMbPerPage >= 1.5) ||
    (fileSizeMb >= 40 && (pages == null || pages <= 30))
  ) {
    tokenCost = 3;
    mode = "EXTREME";
    etaMinLow = 8;
    etaMinHigh = 14;
    reason.push("Very heavy pages likely");
  }

  if (reason.length === 0) reason.push("Standard optimization path");

  return { tokenCost, mode, etaMinLow, etaMinHigh, avgMbPerPage, reason };
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
    const pages = await readPagesWithQpdf(downloaded.tmpFile);

    const p = estimateProfile(fileSizeMb, pages);
    return json(true, {
      tokenCost: p.tokenCost,
      mode: p.mode,
      etaMinLow: p.etaMinLow,
      etaMinHigh: p.etaMinHigh,
      fileSizeMb,
      pages,
      avgMbPerPage: p.avgMbPerPage,
      reason: p.reason,
    });
  } catch {
    return json(false, null, "Precheck failed", 500);
  } finally {
    if (tmpRoot) safeRmDir(tmpRoot);
  }
}
