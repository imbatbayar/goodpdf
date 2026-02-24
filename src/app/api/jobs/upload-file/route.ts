import { NextResponse } from "next/server";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { supabaseServer } from "@/lib/supabase/server";
import { checkRateLimit, rateLimitResponse } from "@/lib/rate-limit";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const revalidate = 0;

function json(ok: boolean, data?: any, error?: string, status = 200) {
  return NextResponse.json({ ok, data, error }, { status });
}

function localIngestDir(jobId: string) {
  return path.join(os.tmpdir(), "goodpdf_ingest", jobId);
}

const R2_ENDPOINT = process.env.R2_ENDPOINT!;
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID!;
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY!;
const R2_BUCKET_IN = process.env.R2_BUCKET_IN || "goodpdf-in";
const MAX_UPLOAD_BYTES = 500 * 1024 * 1024;
const ENABLE_LOCAL_INGEST_FAST_PATH =
  String(process.env.ENABLE_LOCAL_INGEST_FAST_PATH || "false").toLowerCase() ===
  "true";

if (!R2_ENDPOINT || !R2_ACCESS_KEY_ID || !R2_SECRET_ACCESS_KEY) {
  throw new Error("Missing R2 env vars");
}

const r2 = new S3Client({
  region: "auto",
  endpoint: R2_ENDPOINT,
  credentials: {
    accessKeyId: R2_ACCESS_KEY_ID,
    secretAccessKey: R2_SECRET_ACCESS_KEY,
  },
  forcePathStyle: true,
});

export async function POST(req: Request) {
  try {
    const rl = checkRateLimit(req, { key: "jobs:upload-file", limit: 12, windowMs: 60_000 });
    if (!rl.ok) return rateLimitResponse(rl.retryAfterSec);

    const { searchParams } = new URL(req.url);
    const jobId = String(searchParams.get("jobId") || "").trim();
    const ownerToken = String(req.headers.get("x-owner-token") || "").trim();
    if (!jobId) return json(false, null, "Missing jobId", 400);
    if (!ownerToken) return json(false, null, "Forbidden", 403);

    const form = await req.formData();
    const file = form.get("file");

    if (!file || !(file instanceof File)) {
      return json(false, null, "Missing file (form-data field name must be 'file')", 400);
    }
    if (file.size <= 0) return json(false, null, "Empty file", 400);
    if (file.size > MAX_UPLOAD_BYTES) {
      return json(false, null, "File is too large. Maximum is 500MB.", 413);
    }
    const lowerName = String(file.name || "").toLowerCase();
    const hasPdfMime = file.type === "application/pdf";
    const hasPdfExt = lowerName.endsWith(".pdf");
    if (!hasPdfMime && !hasPdfExt) {
      return json(false, null, "Only PDF files are supported.", 400);
    }

    const { data: job, error: selErr } = await supabaseServer
      .from("jobs")
      .select("id,status,owner_token")
      .eq("id", jobId)
      .eq("owner_token", ownerToken)
      .maybeSingle();

    if (selErr) return json(false, null, "Failed to validate job", 500);
    if (!job) return json(false, null, "Forbidden", 403);
    const status = String(job.status || "").toUpperCase();
    if (status !== "UPLOADING") {
      return json(false, null, `Invalid status for upload: ${status || "UNKNOWN"}`, 409);
    }

    // bytes -> Buffer
    const buf = Buffer.from(await file.arrayBuffer());
    const sig = buf.subarray(0, 5).toString("ascii");
    if (sig !== "%PDF-") return json(false, null, "Invalid PDF file", 400);

    const key = `${jobId}/input.pdf`;

    // Upload to R2 (server-side, CORS байхгүй)
    await r2.send(
      new PutObjectCommand({
        Bucket: R2_BUCKET_IN,
        Key: key,
        Body: buf,
        ContentType: "application/pdf",
      })
    );

    // Local ingest cache (same machine as worker-local) to avoid worker re-download.
    if (ENABLE_LOCAL_INGEST_FAST_PATH) {
      try {
        const dir = localIngestDir(jobId);
        fs.mkdirSync(dir, { recursive: true });
        fs.writeFileSync(path.join(dir, "input.pdf"), buf);
      } catch {
        // Non-fatal: worker will fallback to R2 download
      }
    }

    // Mark job as uploaded
    const { error } = await supabaseServer
      .from("jobs")
      .update({
        status: "UPLOADED",
        progress: 10,
        uploaded_at: new Date().toISOString(),
        input_path: key,
        stage: "UPLOAD",
        error_text: null,
      })
      .eq("id", jobId);

    if (error) return json(false, null, "Failed to mark upload", 500);

    return json(true, { jobId, key });
  } catch (e: any) {
    return json(false, null, "Server error", 500);
  }
}
