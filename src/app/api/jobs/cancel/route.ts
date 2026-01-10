export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const revalidate = 0;

import { NextResponse } from "next/server";
import { supabaseAdmin } from "@/lib/supabase/admin";
import { S3Client, DeleteObjectsCommand } from "@aws-sdk/client-s3";

function json(ok: boolean, data?: any, error?: string, status = 200) {
  return NextResponse.json(
    { ok, data, error },
    { status, headers: { "cache-control": "no-store" } }
  );
}

// ---- R2 ----
const R2_ENDPOINT = process.env.R2_ENDPOINT!;
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID!;
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY!;
const R2_BUCKET_IN = process.env.R2_BUCKET_IN || "goodpdf-in";
const R2_BUCKET_OUT = process.env.R2_BUCKET_OUT || process.env.R2_BUCKET || "goodpdf-out";

if (!R2_ENDPOINT || !R2_ACCESS_KEY_ID || !R2_SECRET_ACCESS_KEY) {
  throw new Error("Missing R2_ENDPOINT / R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY");
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

function cleanKey(k: any): string | null {
  if (!k) return null;
  const s = String(k).trim();
  if (!s) return null;
  return s.replace(/^\/+/, "");
}

async function r2DeleteMany(bucket: string, keys: (string | null | undefined)[]) {
  const unique = [...new Set(keys.map(cleanKey).filter(Boolean) as string[])];
  if (unique.length === 0) return;

  await r2.send(
    new DeleteObjectsCommand({
      Bucket: bucket,
      Delete: { Objects: unique.map((Key) => ({ Key })), Quiet: true },
    })
  );
}

export async function POST(req: Request) {
  try {
    const body = await req.json().catch(() => ({} as any));
    const jobId = String(body.jobId || "").trim();
    if (!jobId) return json(false, null, "Missing jobId", 400);

    // ✅ SECURITY GATE: require owner token
    const ownerToken = (req.headers.get("x-owner-token") || "").trim();
    if (!ownerToken) return json(false, null, "Forbidden", 403);

    const { data: job, error } = await supabaseAdmin
      .from("jobs")
      .select("status,input_path,output_zip_path,cleaned_at")
      .eq("id", jobId)
      .eq("owner_token", ownerToken)
      .maybeSingle();

    if (error) return json(false, null, error.message, 500);

    // ✅ Do NOT reveal existence if token mismatch
    if (!job) return json(false, null, "Forbidden", 403);

    // idempotent
    if (job.cleaned_at || String(job.status).toUpperCase() === "CLEANED") {
      return json(true, { jobId, status: "CLEANED" });
    }

    // Best-effort R2 cleanup
    const inputKey = cleanKey(job.input_path) || `${jobId}/input.pdf`;
    const outKey = cleanKey(job.output_zip_path);

    await r2DeleteMany(R2_BUCKET_IN, [inputKey]);
    if (outKey) await r2DeleteMany(R2_BUCKET_OUT, [outKey]);

    // Mark CLEANED
    const now = new Date().toISOString();
    const { error: u2 } = await supabaseAdmin
      .from("jobs")
      .update({
        status: "CLEANED",
        cleaned_at: now,
        input_path: null,
        output_zip_path: null,
      })
      .eq("id", jobId)
      .eq("owner_token", ownerToken);

    if (u2) return json(false, null, u2.message, 500);

    return json(true, { jobId, status: "CLEANED", cleanedAt: now });
  } catch (e: any) {
    return json(false, null, e?.message || "Server error", 500);
  }
}
