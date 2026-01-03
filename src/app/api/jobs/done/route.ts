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

// ---- Supabase (ADMIN) ----
// ---- R2 ----
const R2_ENDPOINT = process.env.R2_ENDPOINT!;
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID!;
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY!;
const R2_BUCKET_IN = process.env.R2_BUCKET_IN || "goodpdf-in";
const R2_BUCKET_OUT = process.env.R2_BUCKET_OUT || "goodpdf-out";

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

async function r2DeleteMany(bucket: string, keys: (string | null | undefined)[]) {
  const unique = [...new Set(keys.filter(Boolean) as string[])];
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
    const jobId = String(body.jobId || "");
    if (!jobId) return json(false, null, "Missing jobId", 400);

    // 1) Read job (source of truth)
    const { data: job, error } = await supabaseAdmin
      .from("jobs")
      .select("id,status,input_path,output_zip_path,expires_at,cleaned_at")
      .eq("id", jobId)
      .maybeSingle();

    if (error) return json(false, null, error.message, 500);
    if (!job) return json(false, null, "Job not found", 404);

    const status = String(job.status || "").toUpperCase();

    // Already cleaned => idempotent OK
    if (status === "CLEANED" || job.cleaned_at) {
      return json(true, { jobId, status: "CLEANED", cleanedAt: job.cleaned_at });
    }

    // Only confirm when DONE (canonical)
    if (status !== "DONE") {
      return json(false, null, `Not confirmable (status=${status})`, 409);
    }

    // 2) Delete from R2 (input + output zip)
    const inputKey = job.input_path || `${jobId}/input.pdf`;
    const outKey = job.output_zip_path || null;

    // input in bucket-in
    await r2DeleteMany(R2_BUCKET_IN, [inputKey]);
    // output in bucket-out
    if (outKey) await r2DeleteMany(R2_BUCKET_OUT, [outKey]);

    // 3) Mark CLEANED (canonical end)
    const now = new Date().toISOString();
    const { error: u2 } = await supabaseAdmin
      .from("jobs")
      .update({
        status: "CLEANED",
        cleaned_at: now,
        // optional: null out keys for privacy
        input_path: null,
        output_zip_path: null,
      })
      .eq("id", jobId);

    if (u2) return json(false, null, u2.message, 500);

    return json(true, { jobId, status: "CLEANED", cleanedAt: now });
  } catch (e: any) {
    return json(false, null, e?.message || "Server error", 500);
  }
}
