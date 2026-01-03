export const dynamic = "force-dynamic";
export const revalidate = 0;
export const runtime = "nodejs";

import { NextResponse } from "next/server";
import { supabaseAdmin } from "@/lib/supabase/admin";
import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
function json(ok: boolean, error?: string, status = 200) {
  return NextResponse.json(
    { ok, error },
    { status, headers: { "cache-control": "no-store" } }
  );
}

// ---- Supabase (ADMIN) ----
// ---- R2 ----
const R2_ENDPOINT = process.env.R2_ENDPOINT!;
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID!;
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY!;
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

export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const jobId = searchParams.get("jobId");
    if (!jobId) return json(false, "Missing jobId", 400);

    // ✅ Canonical: only use output_zip_path (NO zip_path)
    const { data: job, error } = await supabaseAdmin
      .from("jobs")
      .select("status,output_zip_path,expires_at,cleaned_at")
      .eq("id", jobId)
      .maybeSingle();

    if (error) return json(false, error.message, 500);
    if (!job) return json(false, "Job not found", 404);

    const status = String(job.status || "").toUpperCase();
    if (status !== "DONE") {
      return json(false, `Not downloadable (status=${status})`, 409);
    }

    if (job.cleaned_at) {
      return json(false, "Expired (cleaned)", 410);
    }

    if (job.expires_at && Date.now() > Date.parse(job.expires_at)) {
      return json(false, "Expired", 410);
    }

    const outPath = job.output_zip_path;
    if (!outPath) {
      return json(false, "Output not available", 410);
    }

    // Signed GET (60 sec)
    const signedUrl = await getSignedUrl(
      r2,
      new GetObjectCommand({
        Bucket: R2_BUCKET_OUT,
        Key: outPath,
        ResponseContentDisposition: `attachment; filename="goodpdf-${jobId}.zip"`,
      }),
      { expiresIn: 60 }
    );

    return NextResponse.redirect(signedUrl, 307);
  } catch (e: any) {
    return json(false, e?.message || "Server error", 500);
  }
}
