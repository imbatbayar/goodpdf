export const dynamic = "force-dynamic";
export const revalidate = 0;
export const runtime = "nodejs";

import { NextResponse } from "next/server";
import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { supabaseServer } from "@/lib/supabase/server";

function json(ok: boolean, error?: string, status = 200) {
  return NextResponse.json(
    { ok, error },
    { status, headers: { "cache-control": "no-store" } }
  );
}

// R2 env (Next сервер талаас уншина)
const R2_ENDPOINT = process.env.R2_ENDPOINT!;
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID!;
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY!;
const R2_BUCKET_OUT = process.env.R2_BUCKET_OUT || "goodpdf-out";

const r2 = new S3Client({
  region: "auto",
  endpoint: R2_ENDPOINT,
  credentials: {
    accessKeyId: R2_ACCESS_KEY_ID,
    secretAccessKey: R2_SECRET_ACCESS_KEY,
  },
  // Cloudflare R2 дээр ихэнхдээ safe
  forcePathStyle: true,
});

export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const jobId = searchParams.get("jobId");
    if (!jobId) return json(false, "Missing jobId", 400);

    // DB-ээс job-г уншиж privacy/expiry дүрмээ хэвээр барина
    const { data: job, error } = await supabaseServer
      .from("jobs")
      .select("id,status,output_zip_path,zip_path,expires_at,cleaned_at,confirmed_at")
      .eq("id", jobId)
      .maybeSingle();

    if (error) return json(false, error.message, 500);
    if (!job) return json(false, "Job not found", 404);

    const status = (job as any).status as string;

    const cleaned = (job as any).cleaned_at != null || status === "CLEANED";
    const confirmed = (job as any).confirmed_at != null || status === "DONE_CONFIRMED";

    const expiresAt = (job as any).expires_at ?? null;
    const expired = expiresAt ? Date.now() > Date.parse(expiresAt) : false;

    if (cleaned) return json(false, "Expired (cleaned)", 410);
    if (confirmed) return json(false, "No longer available", 410);
    if (expired) return json(false, "Expired", 410);

    if (status !== "DONE") {
      return json(false, `Not downloadable (status=${status})`, 409);
    }

    // ✅ Worker чинь үүнийг DB-д бичдэг: output_zip_path = "{jobId}/goodpdf.zip"
    const outPath =
      (job as any).output_zip_path || (job as any).zip_path || `${jobId}/goodpdf.zip`;

    // R2 Signed GET (60 sec)
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
