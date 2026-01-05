import { NextResponse } from "next/server";
import { supabaseAdmin } from "@/lib/supabase/admin";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const revalidate = 0;

function json(ok: boolean, data?: any, error?: string, status = 200) {
  return NextResponse.json({ ok, data, error }, { status });
}

console.log("[R2 ENV CHECK]", {
  hasEndpoint: !!process.env.R2_ENDPOINT,
  hasKey: !!process.env.R2_ACCESS_KEY_ID,
  hasSecret: !!process.env.R2_SECRET_ACCESS_KEY,
  bucketIn: process.env.R2_BUCKET_IN,
  bucketOut: process.env.R2_BUCKET_OUT,
});

export async function POST(req: Request) {
  try {
    const body = await req.json().catch(() => null);

    const userId = String(body?.userId || "").trim();
    const fileName = String(body?.fileName || "file.pdf").trim();
    const fileSizeBytes = Number(body?.fileSizeBytes || 0);

    if (!userId) return json(false, null, "userId is required.", 400);
    if (!fileName) return json(false, null, "fileName is required.", 400);
    if (!Number.isFinite(fileSizeBytes) || fileSizeBytes <= 0) {
      return json(false, null, "fileSizeBytes is invalid.", 400);
    }

    const R2_ENDPOINT = process.env.R2_ENDPOINT!;
    const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID!;
    const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY!;
    // ✅ CANONICAL: upload goes to IN bucket (legacy R2_BUCKET байвал мөн зөвшөөрнө)
    const R2_BUCKET_IN = (process.env.R2_BUCKET_IN || process.env.R2_BUCKET || "").trim();

    if (!R2_ENDPOINT || !R2_ACCESS_KEY_ID || !R2_SECRET_ACCESS_KEY || !R2_BUCKET_IN) {
      return json(false, null, "Missing R2 env vars.", 500);
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

    // 1) create job row (UPLOADING state)
    const { data: jobRow, error: jobErr } = await supabaseAdmin
      .from("jobs")
      .insert({
        user_id: userId,
        status: "UPLOADING",
        stage: "UPLOAD",
        progress: 0,
        file_name: fileName,
        file_size_bytes: fileSizeBytes,
        // schema constraint-д зориулсан fallback (Start дээр жинхэнэ утга орно)
        split_mb: body?.splitMb ?? null,
        error_text: null,
        error_code: null,
      })
      .select("id")
      .single();

    if (jobErr) return json(false, null, jobErr.message, 500);

    const jobId = jobRow.id as string;

    // ✅ CANONICAL input key (worker/cleanup бүгдтэй таарна)
    const key = `${jobId}/input.pdf`;

    // 2) presign PUT
    const cmd = new PutObjectCommand({
      Bucket: R2_BUCKET_IN,
      Key: key,
      ContentType: "application/pdf",
    });

    const signedUrl = await getSignedUrl(r2, cmd, { expiresIn: 60 * 10 });

    // 3) save input_path so worker knows where to read
    const { error: updErr } = await supabaseAdmin
      .from("jobs")
      .update({
        input_path: key,
      })
      .eq("id", jobId);

    if (updErr) return json(false, null, updErr.message, 500);

    return json(true, {
      jobId,
      inputKey: key,

      // canonical
      uploadUrl: signedUrl,

      // legacy/compat
      upload: { url: signedUrl },
    });
  } catch (e: any) {
    console.error("[/api/jobs/create ERROR]", e);
    return json(false, null, String(e?.message || e), 500);
  }
}
