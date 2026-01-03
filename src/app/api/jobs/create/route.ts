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

export async function POST(req: Request) {
  try {
    const R2_ENDPOINT = process.env.R2_ENDPOINT!;
    const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID!;
    const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY!;
    const R2_BUCKET_IN = process.env.R2_BUCKET_IN || "goodpdf-in";

    if (!R2_ENDPOINT || !R2_ACCESS_KEY_ID || !R2_SECRET_ACCESS_KEY) {
      return json(
        false,
        null,
        "Missing R2 env (R2_ENDPOINT/R2_ACCESS_KEY_ID/R2_SECRET_ACCESS_KEY)",
        500
      );
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

    const body = await req.json();

    const userId = String(body.userId || "");
    const splitMb = Number(body.splitMb || 0) || 0;

    const fileName = String(body.fileName || "input.pdf");
    const fileType = String(body.fileType || "application/pdf");
    const fileSizeBytes = Number(body.fileSizeBytes || 0) || 0;

    if (!userId) return json(false, null, "Missing userId", 400);
    if (!fileName) return json(false, null, "Missing fileName", 400);

    // split-only: PDF only
    if (!String(fileName).toLowerCase().endsWith(".pdf")) {
      return json(false, null, "Only PDF files are supported", 400);
    }
    if (fileType && !String(fileType).toLowerCase().includes("pdf")) {
      return json(false, null, "Only application/pdf is supported", 400);
    }

    const { data: job, error: jErr } = await supabaseAdmin
      .from("jobs")
      .insert({
        user_id: userId,
        status: "UPLOADING",
        stage: "UPLOAD",
        progress: 0,

        split_mb: splitMb,

        file_name: fileName,
        file_type: fileType,
        file_size_bytes: fileSizeBytes,

        // split-only: keep compress stable
        compress_progress: 100,
        split_progress: 0,

        error_text: null,
      })
      .select("*")
      .single();

    if (jErr) return json(false, null, jErr.message, 500);

    const jobId = String(job.id);
    const key = `${jobId}/input.pdf`;

    const { error: upErr } = await supabaseAdmin
      .from("jobs")
      .update({ input_path: key })
      .eq("id", jobId);

    if (upErr) return json(false, null, upErr.message, 500);

    const cmd = new PutObjectCommand({
      Bucket: R2_BUCKET_IN,
      Key: key,
    });

    const signedUrl = await getSignedUrl(r2, cmd, { expiresIn: 60 * 10 });

    return json(true, {
      jobId,
      inputKey: key,
      upload: { url: signedUrl },
    });
  } catch (e: any) {
    console.error("[/api/jobs/create ERROR]", e);
    return json(false, null, String(e?.message || e), 500);
  }
}
