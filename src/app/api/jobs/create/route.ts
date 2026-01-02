import { NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const revalidate = 0;

function json(ok: boolean, data?: any, error?: string, status = 200) {
  return NextResponse.json({ ok, data, error }, { status });
}

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;

const R2_ENDPOINT = process.env.R2_ENDPOINT!;
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID!;
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY!;
const R2_BUCKET_IN = process.env.R2_BUCKET_IN || "goodpdf-in";

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY");
}
if (!R2_ENDPOINT || !R2_ACCESS_KEY_ID || !R2_SECRET_ACCESS_KEY) {
  throw new Error("Missing R2_ENDPOINT / R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY");
}

const supa = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

const r2 = new S3Client({
  region: "auto",
  endpoint: R2_ENDPOINT,
  credentials: { accessKeyId: R2_ACCESS_KEY_ID, secretAccessKey: R2_SECRET_ACCESS_KEY },
  forcePathStyle: true, // ✅ CRITICAL for Cloudflare R2 presigned URLs
});

export async function POST(req: Request) {
  try {
    const body = await req.json();

    const userId = String(body.userId || "");
    const quality = String(body.quality || "GOOD");
    const splitMb = Number(body.splitMb || 0);
    const fileName = String(body.fileName || "input.pdf");
    const fileSize = Number(body.fileSize || 0);

    if (!userId) return json(false, null, "Missing userId", 400);
    if (!fileName) return json(false, null, "Missing fileName", 400);

    // 1) create job row
    const expiresAt = new Date(Date.now() + 10 * 60 * 1000).toISOString();

    const { data: job, error: jErr } = await supa
      .from("jobs")
      .insert({
        user_id: userId,
        status: "CREATED",
        progress: 0,
        quality,
        split_mb: splitMb,
        file_name: fileName,
        file_size_bytes: fileSize,
        expires_at: expiresAt,
      })
      .select("*")
      .single();

    if (jErr) return json(false, null, jErr.message, 500);

    const jobId = job.id as string;

    // 2) presigned PUT for R2
    const key = `${jobId}/input.pdf`;

    // ⚠️ ContentType-ийг presign дээр хүчээр битгий bind хий.
    // Browser тал PUT дээр header тавихгүй байгаа тул signed headers зөрөөд унадаг кейс их гардаг.
    const cmd = new PutObjectCommand({
      Bucket: R2_BUCKET_IN,
      Key: key,
    });

    const signedUrl = await getSignedUrl(r2, cmd, { expiresIn: 60 * 10 }); // 10 min

    // 3) store input_path for worker trace
    const { error: uErr } = await supa.from("jobs").update({ input_path: key }).eq("id", jobId);
    if (uErr) return json(false, null, uErr.message, 500);

    return json(true, {
      jobId,
      upload: { url: signedUrl },
    });
  } catch (e: any) {
    return json(false, null, e?.message || "Server error", 500);
  }
}
