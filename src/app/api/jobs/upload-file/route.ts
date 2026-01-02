import { NextResponse } from "next/server";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { supabaseServer } from "@/lib/supabase/server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const revalidate = 0;

function json(ok: boolean, data?: any, error?: string, status = 200) {
  return NextResponse.json({ ok, data, error }, { status });
}

const R2_ENDPOINT = process.env.R2_ENDPOINT!;
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID!;
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY!;
const R2_BUCKET_IN = process.env.R2_BUCKET_IN || "goodpdf-in";

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
    const { searchParams } = new URL(req.url);
    const jobId = String(searchParams.get("jobId") || "");
    if (!jobId) return json(false, null, "Missing jobId", 400);

    const form = await req.formData();
    const file = form.get("file");

    if (!file || !(file instanceof File)) {
      return json(false, null, "Missing file (form-data field name must be 'file')", 400);
    }

    // bytes -> Buffer
    const buf = Buffer.from(await file.arrayBuffer());

    const key = `${jobId}/input.pdf`;

    // Upload to R2 (server-side, CORS байхгүй)
    await r2.send(
      new PutObjectCommand({
        Bucket: R2_BUCKET_IN,
        Key: key,
        Body: buf,
        ContentType: file.type || "application/pdf",
      })
    );

    // Mark job as uploaded
    const { error } = await supabaseServer
      .from("jobs")
      .update({
        status: "UPLOADED",
        progress: 10,
        uploaded_at: new Date().toISOString(),
        input_path: key,
        error_text: null,
      })
      .eq("id", jobId);

    if (error) return json(false, null, error.message, 500);

    return json(true, { jobId, key });
  } catch (e: any) {
    return json(false, null, e?.message || "Server error", 500);
  }
}
