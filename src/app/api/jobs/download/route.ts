export const dynamic = "force-dynamic";
export const revalidate = 0;
export const runtime = "nodejs";

import { NextResponse } from "next/server";
import { supabaseAdmin } from "@/lib/supabase/admin";
import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

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
const R2_BUCKET_OUT =
  process.env.R2_BUCKET_OUT || process.env.R2_BUCKET || "goodpdf-out";

if (!R2_ENDPOINT || !R2_ACCESS_KEY_ID || !R2_SECRET_ACCESS_KEY) {
  throw new Error(
    "Missing R2_ENDPOINT / R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY"
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

export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const jobId = (searchParams.get("jobId") || "").trim();
    const debug = (searchParams.get("debug") || "").trim() === "1";

    if (!jobId) return json(false, null, "Missing jobId", 400);

    const { data: job, error } = await supabaseAdmin
      .from("jobs")
      .select("status, output_zip_path, expires_at, cleaned_at")
      .eq("id", jobId)
      .maybeSingle();

    if (error) return json(false, null, error.message, 500);
    if (!job) return json(false, null, "Job not found", 404);

    const status = String(job.status || "").toUpperCase();
    if (status !== "DONE") {
      return json(false, null, `Not downloadable (status=${status})`, 409);
    }

    if (job.cleaned_at) {
      return json(
        false,
        debug
          ? { reason: "cleaned_at", cleaned_at: job.cleaned_at }
          : null,
        "Expired (cleaned)",
        410
      );
    }

    if (job.expires_at && Date.now() > Date.parse(job.expires_at)) {
      return json(
        false,
        debug
          ? { reason: "expires_at", expires_at: job.expires_at, now: new Date().toISOString() }
          : null,
        "Expired",
        410
      );
    }

    const outKey = (job.output_zip_path || "").trim();
    if (!outKey) {
      return json(
        false,
        debug ? { reason: "missing_output_zip_path" } : null,
        "Output not available",
        410
      );
    }

    // 1) R2 signed URL (GetObject)
    const signedUrl = await getSignedUrl(
      r2,
      new GetObjectCommand({
        Bucket: R2_BUCKET_OUT,
        Key: outKey,
        // Энэ header-ийг R2 талаас өгч болно, гэхдээ бид доор өөрсдөө attachment header тавина
        ResponseContentDisposition: `attachment; filename="goodpdf-${jobId}.zip"`,
      }),
      { expiresIn: 60 * 10 }
    );

    // 2) Redirect хийхгүй — server-ээр дамжуулж stream хийнэ (хоосон ZIP асуудлыг шийднэ)
    const r = await fetch(signedUrl);

    if (!r.ok || !r.body) {
      return json(
        false,
        debug ? { reason: "r2_fetch_failed", status: r.status } : null,
        "Failed to fetch zip from R2",
        500
      );
    }

    // Content-Length байвал авч дамжуулна (зарим үед хэрэгтэй)
    const contentLength = r.headers.get("content-length");

    const headers: Record<string, string> = {
      "Content-Type": "application/zip",
      "Content-Disposition": `attachment; filename="goodpdf-${jobId}.zip"`,
      "Cache-Control": "no-store",
    };
    if (contentLength) headers["Content-Length"] = contentLength;

    return new NextResponse(r.body, { status: 200, headers });
  } catch (e: any) {
    return json(false, null, e?.message || "Server error", 500);
  }
}
