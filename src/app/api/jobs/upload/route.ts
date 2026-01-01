import { NextResponse } from "next/server";
import { supabaseServer } from "@/lib/supabase/server";

export const dynamic = "force-dynamic";
export const revalidate = 0;

function json(ok: boolean, data?: any, error?: string, status = 200) {
  return NextResponse.json({ ok, data, error }, { status });
}

/**
 * Client calls:
 *  PUT /api/jobs/upload?jobId=...
 *  Body: file bytes (application/octet-stream)
 *  Headers: x-upload-url, x-upload-token (from /api/jobs/create response)
 */
export async function PUT(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const jobId = searchParams.get("jobId");
    if (!jobId) return json(false, null, "Missing jobId", 400);

    const uploadUrl = req.headers.get("x-upload-url"); // (MVP: not used, kept for compatibility)
    const uploadToken = req.headers.get("x-upload-token");
    if (!uploadUrl || !uploadToken) return json(false, null, "Missing signed upload headers", 400);

    const body = await req.arrayBuffer();
    if (!body || body.byteLength === 0) return json(false, null, "Empty body", 400);

    // 1) Load job info (where to upload)
    const { data: job, error: jErr } = await supabaseServer
      .from("jobs")
      .select("input_path")
      .eq("id", jobId)
      .maybeSingle();

    if (jErr) return json(false, null, jErr.message, 500);
    if (!job?.input_path) return json(false, null, "Job not found or no input_path", 404);

    // 2) Upload bytes to signed URL (Supabase Storage)
    const bytes = new Uint8Array(body);

    const { error: upErr } = await supabaseServer.storage
      .from("job-input")
      .uploadToSignedUrl(job.input_path, uploadToken, bytes, {
        contentType: "application/pdf",
      });

    if (upErr) return json(false, null, `upload failed: ${upErr.message}`, 500);

    // 3) ✅ Upload completed -> mark job as UPLOADED (worker will set PROCESSING)
    const { error: updErr } = await supabaseServer
      .from("jobs")
      .update({
        status: "UPLOADED",
        progress: 1,
        uploaded_at: new Date().toISOString(),
        error_text: null,
      })
      .eq("id", jobId);

    if (updErr) return json(false, null, updErr.message, 500);

    return json(true, { ok: true });
  } catch (e: any) {
    return json(false, null, e?.message || "Server error", 500);
  }
}
