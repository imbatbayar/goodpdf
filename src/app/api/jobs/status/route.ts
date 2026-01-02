export const dynamic = "force-dynamic";
export const revalidate = 0;

import { NextResponse } from "next/server";
import { supabaseAdmin } from "@/lib/supabase/admin";

function res(ok: boolean, data?: any, error?: string, status = 200) {
  return NextResponse.json(
    { ok, data, error },
    { status, headers: { "cache-control": "no-store" } }
  );
}

export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const jobId = searchParams.get("jobId");
    if (!jobId) return res(false, null, "Missing jobId", 400);

    const { data: job, error } = await supabaseAdmin
      .from("jobs")
      .select(
        "id,status,progress,output_zip_path,zip_path,error_text,expires_at,confirmed_at,cleaned_at"
      )
      .eq("id", jobId)
      .maybeSingle();

    if (error) return res(false, null, error.message, 500);
    if (!job) return res(false, null, "Job not found", 404);

    // ✅ Standard: output_zip_path first
    const outPath =
      (job as any).output_zip_path || (job as any).zip_path || null;

    const expiresAt = (job as any).expires_at ?? null;
    const expired = expiresAt ? Date.now() > Date.parse(expiresAt) : false;

    const cleaned =
      (job as any).cleaned_at != null || (job as any).status === "CLEANED";

    const confirmed =
      (job as any).confirmed_at != null ||
      (job as any).status === "DONE_CONFIRMED";

    // ✅ Only downloadable when it is DONE, not expired, not cleaned, not confirmed, and has path
    const canDownload =
      (job as any).status === "DONE" &&
      !!outPath &&
      !expired &&
      !cleaned &&
      !confirmed;

    const downloadUrl = canDownload
      ? `/api/jobs/download?jobId=${encodeURIComponent(jobId)}`
      : null;

    return res(true, {
      status: (job as any).status,
      progress: (job as any).progress ?? 0,

      // main outputs
      downloadUrl,
      errorText: (job as any).error_text ?? null,

      // lifecycle flags
      expiresAt,
      expired,
      cleaned,
      confirmed,
    });
  } catch (e: any) {
    return res(false, null, e?.message || "Server error", 500);
  }
}
