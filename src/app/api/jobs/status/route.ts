export const dynamic = "force-dynamic";
export const revalidate = 0;

import { NextResponse } from "next/server";
import { supabaseServer } from "@/lib/supabase/server";

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);
  const jobId = searchParams.get("jobId");
  if (!jobId) {
    return NextResponse.json({ ok: false, error: "Missing jobId" }, { status: 400 });
  }

  const { data: job, error } = await supabaseServer
    .from("jobs")
    .select("id,status,progress,zip_path,output_zip_path,error_text,expires_at,confirmed_at,cleaned_at")
    .eq("id", jobId)
    .maybeSingle();

  if (error) {
    return NextResponse.json({ ok: false, error: error.message }, { status: 500 });
  }
  if (!job) {
    return NextResponse.json({ ok: false, error: "Job not found" }, { status: 404 });
  }

  const outPath = (job as any).zip_path || (job as any).output_zip_path || null;

  const expiresAt = (job as any).expires_at ?? null;
  const expired = expiresAt ? Date.now() > Date.parse(expiresAt) : false;

  const cleaned = (job as any).cleaned_at != null || (job as any).status === "CLEANED";
  const confirmed = (job as any).confirmed_at != null || (job as any).status === "DONE_CONFIRMED";

  const canDownload =
    (job as any).status === "DONE" && !!outPath && !expired && !cleaned && !confirmed;

  const downloadUrl = canDownload ? `/api/jobs/download?jobId=${encodeURIComponent(jobId)}` : null;

  return NextResponse.json(
    {
      ok: true,
      data: {
        status: (job as any).status,
        progress: (job as any).progress ?? 0,
        downloadUrl,
        errorText: (job as any).error_text ?? null,
        expiresAt,
        expired,
      },
    },
    { headers: { "cache-control": "no-store" } }
  );
}
