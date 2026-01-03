export const runtime = "nodejs";
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

function clampPct(v: any, fallback = 0) {
  const n = Number(v);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(0, Math.min(100, Math.round(n)));
}

export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const jobId = searchParams.get("jobId");
    if (!jobId) return res(false, null, "Missing jobId", 400);

    const { data: job, error } = await supabaseAdmin
      .from("jobs")
      .select("*")
      .eq("id", jobId)
      .maybeSingle();

    if (error) return res(false, null, error.message, 500);
    if (!job) return res(false, null, "Job not found", 404);

    const status = String(job.status || "CREATED").toUpperCase();

    const outputZipPath =
      job.output_zip_path || null;

    const progress = clampPct(job.progress, 0);
    const compressProgress =
      String(job.quality || "").toUpperCase() === "ORIGINAL"
        ? 100
        : clampPct(job.compress_progress, status === "DONE" ? 100 : 0);

    const splitProgress = clampPct(
      job.split_progress,
      progress
    );

    return res(true, {
      // ✅ source of truth
      status,
      outputZipPath,

      // progress
      progress,
      compressProgress,
      splitProgress,

      // summary
      compressedMb: job.compressed_mb ?? null,
      partsCount: job.parts_count ?? null,
      maxPartMb: job.max_part_mb ?? null,
      targetMb: job.target_mb ?? job.split_mb ?? null,

      // lifecycle
      expiresAt: job.expires_at ?? null,
      cleaned: job.cleaned_at != null || status === "CLEANED",
      errorText: job.error_text ?? null,
    });
  } catch (e: any) {
    return res(false, null, e?.message || "Server error", 500);
  }
}
