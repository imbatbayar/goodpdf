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

function phaseFromStatus(status: string | null) {
  const s = String(status || "").toUpperCase();

  // Canonical UI mapping
  if (s === "CREATED" || s === "UPLOADING") return "UPLOADING";
  if (s === "UPLOADED") return "UPLOADED";
  if (s === "QUEUED" || s === "PROCESSING") return "PROCESSING";
  if (s === "DONE" || s === "DONE_CONFIRMED") return "READY";
  if (s === "FAILED") return "ERROR";
  if (s === "EXPIRED" || s === "CANCELLED" || s === "CLEANED") return "IDLE";

  // fallback
  return "IDLE";
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

    /**
     * IMPORTANT:
     * select("*") ашиглаж байна.
     * Ингэснээр дараа нь DB дээр шинэ багана нэмэхэд (compress_progress гэх мэт)
     * энэ route эвдрэхгүй.
     */
    const { data: job, error } = await supabaseAdmin
      .from("jobs")
      .select("*")
      .eq("id", jobId)
      .maybeSingle();

    if (error) return res(false, null, error.message, 500);
    if (!job) return res(false, null, "Job not found", 404);

    const status = String((job as any).status || "CREATED");
    const phase = phaseFromStatus(status);

    // ✅ Standard: output_zip_path first
    const outPath =
      (job as any).output_zip_path || (job as any).zip_path || null;

    const expiresAt = (job as any).expires_at ?? null;
    const expired = expiresAt ? Date.now() > Date.parse(expiresAt) : false;

    const cleaned =
      (job as any).cleaned_at != null || String((job as any).status) === "CLEANED";

    const confirmed =
      (job as any).confirmed_at != null ||
      String((job as any).status) === "DONE_CONFIRMED";

    // ✅ Download зөвхөн DONE үед (confirmed хийсний дараа UI reset хийх ёстой)
    const canDownload =
      String((job as any).status) === "DONE" &&
      !!outPath &&
      !expired &&
      !cleaned;

    const downloadUrl = canDownload
      ? `/api/jobs/download?jobId=${encodeURIComponent(jobId)}`
      : null;

    // ====== Progress (canonical) ======
    const progress = clampPct((job as any).progress, 0);

    // stage progress (байвал ашиглана, байхгүй бол fallback)
    const quality = String((job as any).quality || "GOOD").toUpperCase();

    const compressPct =
      quality === "ORIGINAL"
        ? 100
        : clampPct((job as any).compress_progress, status === "DONE" ? 100 : 0);

    const splitPct = clampPct(
      (job as any).split_progress,
      // fallback: ерөнхий progress-ийг split дээр үзүүлнэ
      progress
    );

    const stage =
      (job as any).stage ||
      (status === "QUEUED"
        ? "QUEUE"
        : status === "PROCESSING"
        ? "PROCESSING"
        : status === "DONE"
        ? "DONE"
        : status === "FAILED"
        ? "FAILED"
        : null);

    // ====== Result summary (байвал харуулна) ======
    // Worker дараа нь эднийг бичдэг болно: compressed_mb, parts_count, max_part_mb
    const compressedMb =
      (job as any).compressed_mb ?? (job as any).compressedMb ?? null;

    const partsCount =
      (job as any).parts_count ?? (job as any).partsCount ?? null;

    const maxPartMb =
      (job as any).max_part_mb ?? (job as any).maxPartMb ?? null;

    const targetMb =
      (job as any).target_mb ??
      (job as any).split_mb ??
      (job as any).splitMb ??
      null;

    return res(true, {
      // canonical
      status,
      phase,

      // progress
      progress,
      stage,
      compressPct,
      splitPct,

      // main outputs
      downloadUrl,
      errorText: (job as any).error_text ?? (job as any).errorText ?? null,

      // summary
      compressedMb,
      partsCount,
      maxPartMb,
      targetMb,

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
