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

function normStatus(s: any) {
  const v = String(s || "").toUpperCase();
  // allow known states; otherwise pass-through
  return v || "UNKNOWN";
}

function clampPct(v: any, fallback = 0) {
  const n = Number(v);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(0, Math.min(100, Math.round(n)));
}

/**
 * GET /api/jobs/status?jobId=<uuid>
 * - Requires: x-owner-token header
 * - Returns: { status, stage, progressPct, ... , errorText?, warningText? }
 *
 * Rules:
 * - errorText ONLY when status === FAILED
 * - warningText ONLY when status === DONE (fallback notice)
 * - Never treat warning as error (prevents "Something went wrong" modal)
 */
export async function GET(req: Request) {
  try {
    const url = new URL(req.url);
    const jobId = String(url.searchParams.get("jobId") || "");
    const ownerToken = req.headers.get("x-owner-token") || "";

    if (!jobId) return res(false, null, "Missing jobId", 400);
    if (!ownerToken) return res(false, null, "Missing x-owner-token", 401);

    // ✅ safest: select('*') so schema drift won't break (missing columns won't 500)
    const { data: job, error } = await supabaseAdmin
      .from("jobs")
      .select("*")
      .eq("id", jobId)
      .eq("owner_token", ownerToken)
      .maybeSingle();

    if (error) return res(false, null, error.message, 500);
    if (!job) return res(false, null, "Not found", 404);

    const status = normStatus(job.status);
    const stage = String(job.stage || "");

    // Prefer progress_pct if exists, else fallback to progress
    const progressPct = clampPct(job.progress_pct ?? job.progress ?? 0, 0);

    // "expires" / "delete" anchors (support both)
    const expiresAt = job.expires_at ?? job.delete_at ?? null;

    // outputs (support both)
    const zipPath = job.output_zip_path ?? job.zip_path ?? job.output_path ?? null;

    // warning field (preferred) — if not present, simply null (won't break)
    const warningText =
      status === "DONE"
        ? (job.warning_text ?? null)
        : null;

    // error field — ONLY for FAILED
    const errorText =
      status === "FAILED"
        ? (job.error_text ?? job.error ?? null)
        : null;

    return res(true, {
      id: job.id,
      status,
      stage,
      progressPct,

      // file meta
      fileName: job.file_name ?? null,
      fileSizeBytes: job.file_size_bytes ?? null,
      fileType: job.file_type ?? null,

      // split settings
      splitMb: job.split_mb ?? null,
      targetMb: job.target_mb ?? job.split_mb ?? null,
      maxPartMb: job.max_part_mb ?? null,

      // results
      partsCount: job.parts_count ?? null,
      outputZipPath: zipPath,
      outputZipBytes: job.output_zip_bytes ?? null,

      // lifecycle
      expiresAt,
      cleaned: job.cleaned_at != null || status === "CLEANED",
      doneAt: job.done_at ?? null,

      // messaging (IMPORTANT)
      errorText,     // only when FAILED
      warningText,   // only when DONE
    });
  } catch (e: any) {
    return res(false, null, e?.message || "Server error", 500);
  }
}
