import { NextResponse } from "next/server";
import { supabaseAdmin } from "@/lib/supabase/admin";
export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const revalidate = 0;

function json(ok: boolean, data?: any, error?: string, status = 200) {
  return NextResponse.json({ ok, data, error }, { status });
}


type Quality = "ORIGINAL" | "GOOD";

function normalizeQuality(v: any): Quality {
  const s = String(v || "").toUpperCase();
  if (s === "ORIGINAL") return "ORIGINAL";
  return "GOOD";
}

function normalizeSplitMb(v: any): number {
  const n = Number(v);
  if (!Number.isFinite(n)) return 9;
  return Math.max(1, Math.min(100, Math.round(n)));
}

/**
 * POST /api/jobs/start
 * Body: { jobId, quality, splitMb }
 *
 * Canonical:
 * - accepts UPLOADED or QUEUED
 * - sets QUEUED
 * - resets processing fields
 */
export async function POST(req: Request) {
  try {
    const body = await req.json().catch(() => ({} as any));

    const jobId = String(body.jobId || "");
    if (!jobId) return json(false, null, "Missing jobId", 400);

    const quality = normalizeQuality(body.quality);
    const splitMb = normalizeSplitMb(body.splitMb);

    const { data, error } = await supabaseAdmin
      .from("jobs")
      .update({
        quality,
        split_mb: splitMb,

        status: "QUEUED",
        stage: "QUEUE",
        progress: 0,

        // reset worker-owned fields
        compress_progress: 0,
        split_progress: 0,
        error_text: null,
        claimed_by: null,
        claimed_at: null,
        processing_started_at: null,
        done_at: null,
      })
      .eq("id", jobId)
      .in("status", ["UPLOADED", "QUEUED"])
      .select("id,status,quality,split_mb,progress")
      .maybeSingle();

    if (error) return json(false, null, error.message, 500);
    if (!data) return json(false, null, "Job not found or not startable", 404);

    return json(true, { job: data });
  } catch (e: any) {
    return json(false, null, e?.message || "Server error", 500);
  }
}
