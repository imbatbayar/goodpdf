import { NextResponse } from "next/server";
import { supabaseAdmin } from "@/lib/supabase/admin";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const revalidate = 0;

function json(ok: boolean, data?: any, error?: string, status = 200) {
  return NextResponse.json({ ok, data, error }, { status, headers: { "cache-control": "no-store" } });
}

function normalizeSplitMb(v: any): number {
  const n = Number(v);
  if (!Number.isFinite(n)) return 9;
  // Canonical UI range (1..100). Exact size not guaranteed (page aligned).
  return Math.max(1, Math.min(100, Math.round(n)));
}

/**
 * POST /api/jobs/start
 * Body: { jobId, splitMb }
 *
 * Idempotent behavior:
 * - If already PROCESSING/DONE/CLEANED/FAILED => ok=true (no hard fail)
 * - Otherwise accept UPLOADED/QUEUED and set QUEUED + reset worker-owned fields
 */
export async function POST(req: Request) {
  try {
    const body = await req.json().catch(() => ({} as any));

    const jobId = String(body.jobId || "");
    if (!jobId) return json(false, null, "Missing jobId", 400);

    const splitMb = normalizeSplitMb(body.splitMb);

    const cur = await supabaseAdmin
      .from("jobs")
      .select("id,status,split_mb,progress,stage")
      .eq("id", jobId)
      .maybeSingle();

    if (cur.error) return json(false, null, cur.error.message, 500);
    if (!cur.data) return json(false, null, "Job not found", 404);

    const curStatus = String(cur.data.status || "").toUpperCase();

    if (["PROCESSING", "DONE", "CLEANED", "FAILED"].includes(curStatus)) {
      return json(true, { job: cur.data, note: "already-started" });
    }

    if (!["UPLOADED", "QUEUED"].includes(curStatus)) {
      return json(false, { job: cur.data }, "Job not startable in current status", 409);
    }

    const { data, error } = await supabaseAdmin
      .from("jobs")
      .update({
        split_mb: splitMb,

        status: "QUEUED",
        stage: "QUEUE",
        progress: 0,

        // split-only: keep compress_progress stable
        compress_progress: 100,
        split_progress: 0,
        error_text: null,

        // allow worker to claim freshly
        claimed_by: null,
        claimed_at: null,
        processing_started_at: null,
        done_at: null,
      })
      .eq("id", jobId)
      .in("status", ["UPLOADED", "QUEUED"])
      .select("id,status,split_mb,progress,stage")
      .maybeSingle();

    if (error) return json(false, null, error.message, 500);
    if (!data) return json(false, null, "Job not found or not startable", 404);

    return json(true, { job: data });
  } catch (e: any) {
    return json(false, null, e?.message || "Server error", 500);
  }
}
