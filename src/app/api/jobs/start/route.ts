import { NextResponse } from "next/server";
import { supabaseServer } from "@/lib/supabase/server";

export const dynamic = "force-dynamic";
export const revalidate = 0;

function json(ok: boolean, data?: any, error?: string, status = 200) {
  return NextResponse.json({ ok, data, error }, { status });
}

type Quality = "ORIGINAL" | "GOOD";

function normalizeQuality(v: any): Quality {
  const s = String(v || "").toUpperCase();
  if (s === "ORIGINAL") return "ORIGINAL";
  return "GOOD"; // default
}

function normalizeSplitMb(v: any): number {
  const n = Number(v);
  if (!Number.isFinite(n)) return 9;
  // user said <=100; protect from nonsense
  const clamped = Math.max(1, Math.min(100, Math.round(n)));
  return clamped;
}

/**
 * Client calls:
 *  POST /api/jobs/start
 *  Body: { jobId, quality, splitMb }
 *  Purpose:
 *    - save processing options (quality/split_mb)
 *    - move job to QUEUED so worker can pick it
 */
export async function POST(req: Request) {
  try {
    const body = await req.json();

    const jobId = String(body.jobId || "");
    if (!jobId) return json(false, null, "Missing jobId", 400);

    const quality = normalizeQuality(body.quality);
    const splitMb = normalizeSplitMb(body.splitMb);

    // IMPORTANT:
    // - only allow start if job is UPLOADED and not claimed
    // - worker will only pick QUEUED
    const { data, error } = await supabaseServer
      .from("jobs")
      .update({
        quality,
        split_mb: splitMb,
        status: "QUEUED",
        progress: 10,
        error_text: null,
        claimed_by: null,
        claimed_at: null,
        processing_started_at: null,
        done_at: null,
      })
      .eq("id", jobId)
      .eq("status", "UPLOADED")
      .is("claimed_by", null)
      .select("id,status,quality,split_mb,progress")
      .single();

    if (error) return json(false, null, error.message, 500);
    if (!data) return json(false, null, "Job not found or not UPLOADED", 404);

    return json(true, { job: data });
  } catch (e: any) {
    return json(false, null, e?.message || "Server error", 500);
  }
}
