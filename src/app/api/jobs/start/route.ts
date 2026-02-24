import { NextResponse } from "next/server";
import { supabaseAdmin } from "@/lib/supabase/admin";
import { checkRateLimit, rateLimitResponse } from "@/lib/rate-limit";

export const runtime = "nodejs";

function jsonError(message: string, status = 400) {
  return NextResponse.json({ ok: false, error: message }, { status });
}

export async function POST(req: Request) {
  try {
    const rl = checkRateLimit(req, { key: "jobs:start", limit: 30, windowMs: 60_000 });
    if (!rl.ok) return rateLimitResponse(rl.retryAfterSec);

    const body = await req.json();
    const jobId = String(body?.jobId || "");
    const ownerToken = String(req.headers.get("x-owner-token") || "").trim();

    const splitMbRaw = Number(body?.splitMb);
    const precheckModeRaw = String(body?.precheckMode || "").toUpperCase();
    const wantsHeavyMode =
      precheckModeRaw === "HEAVY" || precheckModeRaw === "EXTREME";
    if (!jobId) return jsonError("Missing jobId", 400);
    if (!ownerToken) return jsonError("Forbidden", 403);
    if (!Number.isFinite(splitMbRaw)) return jsonError("Invalid splitMb", 400);

    // ✅ future-proof: allow decimals, normalize to 2dp, clamp to sane range
    const splitMb = Math.min(500, Math.max(1, Math.round(splitMbRaw * 100) / 100));

    // ✅ DO NOT refresh expires_at here. Upload time is the hard TTL anchor.
    const { data: job, error } = await supabaseAdmin
      .from("jobs")
      .select("id,status,file_size_bytes,input_path")
      .eq("id", jobId)
      .eq("owner_token", ownerToken)
      .maybeSingle();

    if (error) return jsonError("Failed to start job", 500);
    if (!job) return jsonError("Forbidden", 403);

    const status = String(job.status || "").toUpperCase();
    if (status !== "UPLOADED") {
      return jsonError(`Invalid status for start: ${status || "UNKNOWN"}`, 409);
    }
    if (!job.input_path) return jsonError("Upload is incomplete", 409);

    const { error: updErr } = await supabaseAdmin
      .from("jobs")
      .update({
        status: "QUEUED",
        // Keep canonical queue stage for compatibility with existing workers.
        stage: "QUEUE",
        progress: 1,
        progress_pct: 1,
        split_mb: splitMb, // numeric(10,2)
        updated_at: new Date().toISOString(),
        error: null,
        error_text: null,
        // Non-fatal routing hint consumed by worker.
        error_code: wantsHeavyMode ? "HEAVY_HINT" : null,
      })
      .eq("id", jobId)
      .eq("owner_token", ownerToken);

    if (updErr) return jsonError("Failed to start job", 500);

    return NextResponse.json({ ok: true, jobId });
  } catch (e: any) {
    return jsonError("Start failed", 500);
  }
}
