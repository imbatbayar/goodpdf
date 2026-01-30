import { NextResponse } from "next/server";
import { supabaseAdmin } from "@/lib/supabase/admin";

export const runtime = "nodejs";

function jsonError(message: string, status = 400) {
  return NextResponse.json({ ok: false, error: message }, { status });
}

export async function POST(req: Request) {
  try {
    const body = await req.json();
    const jobId = String(body?.jobId || "");

    const splitMbRaw = Number(body?.splitMb);
    if (!jobId) return jsonError("Missing jobId", 400);
    if (!Number.isFinite(splitMbRaw)) return jsonError("Invalid splitMb", 400);

    // ✅ future-proof: allow decimals, normalize to 2dp, clamp to sane range
    const splitMb = Math.min(500, Math.max(1, Math.round(splitMbRaw * 100) / 100));

    // ✅ DO NOT refresh expires_at here. Upload time is the hard TTL anchor.
    const { error } = await supabaseAdmin
      .from("jobs")
      .update({
        status: "QUEUED",
        stage: "QUEUE",
        progress_pct: 1,
        split_mb: splitMb, // numeric(10,2)
        updated_at: new Date().toISOString(),
        error: null,
      })
      .eq("id", jobId);

    if (error) return jsonError(error.message, 500);

    return NextResponse.json({ ok: true, jobId });
  } catch (e: any) {
    return jsonError(e?.message || "Start failed", 500);
  }
}
