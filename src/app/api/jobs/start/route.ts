import { NextResponse } from "next/server";
import { supabaseAdmin } from "@/lib/supabase/admin";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const revalidate = 0;

// 🔒 LOCKED TTL: privacy-first retention baseline
const LOCKED_TTL_MINUTES = 10;

function json(ok: boolean, data?: any, error?: string, status = 200) {
  return NextResponse.json(
    { ok, data, error },
    { status, headers: { "cache-control": "no-store" } }
  );
}

function parseSplitMb(v: any): { value: number | null; error: string | null } {
  // splitMb нь optional байж болно (хуучин урсгал эвдэхгүй)
  if (v === undefined || v === null || v === "") return { value: null, error: null };

  const n = Number(v);
  if (!Number.isFinite(n)) return { value: null, error: "splitMb must be a number." };
  if (n <= 0) return { value: null, error: "splitMb must be > 0." };

  // production-safe guard (хэт том утгаас хамгаална)
  if (n > 500) return { value: null, error: "splitMb is too large (max 500MB per part)." };

  return { value: Math.round(n * 100) / 100, error: null };
}

function normStatus(s: any) {
  return String(s || "").trim().toUpperCase();
}

export async function POST(req: Request) {
  try {
    const body = await req.json().catch(() => null);

    const jobId = String(body?.jobId || "").trim();
    if (!jobId) return json(false, null, "jobId is required.", 400);

    // ✅ SECURITY GATE: require owner token
    const ownerToken = (req.headers.get("x-owner-token") || "").trim();
    if (!ownerToken) return json(false, null, "Forbidden", 403);

    const { value: splitMb, error: splitErr } = parseSplitMb(body?.splitMb);
    if (splitErr) return json(false, null, splitErr, 400);

    // 1) Fetch job with owner check (do NOT leak existence)
    const { data: job, error: gErr } = await supabaseAdmin
      .from("jobs")
      .select("id,status,split_mb,progress,stage,delete_at,ttl_minutes,cleaned_at")
      .eq("id", jobId)
      .eq("owner_token", ownerToken)
      .maybeSingle();

    if (gErr) return json(false, null, gErr.message, 500);
    if (!job) return json(false, null, "Forbidden", 403);

    const st = normStatus(job.status);

    // 2) Idempotency: if already started (or finished), do NOT re-queue
    if (st === "QUEUED" || st === "PROCESSING") {
      return json(true, { job, alreadyStarted: true });
    }
    if (st === "DONE") {
      return json(true, { job, alreadyStarted: true });
    }
    if (st === "FAILED" || st === "CLEANED") {
      return json(false, null, `Job not startable (status=${st})`, 409);
    }

    // Only UPLOADED can start
    if (st !== "UPLOADED") {
      return json(false, null, `Job not startable (status=${st})`, 409);
    }

    // 🔒 Refresh retention window on start (10 minutes from now)
    const ttlMinutes = LOCKED_TTL_MINUTES;
    const deleteAtIso = new Date(Date.now() + ttlMinutes * 60_000).toISOString();

    const updatePayload: Record<string, any> = {
      status: "QUEUED",
      stage: "QUEUE",
      progress: 0,

      // 🔒 retention baseline
      ttl_minutes: ttlMinutes,
      delete_at: deleteAtIso,
      cleaned_at: null,

      updated_at: new Date().toISOString(),
    };

    if (splitMb !== null) updatePayload.split_mb = splitMb;

    // 3) Atomic-ish update: only if still UPLOADED and owner matches
    const { data: updated, error: uErr } = await supabaseAdmin
      .from("jobs")
      .update(updatePayload)
      .eq("id", jobId)
      .eq("owner_token", ownerToken)
      .eq("status", "UPLOADED")
      .select("id,status,split_mb,progress,stage,delete_at,ttl_minutes,cleaned_at")
      .maybeSingle();

    if (uErr) return json(false, null, uErr.message, 500);

    // If someone else started it between GET and UPDATE, treat as already started (idempotent)
    if (!updated) {
      const { data: job2, error: rErr } = await supabaseAdmin
        .from("jobs")
        .select("id,status,split_mb,progress,stage,delete_at,ttl_minutes,cleaned_at")
        .eq("id", jobId)
        .eq("owner_token", ownerToken)
        .maybeSingle();

      if (rErr) return json(false, null, rErr.message, 500);
      if (!job2) return json(false, null, "Forbidden", 403);

      return json(true, { job: job2, alreadyStarted: true });
    }

    return json(true, { job: updated });
  } catch (e: any) {
    return json(false, null, e?.message || "Server error", 500);
  }
}
