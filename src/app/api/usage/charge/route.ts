import { NextResponse } from "next/server";
import { supabaseAdmin } from "@/lib/supabase/admin";
import { checkRateLimit, rateLimitResponse } from "@/lib/rate-limit";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

/**
 * POST /api/usage/charge
 * Body: { jobId: string }
 * Idempotent: charges exactly 1 token only when job is DONE and not already charged.
 * Requires x-owner-token (job owner).
 */
export async function POST(req: Request) {
  try {
    const rl = checkRateLimit(req, { key: "usage:charge", limit: 60, windowMs: 60_000 });
    if (!rl.ok) return rateLimitResponse(rl.retryAfterSec);

    const body = await req.json().catch(() => ({}));
    const jobId = String(body?.jobId || "").trim();
    const ownerToken = String(req.headers.get("x-owner-token") || "").trim();

    if (!jobId) {
      return NextResponse.json(
        { charged: false, reason: "missing_job_id" },
        { status: 400 }
      );
    }
    if (!ownerToken) {
      return NextResponse.json(
        { charged: false, reason: "forbidden" },
        { status: 403 }
      );
    }

    const { data: job, error: fetchErr } = await supabaseAdmin
      .from("jobs")
      .select("id, status, usage_charged_at")
      .eq("id", jobId)
      .eq("owner_token", ownerToken)
      .maybeSingle();

    if (fetchErr) {
      return NextResponse.json(
        { charged: false, reason: "error" },
        { status: 500 }
      );
    }
    if (!job) {
      return NextResponse.json(
        { charged: false, reason: "not_found" },
        { status: 404 }
      );
    }

    const status = String(job.status || "").toUpperCase();
    if (status !== "DONE") {
      return NextResponse.json(
        { charged: false, reason: "not_done" },
        { status: 200 }
      );
    }

    if (job.usage_charged_at != null) {
      return NextResponse.json(
        { charged: false, reason: "already_charged" },
        { status: 200 }
      );
    }

    const nowIso = new Date().toISOString();
    const { data: updated, error: updateErr } = await supabaseAdmin
      .from("jobs")
      .update({
        usage_charged_at: nowIso,
        updated_at: nowIso,
      })
      .eq("id", jobId)
      .eq("owner_token", ownerToken)
      .eq("status", "DONE")
      .is("usage_charged_at", null)
      .select("id")
      .maybeSingle();

    if (updateErr) {
      return NextResponse.json(
        { charged: false, reason: "error" },
        { status: 500 }
      );
    }

    if (!updated) {
      return NextResponse.json(
        { charged: false, reason: "already_charged" },
        { status: 200 }
      );
    }

    return NextResponse.json({ charged: true }, { status: 200 });
  } catch (e: any) {
    return NextResponse.json(
      { charged: false, reason: "error" },
      { status: 500 }
    );
  }
}
