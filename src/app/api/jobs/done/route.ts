export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const revalidate = 0;

import { NextResponse } from "next/server";
import { supabaseAdmin } from "@/lib/supabase/admin";

function json(ok: boolean, data?: any, error?: string, status = 200) {
  return NextResponse.json(
    { ok, data, error },
    { status, headers: { "cache-control": "no-store" } }
  );
}

function toMs(v: any): number | null {
  if (!v) return null;
  const t = Date.parse(String(v));
  return Number.isFinite(t) ? t : null;
}

export async function POST(req: Request) {
  try {
    const body = await req.json().catch(() => ({} as any));
    const jobId = String(body.jobId || "").trim();
    if (!jobId) return json(false, null, "Missing jobId", 400);

    // ✅ SECURITY GATE: require owner token
    const ownerToken = (req.headers.get("x-owner-token") || "").trim();
    if (!ownerToken) return json(false, null, "Forbidden", 403);

    // DONE = confirm only (no side effects)
    const { data: job, error } = await supabaseAdmin
      .from("jobs")
      .select("id,status,delete_at,ttl_minutes,cleaned_at")
      .eq("id", jobId)
      .eq("owner_token", ownerToken)
      .maybeSingle();

    if (error) return json(false, null, error.message, 500);

    // ✅ Do NOT reveal existence if token mismatch
    if (!job) return json(false, null, "Forbidden", 403);

    const status = String(job.status || "").toUpperCase();

    // Already cleaned
    if (status === "CLEANED" || job.cleaned_at) {
      return json(true, {
        jobId,
        status: "CLEANED",
        cleanedAt: job.cleaned_at,
        deleteAt: job.delete_at,
        ttlMinutes: job.ttl_minutes ?? null,
        confirmed: true,
      });
    }

    const deleteAtMs = toMs(job.delete_at);

    // If delete_at missing -> download hasn't scheduled retention
    if (!deleteAtMs) {
      return json(
        false,
        { jobId, status, deleteAt: job.delete_at ?? null },
        "Not scheduled yet. Please download first.",
        409
      );
    }

    const nowMs = Date.now();
    const remainingMs = deleteAtMs - nowMs;

    if (remainingMs <= 0) {
      return json(
        false,
        {
          jobId,
          status,
          deleteAt: job.delete_at,
          now: new Date().toISOString(),
        },
        "Expired (waiting for cleanup)",
        410
      );
    }

    return json(true, {
      jobId,
      status,
      deleteAt: job.delete_at,
      ttlMinutes: job.ttl_minutes ?? null,
      remainingSeconds: Math.ceil(remainingMs / 1000),
      confirmed: true,
    });
  } catch (e: any) {
    return json(false, null, e?.message || "Server error", 500);
  }
}
