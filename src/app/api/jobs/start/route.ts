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
  // Хэрвээ чиний UI өөр хүрээ ашигладаг бол энэ дээд хязгаар асуудалгүйгээр өөрчлөгдөнө.
  if (n > 500) return { value: null, error: "splitMb is too large (max 500MB per part)." };

  return { value: Math.round(n * 100) / 100, error: null };
}

export async function POST(req: Request) {
  try {
    const body = await req.json().catch(() => null);

    const jobId = String(body?.jobId || "").trim();
    if (!jobId) return json(false, null, "jobId is required.", 400);

    const { value: splitMb, error: splitErr } = parseSplitMb(body?.splitMb);
    if (splitErr) return json(false, null, splitErr, 400);

    // 🔒 Refresh retention window on start
    const ttlMinutes = LOCKED_TTL_MINUTES;
    const deleteAtIso = new Date(Date.now() + ttlMinutes * 60_000).toISOString();

    // NOTE: урсгал эвдэхгүй:
    // - зөвхөн UPLOADED / QUEUED үед start зөвшөөрнө (хуучин логик)
    // - splitMb null байж болно (хуучин split_mb-аа хэвээр үлдээнэ)
    const updatePayload: Record<string, any> = {
      status: "QUEUED",
      stage: "QUEUE",
      progress: 0,

      // 🔒 retention baseline
      ttl_minutes: ttlMinutes,
      delete_at: deleteAtIso,
      cleaned_at: null,

      // optional timestamps (байхгүй column байсан ч асуудалгүй — доорх payload-оос аваад устгаж болно)
      updated_at: new Date().toISOString(),
    };

    if (splitMb !== null) updatePayload.split_mb = splitMb;

    const { data, error } = await supabaseAdmin
      .from("jobs")
      .update(updatePayload)
      .eq("id", jobId)
      .in("status", ["UPLOADED", "QUEUED"])
      .select("id,status,split_mb,progress,stage,delete_at,ttl_minutes,cleaned_at")
      .maybeSingle();

    if (error) return json(false, null, error.message, 500);
    if (!data) return json(false, null, "Job not found or not startable.", 404);

    return json(true, { job: data });
  } catch (e: any) {
    return json(false, null, e?.message || "Server error", 500);
  }
}
