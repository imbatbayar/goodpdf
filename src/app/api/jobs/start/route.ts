import { NextResponse } from "next/server";
import { supabaseAdmin } from "@/lib/supabase/admin";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const revalidate = 0;

function json(ok: boolean, data?: any, error?: string, status = 200) {
  return NextResponse.json(
    { ok, data, error },
    { status, headers: { "cache-control": "no-store" } }
  );
}

function parseSplitMb(v: any): { value: number | null; error: string | null } {
  const n = Number(v);

  if (!Number.isFinite(n)) return { value: null, error: "Split size is required." };
  if (!Number.isInteger(n)) return { value: null, error: "Please enter a whole number (MB)." };
  if (n <= 0) return { value: null, error: "Split size must be greater than 0." };
  if (n > 500) return { value: null, error: "Max allowed size is 500MB." };

  return { value: n, error: null };
}

export async function POST(req: Request) {
  try {
    const body = await req.json().catch(() => null);
    const jobId = String(body?.jobId || "").trim();
    if (!jobId) return json(false, null, "jobId is required.", 400);

    const parsed = parseSplitMb(body?.splitMb);
    if (parsed.error) return json(false, null, parsed.error, 400);

    const splitMb = parsed.value as number;

    const cur = await supabaseAdmin
      .from("jobs")
      .select("id,status")
      .eq("id", jobId)
      .maybeSingle();

    if (cur.error) return json(false, null, cur.error.message, 500);
    if (!cur.data) return json(false, null, "Job not found.", 404);

    const curStatus = String(cur.data.status || "");
    if (!["UPLOADED", "QUEUED"].includes(curStatus)) {
      return json(false, { job: cur.data }, "Job not startable in current status.", 409);
    }

    const { data, error } = await supabaseAdmin
      .from("jobs")
      .update({
        split_mb: splitMb,

        status: "QUEUED",
        stage: "QUEUE",
        progress: 0,

        // split-only
        compress_progress: 100,
        split_progress: 0,

        // clear errors + allow new claim
        error_text: null,
        error_code: null,
        claimed_by: null,
        claimed_at: null,

        // reset run markers
        processing_started_at: null,
        done_at: null,
      })
      .eq("id", jobId)
      .in("status", ["UPLOADED", "QUEUED"])
      .select("id,status,split_mb,progress,stage")
      .maybeSingle();

    if (error) return json(false, null, error.message, 500);
    if (!data) return json(false, null, "Job not found or not startable.", 404);

    return json(true, { job: data });
  } catch (e: any) {
    return json(false, null, e?.message || "Server error", 500);
  }
}
