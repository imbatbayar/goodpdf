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

function normStatus(s: any) {
  return String(s || "").trim().toUpperCase();
}

/**
 * Client calls:
 *  POST /api/jobs/upload
 *  Body: { jobId: string }
 *  Purpose: mark job as UPLOADED after direct PUT to R2 succeeded.
 */
export async function POST(req: Request) {
  try {
    const body = await req.json().catch(() => ({} as any));
    const jobId = String(body.jobId || "").trim();
    if (!jobId) return json(false, null, "Missing jobId", 400);

    // ✅ SECURITY GATE: require owner token
    const ownerToken = (req.headers.get("x-owner-token") || "").trim();
    if (!ownerToken) return json(false, null, "Forbidden", 403);

    // 1) Fetch job with owner check (do NOT leak existence)
    const { data: job, error: gErr } = await supabaseAdmin
      .from("jobs")
      .select("id,status,owner_token,input_path,uploaded_at")
      .eq("id", jobId)
      .eq("owner_token", ownerToken)
      .maybeSingle();

    if (gErr) return json(false, null, gErr.message, 500);
    if (!job) return json(false, null, "Forbidden", 403);

    const st = normStatus(job.status);

    const inputKey = `${jobId}/input.pdf`;

    // 2) Idempotency:
    // - If already UPLOADED/QUEUED/PROCESSING/DONE => treat as ok (do not downgrade state)
    if (st === "UPLOADED" || st === "QUEUED" || st === "PROCESSING" || st === "DONE") {
      return json(true, { jobId, inputKey, alreadyUploaded: true });
    }
    if (st === "FAILED" || st === "CLEANED") {
      return json(false, null, `Not uploadable (status=${st})`, 409);
    }

    // 3) Update only if still UPLOADING (or CREATED)
    const { error: uErr } = await supabaseAdmin
      .from("jobs")
      .update({
        status: "UPLOADED",
        stage: "UPLOAD",
        progress: 10,
        uploaded_at: new Date().toISOString(),
        input_path: inputKey, // ✅ canonical key worker-той таарна
        error_text: null,
      })
      .eq("id", jobId)
      .eq("owner_token", ownerToken)
      .in("status", ["UPLOADING", "CREATED"]); // allow legacy states

    if (uErr) return json(false, null, uErr.message, 500);

    return json(true, { jobId, inputKey });
  } catch (e: any) {
    return json(false, null, e?.message || "Server error", 500);
  }
}

// Keep PUT returning 405 to catch old clients
export async function PUT() {
  return json(
    false,
    null,
    "Use POST with JSON body { jobId }. Client must PUT directly to signed URL.",
    405
  );
}
