import { NextResponse } from "next/server";
import { supabaseAdmin } from "@/lib/supabase/admin";
import { checkRateLimit, rateLimitResponse } from "@/lib/rate-limit";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const revalidate = 0;

function json(ok: boolean, data?: any, error?: string, status = 200) {
  return NextResponse.json(
    { ok, data, error },
    { status, headers: { "cache-control": "no-store" } }
  );
}

export async function POST(req: Request) {
  try {
    const rl = checkRateLimit(req, { key: "jobs:upload", limit: 40, windowMs: 60_000 });
    if (!rl.ok) return rateLimitResponse(rl.retryAfterSec);

    // ✅ JobService.markUploaded() чинь JSON явуулдаг:
    // fetch("/api/jobs/upload", { headers:{ "content-type":"application/json", "x-owner-token":... }, body: JSON.stringify({jobId}) })
    const ct = req.headers.get("content-type") || "";
    if (!ct.includes("application/json")) {
      return json(false, null, "Unsupported Content-Type. Expected application/json", 415);
    }

    const ownerToken = req.headers.get("x-owner-token") || "";
    if (!ownerToken) return json(false, null, "Missing x-owner-token", 401);

    const body = await req.json().catch(() => null);
    const jobId = String(body?.jobId || "");
    if (!jobId) return json(false, null, "Missing jobId", 400);

    // ✅ Owner-token gate: jobId таахаас хамгаална
    const { data: job, error: selErr } = await supabaseAdmin
      .from("jobs")
      .select("id,input_path,delete_at,owner_token,status")
      .eq("id", jobId)
      .eq("owner_token", ownerToken)
      .maybeSingle();

    if (selErr) return json(false, null, "Failed to validate upload", 500);
    if (!job) return json(false, null, "Forbidden", 403);

    const inputKey = String(job.input_path || "");
    if (!inputKey) return json(false, null, "Missing input_path for job", 500);

    // ✅ Mark uploaded (status API чинь UPLOADED-г хүлээдэг)
    const { error: updErr } = await supabaseAdmin
      .from("jobs")
      .update({
        status: "UPLOADED",
        stage: "UPLOAD",
        progress: 0,
        progress_pct: 0,
        uploaded_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
        error: null,
        error_text: null,
        error_code: null,
      })
      .eq("id", jobId)
      .eq("owner_token", ownerToken);

    if (updErr) return json(false, null, "Failed to mark upload", 500);

    // JobService.markUploaded() -> { ok, data:{ jobId, inputKey } } shape
    return json(true, { jobId, inputKey, expires_at: job.delete_at || null });
  } catch (e: any) {
    return json(false, null, "Upload failed", 500);
  }
}
