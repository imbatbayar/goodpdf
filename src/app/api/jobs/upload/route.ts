import { NextResponse } from "next/server";
import { supabaseAdmin } from "@/lib/supabase/admin";
export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const revalidate = 0;

function json(ok: boolean, data?: any, error?: string, status = 200) {
  return NextResponse.json({ ok, data, error }, { status });
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
    const jobId = String(body.jobId || "");
    if (!jobId) return json(false, null, "Missing jobId", 400);

    const inputKey = `${jobId}/input.pdf`;

    const { error } = await supabaseAdmin
      .from("jobs")
      .update({
        status: "UPLOADED",
        stage: "UPLOAD",
        progress: 10,
        uploaded_at: new Date().toISOString(),
        input_path: inputKey, // ✅ canonical key worker-той таарна
        error_text: null,
      })
      .eq("id", jobId);

    if (error) return json(false, null, error.message, 500);

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
