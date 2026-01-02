import { NextResponse } from "next/server";
import { supabaseServer } from "@/lib/supabase/server";

export const dynamic = "force-dynamic";
export const revalidate = 0;

function json(ok: boolean, data?: any, error?: string, status = 200) {
  return NextResponse.json({ ok, data, error }, { status });
}

/**
 * Client calls:
 *  POST /api/jobs/upload?jobId=...
 *  Body: NONE
 *  Purpose: mark job as UPLOADED (worker will pick it)
 */
export async function POST(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const jobId = String(searchParams.get("jobId") || "");
    if (!jobId) return json(false, null, "Missing jobId", 400);

    const { error } = await supabaseServer
      .from("jobs")
      .update({
        status: "UPLOADED",
        progress: 10,
        uploaded_at: new Date().toISOString(),
        error_text: null,
      })
      .eq("id", jobId);

    if (error) return json(false, null, error.message, 500);

    return json(true, { jobId });
  } catch (e: any) {
    return json(false, null, e?.message || "Server error", 500);
  }
}

// (Optional) Keep PUT returning 405 to catch old clients
export async function PUT() {
  return json(false, null, "Use POST (no body). Client must PUT directly to signed URL.", 405);
}
