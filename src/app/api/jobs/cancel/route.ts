export const dynamic = "force-dynamic";
export const revalidate = 0;

import { NextResponse } from "next/server";
import { supabaseServer } from "@/lib/supabase/server";

function json(ok: boolean, data?: any, error?: string, status = 200) {
  return NextResponse.json({ ok, data, error }, { status, headers: { "cache-control": "no-store" } });
}

const BUCKET_IN = process.env.BUCKET_IN || "job-input";
const BUCKET_OUT = process.env.BUCKET_OUT || "jobs-output";

export async function POST(req: Request) {
  try {
    const body = await req.json().catch(() => ({}));
    const jobId = body?.jobId as string | undefined;
    if (!jobId) return json(false, null, "Missing jobId", 400);

    const { data: job, error } = await supabaseServer
      .from("jobs")
      .select("id,status,input_path,output_zip_path,zip_path,cleaned_at")
      .eq("id", jobId)
      .maybeSingle();

    if (error) return json(false, null, error.message, 500);
    if (!job) return json(false, null, "Job not found", 404);

    // 1) CANCELLED mark (эхлээд)
    if (job.status !== "CLEANED") {
      const { error: u1 } = await supabaseServer
        .from("jobs")
        .update({ status: "CANCELLED" })
        .eq("id", jobId);
      if (u1) return json(false, null, u1.message, 500);
    }

    // 2) storage cleanup best-effort
    const inputPath = (job as any).input_path || null;
    const outPath = (job as any).zip_path || (job as any).output_zip_path || null;

    const errs: string[] = [];

    if (inputPath) {
      const { error: eIn } = await supabaseServer.storage.from(BUCKET_IN).remove([inputPath]);
      if (eIn) errs.push(`IN: ${eIn.message}`);
    }
    if (outPath) {
      const { error: eOut } = await supabaseServer.storage.from(BUCKET_OUT).remove([outPath]);
      if (eOut) errs.push(`OUT: ${eOut.message}`);
    }

    // 3) CLEANED mark (remove амжилттай бол)
    if (errs.length === 0) {
      const { error: u2 } = await supabaseServer
        .from("jobs")
        .update({ status: "CLEANED", cleaned_at: new Date().toISOString() })
        .eq("id", jobId);
      if (u2) return json(false, null, u2.message, 500);

      return json(true, { jobId, status: "CLEANED" });
    }

    return json(false, { jobId, status: "CANCELLED" }, `Cleanup failed: ${errs.join(" | ")}`, 500);
  } catch (e: any) {
    return json(false, null, e?.message || "Unexpected error", 500);
  }
}
