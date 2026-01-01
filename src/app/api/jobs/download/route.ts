export const dynamic = "force-dynamic";
export const revalidate = 0;

import { NextResponse } from "next/server";
import { supabaseServer } from "@/lib/supabase/server";

// worker-local/worker.mjs-тай нэг мөр болгоно
const BUCKET_OUT = process.env.BUCKET_OUT || "jobs-output";

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);
  const jobId = searchParams.get("jobId");
  if (!jobId) {
    return NextResponse.json({ ok: false, error: "Missing jobId" }, { status: 400 });
  }

  const { data: job, error } = await supabaseServer
    .from("jobs")
    .select("id,status,zip_path,output_zip_path,expires_at,cleaned_at,confirmed_at")
    .eq("id", jobId)
    .maybeSingle();

  if (error) return NextResponse.json({ ok: false, error: error.message }, { status: 500 });
  if (!job) return NextResponse.json({ ok: false, error: "Job not found" }, { status: 404 });

  if ((job as any).status !== "DONE") {
    return NextResponse.json(
      { ok: false, error: `Not downloadable (status=${(job as any).status})` },
      { status: 409 }
    );
  }

  const outPath = (job as any).zip_path || (job as any).output_zip_path || null;
  if (!outPath) {
    return NextResponse.json({ ok: false, error: "Output not ready" }, { status: 409 });
  }

  const expiresAt = (job as any).expires_at ?? null;
  if (expiresAt && Date.now() > Date.parse(expiresAt)) {
    return NextResponse.json({ ok: false, error: "Expired" }, { status: 410 });
  }

  const { data: signed, error: signErr } = await supabaseServer.storage
    .from(BUCKET_OUT)
    .createSignedUrl(outPath, 60, { download: `goodpdf-${jobId}.zip` });

  if (signErr || !signed?.signedUrl) {
    return NextResponse.json(
      { ok: false, error: signErr?.message || "Failed to sign URL" },
      { status: 500 }
    );
  }

  // ✅ Redirect-г browser navigation-аар дагуулна (CORS асуудалгүй)
  return NextResponse.redirect(signed.signedUrl, 307);
}
