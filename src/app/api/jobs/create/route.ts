import { NextResponse } from "next/server";
import { supabaseServer } from "@/lib/supabase/server";

function json(ok: boolean, data?: any, error?: string, status = 200) {
  return NextResponse.json({ ok, data, error }, { status });
}

export async function POST(req: Request) {
  try {
    const body = await req.json();

    const fileName = String(body?.fileName || "");
    const fileSize = Number(body?.fileSize || 0);
    const quality = String(body?.quality || "GOOD"); // ORIGINAL | GOOD | MAX
    const splitMb = Number(body?.splitMb || 9);

    if (!fileName || !fileSize) return json(false, null, "Missing fileName/fileSize", 400);
    if (!["ORIGINAL", "GOOD", "MAX"].includes(quality)) return json(false, null, "Invalid quality", 400);
    if (!(splitMb >= 1 && splitMb <= 50)) return json(false, null, "Invalid splitMb", 400);

    // 1) Create a job row
    // We keep user_id server-side by requiring caller to send userId for now.
    // Later: switch to session cookie auth middleware.
    const userId = String(body?.userId || "");
    if (!userId) return json(false, null, "Missing userId (MVP)", 400);

    const { data: job, error: jobErr } = await supabaseServer
      .from("jobs")
      .insert({
        user_id: userId,
        file_name: fileName,
        file_size_bytes: fileSize,
        quality,
        split_mb: splitMb,
        status: "CREATED",
        progress: 0,
        ttl_minutes: 10,
      })
      .select("id")
      .single();

    if (jobErr) return json(false, null, `jobs insert failed: ${jobErr.message}`, 500);

    const jobId = job.id as string;

    // 2) Signed upload URL (jobs-input/{userId}/{jobId}/input.pdf)
    // Keep original extension if exists
    const ext = (fileName.split(".").pop() || "pdf").toLowerCase();
    const objectPath = `${userId}/${jobId}/input.${ext}`;

    const { data: signed, error: signErr } = await supabaseServer.storage
      .from("job-input")
      .createSignedUploadUrl(objectPath);

    if (signErr || !signed) {
      return json(false, null, `signed upload url failed: ${signErr?.message || "unknown"}`, 500);
    }

    // 3) Update job with input_path + set status UPLOADING
    const { error: updErr } = await supabaseServer
      .from("jobs")
      .update({ input_path: objectPath, status: "UPLOADING" })
      .eq("id", jobId);

    if (updErr) return json(false, null, `jobs update failed: ${updErr.message}`, 500);

    return json(true, {
      jobId,
      // Supabase signed upload uses: { url, token } and requires a PUT with header "x-upsert: true" sometimes.
      // Our UI will call /api/jobs/upload which can use these to upload.
      upload: signed,
    });
  } catch (e: any) {
    return json(false, null, e?.message || "Server error", 500);
  }
}
