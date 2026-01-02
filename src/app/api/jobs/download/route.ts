export const dynamic = "force-dynamic";
export const revalidate = 0;

import { NextResponse } from "next/server";
import { supabaseServer } from "@/lib/supabase/server";

// worker-local/worker.mjs-тай нэг мөр
const BUCKET_OUT = process.env.BUCKET_OUT || "jobs-output";

function json(ok: boolean, error?: string, status = 200) {
  return NextResponse.json(
    { ok, error },
    { status, headers: { "cache-control": "no-store" } }
  );
}

export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const jobId = searchParams.get("jobId");
    if (!jobId) return json(false, "Missing jobId", 400);

    const { data: job, error } = await supabaseServer
      .from("jobs")
      .select("id,status,output_zip_path,zip_path,expires_at,cleaned_at,confirmed_at")
      .eq("id", jobId)
      .maybeSingle();

    if (error) return json(false, error.message, 500);
    if (!job) return json(false, "Job not found", 404);

    const status = (job as any).status as string;

    const cleaned =
      (job as any).cleaned_at != null || status === "CLEANED";

    const confirmed =
      (job as any).confirmed_at != null || status === "DONE_CONFIRMED";

    const expiresAt = (job as any).expires_at ?? null;
    const expired = expiresAt ? Date.now() > Date.parse(expiresAt) : false;

    // ✅ Privacy-first: cleaned/confirmed/expired бол татах боломжгүй (Gone)
    if (cleaned) return json(false, "Expired (cleaned)", 410);
    if (confirmed) return json(false, "No longer available", 410);
    if (expired) return json(false, "Expired", 410);

    // ✅ Only downloadable when DONE
    if (status !== "DONE") {
      return json(false, `Not downloadable (status=${status})`, 409);
    }

    // ✅ Standard: output_zip_path first
    const outPath =
      (job as any).output_zip_path || (job as any).zip_path || null;

    if (!outPath) {
      // DONE мөр байлаа ч path алга бол storage delete / cleanup болсон гэж үзнэ
      return json(false, "Expired (output removed)", 410);
    }

    // Optional: storage дээр байхгүй бол signedUrl хийхгүй (410)
    // supabase storage .exists API байхгүй тул download оролдож шалгана (бага зардалтай, зөвхөн 1 файл)
    const { error: headErr } = await supabaseServer.storage
      .from(BUCKET_OUT)
      .download(outPath);

    if (headErr) {
      // аль хэдийн устсан эсвэл path буруу
      return json(false, "Expired (file missing)", 410);
    }

    const { data: signed, error: signErr } = await supabaseServer.storage
      .from(BUCKET_OUT)
      .createSignedUrl(outPath, 60, { download: `goodpdf-${jobId}.zip` });

    if (signErr || !signed?.signedUrl) {
      return json(false, signErr?.message || "Failed to sign URL", 500);
    }

    // ✅ Redirect-г browser navigation-аар дагуулна (CORS асуудалгүй)
    return NextResponse.redirect(signed.signedUrl, 307);
  } catch (e: any) {
    return json(false, e?.message || "Server error", 500);
  }
}
