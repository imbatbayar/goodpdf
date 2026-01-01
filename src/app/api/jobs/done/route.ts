export const dynamic = "force-dynamic";
export const revalidate = 0;

import { NextResponse } from "next/server";
import { supabaseServer } from "@/lib/supabase/server";

function json(ok: boolean, data?: any, error?: string, status = 200) {
  return NextResponse.json(
    { ok, data, error },
    { status, headers: { "cache-control": "no-store" } }
  );
}

const BUCKET_IN = process.env.BUCKET_IN || "job-input";
const BUCKET_OUT = process.env.BUCKET_OUT || "jobs-output";

// “file байхгүй” төрлийн алдааг OK гэж үзэх (idempotent)
function isIgnorableStorageError(msg?: string) {
  const m = (msg || "").toLowerCase();
  return (
    m.includes("not found") ||
    m.includes("does not exist") ||
    m.includes("no such") ||
    m.includes("404")
  );
}

function dirname(p: string) {
  const i = p.lastIndexOf("/");
  return i >= 0 ? p.slice(0, i) : "";
}

async function removeByPrefix(bucket: string, prefix: string) {
  if (!prefix) return { ok: true as const };

  // Supabase storage list: prefix доторх файлуудыг авах
  const { data, error } = await supabaseServer.storage
    .from(bucket)
    .list(prefix, { limit: 1000, offset: 0 });

  if (error) {
    // list fail бол хүчээр “ok” гэж үзэхгүй — буцааж алдаа гаргана
    return { ok: false as const, error: error.message };
  }

  const files = (data || [])
    .filter((x) => x?.name && x.name !== ".emptyFolderPlaceholder")
    .map((x) => `${prefix}/${x.name}`);

  if (files.length === 0) return { ok: true as const };

  const { error: rmErr } = await supabaseServer.storage.from(bucket).remove(files);

  if (rmErr) {
    if (isIgnorableStorageError(rmErr.message)) return { ok: true as const };
    return { ok: false as const, error: rmErr.message };
  }

  return { ok: true as const };
}

async function removeSingle(bucket: string, key: string) {
  const { error } = await supabaseServer.storage.from(bucket).remove([key]);
  if (!error) return { ok: true as const };

  if (isIgnorableStorageError(error.message)) return { ok: true as const };
  return { ok: false as const, error: error.message };
}

export async function POST(req: Request) {
  try {
    const body = await req.json().catch(() => ({}));
    const jobId = body?.jobId as string | undefined;
    if (!jobId) return json(false, null, "Missing jobId", 400);

    // 1) job авах (user_id хэрэгтэй: prefix cleanup хийх гэж)
    const { data: job, error } = await supabaseServer
      .from("jobs")
      .select(
        "id,user_id,status,input_path,output_zip_path,zip_path,expires_at,cleaned_at,confirmed_at"
      )
      .eq("id", jobId)
      .maybeSingle();

    if (error) return json(false, null, error.message, 500);
    if (!job) return json(false, null, "Job not found", 404);

    // DONE дээр л хэрэглэгч баталгаажуулна
    if (job.status !== "DONE" && job.status !== "DONE_CONFIRMED") {
      return json(false, null, `Not confirmable (status=${job.status})`, 409);
    }

    // 2) DONE -> DONE_CONFIRMED (зөвхөн нэг удаа)
    if (job.status === "DONE_CONFIRMED" && job.cleaned_at) {
      // аль хэдийн цэвэрлэсэн бол idempotent OK
      return json(true, { jobId, status: "CLEANED", cleaned_at: job.cleaned_at });
    }

    if (job.status === "DONE") {
      const { error: u1 } = await supabaseServer
        .from("jobs")
        .update({
          status: "DONE_CONFIRMED",
          confirmed_at: new Date().toISOString(),
        })
        .eq("id", jobId);

      if (u1) return json(false, null, u1.message, 500);
    }

    // 3) storage cleanup (илүү бат бөх: prefix-оор устгах)
    const userId = (job as any).user_id || "dev";

    const inputPath = (job as any).input_path || null;
    const outPath = (job as any).zip_path || (job as any).output_zip_path || null;

    const errs: string[] = [];

    // ✅ эхлээд хамгийн найдвартай: job folder prefix-оор list/remove
    // input: {userId}/{jobId}/...
    const inPrefix =
      inputPath && inputPath.includes("/")
        ? dirname(inputPath)
        : `${userId}/${jobId}`;
    const outPrefix =
      outPath && outPath.includes("/")
        ? dirname(outPath)
        : `${userId}/${jobId}`;

    const rIn = await removeByPrefix(BUCKET_IN, inPrefix);
    if (!rIn.ok) errs.push(`IN(prefix): ${rIn.error}`);

    const rOut = await removeByPrefix(BUCKET_OUT, outPrefix);
    if (!rOut.ok) errs.push(`OUT(prefix): ${rOut.error}`);

    // ✅ backup: ганц файл устгал (list ажиллахгүй тохиолдолд)
    if (errs.length > 0) {
      if (inputPath) {
        const r = await removeSingle(BUCKET_IN, inputPath);
        if (!r.ok) errs.push(`IN(single): ${r.error}`);
      }
      if (outPath) {
        const r = await removeSingle(BUCKET_OUT, outPath);
        if (!r.ok) errs.push(`OUT(single): ${r.error}`);
      }
    }

    // 4) CLEANED mark
    if (errs.length === 0) {
      const now = new Date().toISOString();
      const { error: u2 } = await supabaseServer
        .from("jobs")
        .update({ status: "CLEANED", cleaned_at: now })
        .eq("id", jobId);

      if (u2) return json(false, null, u2.message, 500);

      return json(true, { jobId, status: "CLEANED", cleaned_at: now });
    }

    // storage дээр бодит алдаа гарвал DONE_CONFIRMED хэвээр үлдээнэ
    return json(
      false,
      { jobId, status: "DONE_CONFIRMED" },
      `Cleanup failed: ${errs.join(" | ")}`,
      500
    );
  } catch (e: any) {
    return json(false, null, e?.message || "Unexpected error", 500);
  }
}
