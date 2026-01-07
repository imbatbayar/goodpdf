// deno-lint-ignore-file
/// <reference lib="deno.ns" />

import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

// NOTE: Env-үүдийг эвдэхгүй (чи одоо ашиглаж байгаа нэршлээр нь үлдээлээ)
const BUCKET_IN = Deno.env.get("BUCKET_IN") || "job-input";
const BUCKET_OUT = Deno.env.get("BUCKET_OUT") || "jobs-output";

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

function nowIso() {
  return new Date().toISOString();
}

// Supabase Storage remove() дээр "байхгүй object" үед ирдэг алдаануудыг идемпотент байдлаар ignore хийх
function isIgnorable(err: any): boolean {
  const msg = String(err?.message || err || "").toLowerCase();
  if (!msg) return false;

  // common "not found" / "missing" patterns
  return (
    msg.includes("not found") ||
    msg.includes("does not exist") ||
    msg.includes("no such") ||
    msg.includes("404") ||
    msg.includes("object not found") ||
    msg.includes("key not found")
  );
}

function cleanKey(k: any): string | null {
  if (!k) return null;
  const s = String(k).trim();
  if (!s) return null;
  // safety: remove leading slashes
  return s.replace(/^\/+/, "");
}

async function removeSingle(bucket: string, key: string) {
  const { error } = await supabase.storage.from(bucket).remove([key]);
  if (error && !isIgnorable(error)) throw error;
}

function json(body: any, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "content-type": "application/json",
      "cache-control": "no-store",
      "access-control-allow-origin": "*",
      "access-control-allow-headers": "authorization, x-client-info, apikey, content-type",
      "access-control-allow-methods": "POST, OPTIONS",
    },
  });
}

serve(async (req) => {
  if (req.method === "OPTIONS") return json({ ok: true }, 200);
  if (req.method !== "POST") return json({ ok: false, error: "Method not allowed" }, 405);

  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    return json({ ok: false, error: "Missing SUPABASE env vars" }, 500);
  }

  const now = nowIso();

  // ✅ delete_at хүрмэгц (DONE эсэхээс үл хамааран) цэвэрлэнэ
  const { data: jobs, error: qErr } = await supabase
    .from("jobs")
    .select("id,status,input_path,zip_path,output_zip_path,delete_at,cleaned_at")
    .is("cleaned_at", null)
    .not("delete_at", "is", null)
    .lt("delete_at", now)
    .limit(100);

  if (qErr) return json({ ok: false, error: qErr.message }, 500);

  const rows = jobs || [];
  let cleaned = 0;
  const errors: Array<{ jobId: string; error: string }> = [];

  for (const job of rows) {
    const jobId = String(job.id);

    // ✅ Input key canonical: DB-д input_path байвал тэрийг, үгүй бол fallback "<jobId>/input.pdf"
    const inputKey =
      cleanKey(job.input_path) || `${jobId}/input.pdf`;

    // ✅ Output key: output_zip_path || zip_path (байхгүй байж болно)
    const outKey =
      cleanKey(job.output_zip_path) || cleanKey(job.zip_path);

    const stepErrors: string[] = [];

    // 1) delete input
    try {
      await removeSingle(BUCKET_IN, inputKey);
    } catch (e: any) {
      stepErrors.push(`IN delete failed: ${String(e?.message || e)}`);
    }

    // 2) delete output
    if (outKey) {
      try {
        await removeSingle(BUCKET_OUT, outKey);
      } catch (e: any) {
        stepErrors.push(`OUT delete failed: ${String(e?.message || e)}`);
      }
    }

    // 3) mark cleaned (status үл хамаарна)
    if (stepErrors.length === 0) {
      const { error: uErr } = await supabase
        .from("jobs")
        .update({
          status: "CLEANED",
          stage: "CLEANUP",
          cleaned_at: now,
          updated_at: now,
        })
        .eq("id", jobId);

      if (uErr) {
        errors.push({ jobId, error: `DB update failed: ${uErr.message}` });
      } else {
        cleaned++;
      }
    } else {
      errors.push({ jobId, error: stepErrors.join(" | ") });
    }
  }

  return json({ ok: true, cleaned, errors }, 200);
});
