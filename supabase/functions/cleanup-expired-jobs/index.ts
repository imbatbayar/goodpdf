// deno-lint-ignore-file
/// <reference lib="deno.ns" />

import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

import {
  S3Client,
  ListObjectsV2Command,
  DeleteObjectsCommand,
} from "npm:@aws-sdk/client-s3";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

// R2
const R2_ENDPOINT = Deno.env.get("R2_ENDPOINT")!;
const R2_ACCESS_KEY_ID = Deno.env.get("R2_ACCESS_KEY_ID")!;
const R2_SECRET_ACCESS_KEY = Deno.env.get("R2_SECRET_ACCESS_KEY")!;
const R2_BUCKET_IN = (Deno.env.get("R2_BUCKET_IN") || "goodpdf-in").trim();
const R2_BUCKET_OUT = (Deno.env.get("R2_BUCKET_OUT") || "goodpdf-out").trim();

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

function nowIso() {
  return new Date().toISOString();
}

function json(body: any, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "content-type": "application/json",
      "cache-control": "no-store",
      "access-control-allow-origin": "*",
      "access-control-allow-headers":
        "authorization, x-client-info, apikey, content-type",
      "access-control-allow-methods": "POST, OPTIONS",
    },
  });
}

function normStatus(s: any): string {
  return String(s || "").trim().toUpperCase();
}

function cleanKey(k: any): string | null {
  if (!k) return null;
  const s = String(k).trim();
  if (!s) return null;
  return s.replace(/^\/+/, "");
}

function uniqueStrings(xs: (string | null | undefined)[]) {
  return [...new Set(xs.filter(Boolean) as string[])];
}

const r2 = new S3Client({
  region: "auto",
  endpoint: R2_ENDPOINT,
  credentials: {
    accessKeyId: R2_ACCESS_KEY_ID,
    secretAccessKey: R2_SECRET_ACCESS_KEY,
  },
  forcePathStyle: true,
});

async function listAllKeys(bucket: string, prefix: string): Promise<string[]> {
  const keys: string[] = [];
  let token: string | undefined = undefined;

  while (true) {
    const res = await r2.send(
      new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: prefix,
        ContinuationToken: token,
      })
    );

    const contents = res.Contents || [];
    for (const obj of contents) {
      if (obj?.Key) keys.push(obj.Key);
    }

    if (!res.IsTruncated) break;
    token = res.NextContinuationToken;
    if (!token) break;
  }

  return keys;
}

async function deleteKeys(bucket: string, keys: string[]) {
  const uniq = uniqueStrings(keys).map((k) => k.replace(/^\/+/, ""));
  if (uniq.length === 0) return;

  // S3 DeleteObjects max 1000
  for (let i = 0; i < uniq.length; i += 1000) {
    const chunk = uniq.slice(i, i + 1000);
    await r2.send(
      new DeleteObjectsCommand({
        Bucket: bucket,
        Delete: { Objects: chunk.map((Key) => ({ Key })), Quiet: true },
      })
    );
  }
}

async function deleteByPrefix(bucket: string, prefix: string) {
  const keys = await listAllKeys(bucket, prefix);
  await deleteKeys(bucket, keys);
}

serve(async (req) => {
  if (req.method === "OPTIONS") return json({ ok: true }, 200);
  if (req.method !== "POST")
    return json({ ok: false, error: "Method not allowed" }, 405);

  if (
    !SUPABASE_URL ||
    !SUPABASE_SERVICE_ROLE_KEY ||
    !R2_ENDPOINT ||
    !R2_ACCESS_KEY_ID ||
    !R2_SECRET_ACCESS_KEY
  ) {
    return json({ ok: false, error: "Missing required env vars" }, 500);
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
  let skippedActive = 0;
  let lockedByOthers = 0;

  const errors: Array<{ jobId: string; error: string }> = [];

  for (const job of rows) {
    const jobId = String(job.id);
    const st = normStatus(job.status);

    // 🛡️ SAFETY #1: Ажиллаж байгаа job-уудыг ХЭЗЭЭ Ч устгахгүй
    if (st === "PROCESSING" || st === "QUEUED" || st === "UPLOADING") {
      skippedActive++;
      continue;
    }

    // 🛡️ SAFETY #2: Soft-lock
    const { data: lockRow, error: lockErr } = await supabase
      .from("jobs")
      .update({
        status: "CLEANING",
        stage: "CLEANUP",
        updated_at: now,
      })
      .eq("id", jobId)
      .is("cleaned_at", null)
      .not("delete_at", "is", null)
      .lt("delete_at", now)
      .not("status", "in", "(PROCESSING,QUEUED,UPLOADING)")
      .select("id")
      .maybeSingle();

    if (lockErr) {
      errors.push({ jobId, error: `DB lock failed: ${lockErr.message}` });
      continue;
    }
    if (!lockRow?.id) {
      lockedByOthers++;
      continue;
    }

    const stepErrors: string[] = [];

    // ✅ Canonical prefix: бүх object-ийг jobId/ доороос цэвэрлэнэ
    const prefix = `${jobId}/`;

    // (optional) job дээр хадгалсан key-үүд — prefix-ээс гадуур байвал давхар устгана
    const inputKey = cleanKey(job.input_path);
    const outKey = cleanKey(job.output_zip_path) || cleanKey(job.zip_path);

    try {
      await deleteByPrefix(R2_BUCKET_IN, prefix);
      // inputKey нь prefix-ээс гадуур байсан ч устгана
      if (inputKey && !inputKey.startsWith(prefix)) {
        await deleteKeys(R2_BUCKET_IN, [inputKey]);
      }
    } catch (e: any) {
      stepErrors.push(`IN cleanup failed: ${String(e?.message || e)}`);
    }

    try {
      await deleteByPrefix(R2_BUCKET_OUT, prefix);
      // outKey нь prefix-ээс гадуур байсан ч устгана
      if (outKey && !outKey.startsWith(prefix)) {
        await deleteKeys(R2_BUCKET_OUT, [outKey]);
      }
    } catch (e: any) {
      stepErrors.push(`OUT cleanup failed: ${String(e?.message || e)}`);
    }

    if (stepErrors.length === 0) {
      const { error: uErr } = await supabase
        .from("jobs")
        .update({
          status: "CLEANED",
          stage: "CLEANUP",
          cleaned_at: now,
          updated_at: now,
          // optional: DB дээр key-үүдийг цэвэрлэчихвэл дараа debug амар
          input_path: null,
          output_zip_path: null,
          zip_path: null,
        })
        .eq("id", jobId);

      if (uErr) {
        errors.push({ jobId, error: `DB update failed: ${uErr.message}` });
      } else {
        cleaned++;
      }
    } else {
      const msg = stepErrors.join(" | ");
      const { error: uErr } = await supabase
        .from("jobs")
        .update({
          status: "FAILED",
          stage: "CLEANUP",
          error_text: msg,
          updated_at: now,
        })
        .eq("id", jobId);

      if (uErr) {
        errors.push({
          jobId,
          error: `Cleanup failed: ${msg} | DB mark failed: ${uErr.message}`,
        });
      } else {
        errors.push({ jobId, error: `Cleanup failed: ${msg}` });
      }
    }
  }

  return json(
    { ok: true, cleaned, skippedActive, lockedByOthers, errors },
    200
  );
});
