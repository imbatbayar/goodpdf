// deno-lint-ignore-file
/// <reference lib="deno.ns" />


import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const BUCKET_IN = Deno.env.get("BUCKET_IN") || "job-input";
const BUCKET_OUT = Deno.env.get("BUCKET_OUT") || "jobs-output";

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

function isIgnorable(msg = "") {
  const m = msg.toLowerCase();
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
  if (!prefix) return { ok: true };
  const { data, error } = await supabase.storage.from(bucket).list(prefix, {
    limit: 1000,
    offset: 0,
  });
  if (error) return { ok: false, error: error.message };

  const files = (data || [])
    .filter((x) => x?.name && x.name !== ".emptyFolderPlaceholder")
    .map((x) => `${prefix}/${x.name}`);

  if (files.length === 0) return { ok: true };

  const { error: rmErr } = await supabase.storage.from(bucket).remove(files);
  if (rmErr && !isIgnorable(rmErr.message))
    return { ok: false, error: rmErr.message };

  return { ok: true };
}

async function removeSingle(bucket: string, key: string) {
  const { error } = await supabase.storage.from(bucket).remove([key]);
  if (error && !isIgnorable(error.message))
    return { ok: false, error: error.message };
  return { ok: true };
}

serve(async () => {
  const now = new Date().toISOString();

  const { data: jobs, error } = await supabase
    .from("jobs")
    .select("id,user_id,status,input_path,zip_path,output_zip_path")
    .in("status", ["DONE", "DONE_CONFIRMED"])
    .lt("expires_at", now)
    .limit(100);

  if (error) {
    return new Response(
      JSON.stringify({ ok: false, error: error.message }),
      { status: 500 }
    );
  }

  let cleaned = 0;
  const errors: any[] = [];

  for (const job of jobs || []) {
    const inputPath = job.input_path || null;
    const outPath = job.zip_path || job.output_zip_path || null;

    const inPrefix =
      inputPath && inputPath.includes("/")
        ? dirname(inputPath)
        : `${job.user_id}/${job.id}`;
    const outPrefix =
      outPath && outPath.includes("/")
        ? dirname(outPath)
        : `${job.user_id}/${job.id}`;

    let errs: string[] = [];

    const rIn = await removeByPrefix(BUCKET_IN, inPrefix);
    if (!rIn.ok) errs.push(`IN(prefix): ${rIn.error}`);

    const rOut = await removeByPrefix(BUCKET_OUT, outPrefix);
    if (!rOut.ok) errs.push(`OUT(prefix): ${rOut.error}`);

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

    if (errs.length === 0) {
      const { error: u } = await supabase
        .from("jobs")
        .update({
          status: "CLEANED",
          cleaned_at: new Date().toISOString(),
        })
        .eq("id", job.id);

      if (!u) cleaned++;
      else errors.push({ jobId: job.id, error: u.message });
    } else {
      errors.push({ jobId: job.id, error: errs.join(" | ") });
    }
  }

  return new Response(
    JSON.stringify({ ok: true, cleaned, errors }),
    { headers: { "content-type": "application/json" } }
  );
});
