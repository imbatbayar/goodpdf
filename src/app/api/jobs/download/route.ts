export const dynamic = "force-dynamic";
export const revalidate = 0;
export const runtime = "nodejs";

import { NextResponse } from "next/server";
import { supabaseAdmin } from "@/lib/supabase/admin";
import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { checkRateLimit, rateLimitResponse } from "@/lib/rate-limit";

function json(ok: boolean, data?: any, error?: string, status = 200) {
  return NextResponse.json(
    { ok, data, error },
    { status, headers: { "cache-control": "no-store" } }
  );
}

// 🔒 LOCKED TTL (privacy-first)
const LOCKED_TTL_MINUTES = 10;

// ---- R2 ----
const R2_ENDPOINT = process.env.R2_ENDPOINT!;
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID!;
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY!;
const R2_BUCKET_OUT = process.env.R2_BUCKET_OUT || process.env.R2_BUCKET || "goodpdf-out";

if (!R2_ENDPOINT || !R2_ACCESS_KEY_ID || !R2_SECRET_ACCESS_KEY) {
  throw new Error("Missing R2_ENDPOINT / R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY");
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

function parseIsoMs(v: any): number | null {
  if (!v) return null;
  const t = Date.parse(String(v));
  return Number.isFinite(t) ? t : null;
}

function cleanKey(k: any): string | null {
  if (!k) return null;
  const s = String(k).trim();
  if (!s) return null;
  const normalized = s.replace(/^\/+/, "");
  if (normalized.includes("..")) return null;
  if (normalized.includes("\\")) return null;
  if (!/^[A-Za-z0-9._/-]+$/.test(normalized)) return null;
  return normalized;
}

export async function GET(req: Request) {
  try {
    const rl = checkRateLimit(req, { key: "jobs:download", limit: 30, windowMs: 60_000 });
    if (!rl.ok) return rateLimitResponse(rl.retryAfterSec);

    const { searchParams } = new URL(req.url);
    const jobId = (searchParams.get("jobId") || "").trim();
    const debug = (searchParams.get("debug") || "").trim() === "1";

    if (!jobId) return json(false, null, "Missing jobId", 400);

    // Header-only token to avoid leaking secrets through URLs.
    const ownerToken = (req.headers.get("x-owner-token") || "").trim();

    if (!ownerToken) return json(false, null, "Forbidden", 403);

    // ✅ Owner match — token зөрвөл job “байгаа/байхгүй”-г задруулахгүй
    const { data: job, error } = await supabaseAdmin
      .from("jobs")
      .select("id,status,output_zip_path,zip_path,delete_at,cleaned_at")
      .eq("id", jobId)
      .eq("owner_token", ownerToken)
      .maybeSingle();

    if (error) return json(false, null, "Failed to read job", 500);
    if (!job) return json(false, null, "Forbidden", 403);

    const status = String(job.status || "").toUpperCase();

    // Only DONE is downloadable
    if (status !== "DONE") {
      return json(false, null, "Not downloadable yet", 409);
    }

    // Already cleaned
    if (job.cleaned_at) {
      return json(
        false,
        debug ? { reason: "cleaned_at", cleaned_at: job.cleaned_at } : null,
        "Expired (cleaned)",
        410
      );
    }

    // ✅ Download дарсан мөчөөс countdown эхэлнэ: delete_at = now + 10min
    const newDeleteAtIso = new Date(Date.now() + LOCKED_TTL_MINUTES * 60_000).toISOString();

    const { data: upd, error: uErr } = await supabaseAdmin
      .from("jobs")
      .update({
        ttl_minutes: LOCKED_TTL_MINUTES,
        delete_at: newDeleteAtIso,
        updated_at: new Date().toISOString(),
      })
      .eq("id", jobId)
      .eq("owner_token", ownerToken) // ✅ extra safety
      .select("delete_at")
      .maybeSingle();

    if (uErr) {
      return json(
        false,
        debug ? { reason: "delete_at_update_failed" } : null,
        "Failed to schedule cleanup",
        500
      );
    }

    const effectiveDeleteAt = upd?.delete_at || newDeleteAtIso;

    const deleteAtMs = parseIsoMs(effectiveDeleteAt);
    if (deleteAtMs && Date.now() > deleteAtMs) {
      return json(
        false,
        debug
          ? { reason: "delete_at_passed", delete_at: effectiveDeleteAt, now: new Date().toISOString() }
          : null,
        "Expired",
        410
      );
    }

    const outKey = cleanKey(job.output_zip_path) || cleanKey(job.zip_path);
    if (!outKey) {
      return json(
        false,
        debug ? { reason: "missing_or_invalid_output_zip_path" } : null,
        "Output not available",
        410
      );
    }

    // 1) R2 signed URL (GetObject)
    const signedUrl = await getSignedUrl(
      r2,
      new GetObjectCommand({
        Bucket: R2_BUCKET_OUT,
        Key: outKey,
        ResponseContentDisposition: `attachment; filename="goodpdf-${jobId}.zip"`,
      }),
      { expiresIn: 60 * LOCKED_TTL_MINUTES }
    );

    // 2) Stream via server
    const r = await fetch(signedUrl);

    if (!r.ok || !r.body) {
      return json(
        false,
        debug ? { reason: "r2_fetch_failed", status: r.status } : null,
        "Failed to fetch zip from R2",
        500
      );
    }

    const contentLength = r.headers.get("content-length");

    const headers: Record<string, string> = {
      "Content-Type": "application/zip",
      "Content-Disposition": `attachment; filename="goodpdf-${jobId}.zip"`,
      "Cache-Control": "no-store",
      // ops/debug: network дээр харагдана
      "x-goodpdf-delete-at": String(effectiveDeleteAt),
    };
    if (contentLength) headers["Content-Length"] = contentLength;

    return new NextResponse(r.body, { status: 200, headers });
  } catch (e: any) {
    return json(false, null, "Server error", 500);
  }
}
