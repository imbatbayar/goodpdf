export const runtime = "nodejs";
export const dynamic = "force-dynamic";

import { supabaseAdmin } from "@/lib/supabase/admin";
import { checkRateLimit, rateLimitResponse } from "@/lib/rate-limit";

function normStatus(s: any) {
  const v = String(s || "").toUpperCase();
  return v || "UNKNOWN";
}

function clampPct(v: any, fallback = 0) {
  const n = Number(v);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(0, Math.min(100, Math.round(n)));
}

function buildStatusPayload(job: any) {
  const status = normStatus(job?.status);
  const stage = String(job?.stage || "");
  const progressPct = clampPct(job?.progress ?? job?.progress_pct ?? 0, 0);
  const expiresAt = job?.expires_at ?? job?.delete_at ?? null;
  const zipPath = job?.output_zip_path ?? job?.zip_path ?? job?.output_path ?? null;
  const warningText = status === "DONE" ? (job?.warning_text ?? null) : null;
  const errorText = status === "FAILED" ? (job?.error_text ?? job?.error ?? null) : null;

  return {
    id: job?.id,
    status,
    stage,
    progressPct,
    fileName: job?.file_name ?? null,
    fileSizeBytes: job?.file_size_bytes ?? null,
    fileType: job?.file_type ?? null,
    splitMb: job?.split_mb ?? null,
    targetMb: job?.target_mb ?? job?.split_mb ?? null,
    maxPartMb: job?.max_part_mb ?? null,
    partsCount: job?.parts_count ?? null,
    outputZipPath: zipPath,
    outputZipBytes: job?.output_zip_bytes ?? null,
    expiresAt,
    cleaned: job?.cleaned_at != null || status === "CLEANED",
    doneAt: job?.done_at ?? null,
    errorText,
    warningText,
  };
}

/** Terminal states: send final event, close stream, stop polling. */
function isTerminalStatus(status: string): boolean {
  const s = status.toUpperCase();
  return s === "DONE" || s === "FAILED" || s === "CLEANED" || s === "CANCELED";
}

/** Returns true if we should keep polling. Only QUEUED and PROCESSING continue. */
function shouldContinuePolling(status: string): boolean {
  return status === "QUEUED" || status === "PROCESSING";
}

const STREAM_POLL_MS = 2000;

/** Abortable sleep: resolves when ms elapsed OR when signal aborts. No unhandled rejections. */
function sleepAbortable(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) {
      resolve();
      return;
    }
    const t = setTimeout(resolve, ms);
    const onAbort = () => {
      clearTimeout(t);
      resolve();
    };
    signal.addEventListener("abort", onAbort, { once: true });
  });
}

/**
 * GET /api/jobs/stream?jobId=<uuid>&ownerToken=<token>
 * SSE stream of job status updates. Closes when status is terminal (DONE, FAILED, CLEANED, etc.)
 * or when client disconnects. EventSource cannot send headers, so ownerToken is passed in query.
 */
export async function GET(req: Request) {
  try {
    const rl = checkRateLimit(req, { key: "jobs:stream", limit: 20, windowMs: 60_000 });
    if (!rl.ok) return rateLimitResponse(rl.retryAfterSec);

    const url = new URL(req.url);
    const jobId = String(url.searchParams.get("jobId") || "").trim();
    const ownerToken = String(url.searchParams.get("ownerToken") || "").trim();

    if (!jobId || !ownerToken) {
      return new Response(
        JSON.stringify({ error: "Missing jobId or ownerToken" }),
        { status: 400, headers: { "content-type": "application/json" } }
      );
    }

    const signal = req.signal;
    const encoder = new TextEncoder();

    const stream = new ReadableStream({
      async start(controller) {
        let closed = false;
        const safeClose = () => {
          if (closed) return;
          closed = true;
          try {
            controller.close();
          } catch {
            /* already closed */
          }
        };

        const send = (data: object) => {
          if (closed) return;
          try {
            controller.enqueue(encoder.encode(`data: ${JSON.stringify(data)}\n\n`));
          } catch {
            safeClose();
          }
        };

        let aborted = false;
        const onAbort = () => {
          aborted = true;
          safeClose();
        };
        signal.addEventListener("abort", onAbort, { once: true });

        try {
          while (true) {
            if (aborted || signal.aborted) break;

            const { data: job, error } = await supabaseAdmin
              .from("jobs")
              .select("*")
              .eq("id", jobId)
              .eq("owner_token", ownerToken)
              .maybeSingle();

            if (aborted || signal.aborted) break;

            if (error) {
              send({ status: "FAILED", errorText: "Failed to read status" });
              break;
            }
            if (!job) {
              send({ status: "FAILED", errorText: "Not found" });
              break;
            }

            const payload = buildStatusPayload(job);
            send(payload);

            const status = normStatus(job.status);
            if (isTerminalStatus(status) || !shouldContinuePolling(status)) break;

            await sleepAbortable(STREAM_POLL_MS, signal);
          }
        } catch (e: any) {
          if (!closed) {
            send({ status: "FAILED", errorText: e?.message || "Stream error" });
          }
        } finally {
          safeClose();
        }
      },
    });

    return new Response(stream, {
      headers: {
        "content-type": "text/event-stream",
        "cache-control": "no-store",
        connection: "keep-alive",
      },
    });
  } catch (e: any) {
    return new Response(
      JSON.stringify({ error: e?.message || "Server error" }),
      { status: 500, headers: { "content-type": "application/json" } }
    );
  }
}
