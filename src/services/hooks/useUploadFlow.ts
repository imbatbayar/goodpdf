"use client";

import { useCallback, useMemo, useState } from "react";
import type { QualityMode } from "@/domain/jobs/quality";
import { DEFAULT_SPLIT_MB } from "@/config/constants";

const DEV_USER_ID = "00000000-0000-0000-0000-000000000001";

type Phase = "IDLE" | "UPLOADING" | "UPLOADED" | "PROCESSING" | "READY" | "ERROR";

type ResultSummary = {
  compressedMb?: number | null; // ORIGINAL дээр null байж болно
  partsCount?: number | null;
  maxPartMb?: number | null;
  targetMb?: number | null;
};

type ApiResp<T> = { ok: boolean; data?: T; error?: string };

function clampPct(n: any, fallback = 0) {
  const x = Number(n);
  if (!Number.isFinite(x)) return fallback;
  return Math.max(0, Math.min(100, Math.round(x)));
}

async function readJson<T>(r: Response): Promise<ApiResp<T>> {
  const j = (await r.json().catch(() => null)) as any;
  if (j && typeof j.ok === "boolean") return j;
  // fallback (энэ тохиолдол гарах ёсгүй)
  return { ok: false, error: `Bad response (${r.status})` };
}

/**
 * XHR PUT upload (progress-тэй)
 * - presigned URL руу шууд PUT
 */
function putFileWithProgress(url: string, file: File, onProgress?: (pct: number) => void) {
  return new Promise<void>((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("PUT", url, true);

    // R2 presigned PUT дээр content-type зөрөхөөр 403 болох эрсдэлээс хамгаалж:
    // create дээр "contentType" шаардсан бол яг тэрийг тааруулах хэрэгтэй.
    // Одоогийн canonical: application/pdf
    xhr.setRequestHeader("Content-Type", file.type || "application/pdf");

    xhr.upload.onprogress = (evt) => {
      if (!evt.lengthComputable) return;
      const pct = (evt.loaded / evt.total) * 100;
      onProgress?.(pct);
    };

    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) return resolve();
      reject(new Error(`Upload failed (PUT ${xhr.status})`));
    };

    xhr.onerror = () => reject(new Error("Upload failed (network error)"));
    xhr.onabort = () => reject(new Error("Upload aborted"));

    xhr.send(file);
  });
}

export function useUploadFlow() {
  const [phase, setPhase] = useState<Phase>("IDLE");
  const [busy, setBusy] = useState(false);

  const [jobId, setJobId] = useState<string | null>(null);

  // progress
  const [uploadPct, setUploadPct] = useState(0);
  const [compressPct, setCompressPct] = useState(0);
  const [splitPct, setSplitPct] = useState(0);

  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<ResultSummary | null>(null);

  const resetAll = useCallback(() => {
    setPhase("IDLE");
    setBusy(false);
    setJobId(null);
    setUploadPct(0);
    setCompressPct(0);
    setSplitPct(0);
    setError(null);
    setResult(null);
  }, []);

  /**
   * 1) Upload ONLY:
   * - POST /api/jobs/create -> presigned PUT URL + jobId
   * - PUT upload to R2 (XHR progress)
   * - POST /api/jobs/upload -> status UPLOADED
   */
  const uploadOnly = useCallback(async (file: File, quality: QualityMode, splitMb: number) => {
    setBusy(true);
    setError(null);
    setResult(null);
    setUploadPct(0);
    setCompressPct(0);
    setSplitPct(0);
    setPhase("UPLOADING");

    try {
      // 1) create job (server returns presigned PUT)
      const createResp = await fetch("/api/jobs/create", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          userId: DEV_USER_ID,
          fileName: file.name,
          fileType: file.type || "application/pdf",
          fileSizeBytes: file.size,
          quality,
          splitMb,
        }),
      }).then((r) => readJson<{ jobId: string; upload: { url: string } }>(r));

      if (!createResp.ok) throw new Error(createResp.error || "Create job failed");

      const createdJobId = createResp.data!.jobId;
      const uploadUrl = createResp.data!.upload.url;

      setJobId(createdJobId);

      // 2) direct PUT upload with progress
      await putFileWithProgress(uploadUrl, file, (p) => setUploadPct(clampPct(p)));

      setUploadPct(100);

      // 3) mark uploaded
      const upRes = await fetch("/api/jobs/upload", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ jobId: createdJobId }),
      }).then((r) => readJson<{}>(r));

      if (!upRes.ok) throw new Error(upRes.error || "Mark uploaded failed");

      setPhase("UPLOADED");
      return createdJobId;
    } catch (e: any) {
      setError(e?.message || "Upload failed");
      setPhase("ERROR");
      throw e;
    } finally {
      setBusy(false);
    }
  }, []);

  /**
   * 2) Start PROCESSING:
   * - POST /api/jobs/start { jobId, quality, splitMb }  => UPLOADED -> QUEUED
   * - poll /api/jobs/status until DONE
   *
   * Stage progress canonical:
   * - compressPct <- data.compress_progress
   * - splitPct    <- data.split_progress
   */
  const startProcessing = useCallback(
    async (opts: { quality: QualityMode; splitMb: number }) => {
      if (!jobId) throw new Error("Missing jobId");

      setBusy(true);
      setError(null);
      setResult(null);
      setCompressPct(0);
      setSplitPct(0);
      setPhase("PROCESSING");

      try {
        // 1) start -> QUEUED
        const startRes = await fetch("/api/jobs/start", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            jobId,
            quality: opts.quality,
            splitMb: opts.splitMb,
          }),
        }).then((r) => readJson<{}>(r));

        if (!startRes.ok) throw new Error(startRes.error || "Start failed");

        // 2) poll status until DONE / FAILED / CLEANED
        const deadline = Date.now() + 10 * 60 * 1000; // 10 минут
        while (Date.now() < deadline) {
          const st = await fetch(`/api/jobs/status?jobId=${encodeURIComponent(jobId)}`, {
            cache: "no-store",
            headers: { "cache-control": "no-store" },
          }).then((r) => readJson<any>(r));

          if (!st.ok) throw new Error(st.error || "Status failed");

          const d = st.data || {};
          const status = String(d.status || "").toUpperCase();
          const stage = String(d.stage || "").toUpperCase();

          // canonical stage progress
          const cp = d.compress_progress ?? d.compressProgress ?? null;
          const sp = d.split_progress ?? d.splitProgress ?? null;

          // ORIGINAL үед worker дээр compress_progress=100 тавьдаг.
          if (cp != null) setCompressPct(clampPct(cp));
          else if (stage === "COMPRESS") setCompressPct(0);

          if (sp != null) setSplitPct(clampPct(sp));
          else if (stage === "SPLIT") setSplitPct(clampPct(d.progress ?? 0));

          if (status === "FAILED") {
            throw new Error(d.error_text || d.errorText || "Processing failed");
          }

          if (status === "CLEANED") {
            throw new Error("Job cleaned (expired)");
          }

          if (status === "DONE") {
            setCompressPct(100);
            setSplitPct(100);

            setResult({
              compressedMb: d.compressed_mb ?? d.compressedMb ?? null,
              partsCount: d.parts_count ?? d.partsCount ?? null,
              maxPartMb: d.max_part_mb ?? d.maxPartMb ?? null,
              targetMb: d.target_mb ?? d.targetMb ?? opts.splitMb ?? DEFAULT_SPLIT_MB,
            });

            setPhase("READY");
            return;
          }

          // interval
          await new Promise((r) => setTimeout(r, 1000));
        }

        throw new Error("Timeout: processing took too long");
      } catch (e: any) {
        setError(e?.message || "Processing failed");
        setPhase("ERROR");
        throw e;
      } finally {
        setBusy(false);
      }
    },
    [jobId]
  );

  const downloadUrl = useMemo(() => {
    if (!jobId) return null;
    return `/api/jobs/download?jobId=${encodeURIComponent(jobId)}`;
  }, [jobId]);

  const triggerDownload = useCallback(() => {
    if (!downloadUrl) return;
    window.open(downloadUrl, "_blank", "noopener,noreferrer");
  }, [downloadUrl]);

  const confirmDone = useCallback(async () => {
    if (!jobId) return;
    setBusy(true);
    try {
      const res = await fetch("/api/jobs/done", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ jobId }),
      }).then((r) => readJson<{}>(r));

      if (!res.ok) throw new Error(res.error || "Confirm failed");

      resetAll();
    } catch (e: any) {
      setError(e?.message || "Confirm failed");
      setPhase("ERROR");
    } finally {
      setBusy(false);
    }
  }, [jobId, resetAll]);

  const newFile = useCallback(async () => {
    const id = jobId;
    resetAll();
    if (!id) return;

    try {
      await fetch("/api/jobs/cancel", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ jobId: id }),
      }).then((r) => r.json());
    } catch {
      // cancel нь best effort
    }
  }, [jobId, resetAll]);

  return {
    // state
    phase,
    busy,
    jobId,
    uploadPct,
    compressPct,
    splitPct,
    error,
    result,
    downloadUrl,

    // actions
    uploadOnly,
    startProcessing,
    triggerDownload,
    confirmDone,
    newFile,
    resetAll,
  };
}
