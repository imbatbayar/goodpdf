"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { DEFAULT_SPLIT_MB } from "@/config/constants";

// NOTE: MVP/demo user. Paid auth + quota enforcement will be added in CHK-03.
const DEV_USER_ID = "00000000-0000-0000-0000-000000000001";
const LS_JOB_ID = "goodpdf_last_job_id";

type Phase = "IDLE" | "UPLOADING" | "UPLOADED" | "PROCESSING" | "READY" | "ERROR";

type ResultSummary = {
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
  return { ok: false, error: `Bad response (${r.status})` };
}

/**
 * XHR PUT upload (progress-тэй)
 * - presigned URL руу шууд PUT
 * IMPORTANT: Presign дээр Content-Type bind хийхгүй тул header set хийхгүй.
 */
function putFileWithProgress(url: string, file: File, onProgress?: (pct: number) => void) {
  return new Promise<void>((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("PUT", url, true);

    xhr.upload.onprogress = (evt) => {
      if (!evt.lengthComputable) return;
      const pct = (evt.loaded / evt.total) * 100;
      onProgress?.(pct);
    };

    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) return resolve();
      const detail = xhr.responseText ? ` ${xhr.responseText}` : "";
      reject(new Error(`Upload failed (PUT ${xhr.status}).${detail}`));
    };

    xhr.onerror = () => reject(new Error("Upload failed (network/CORS error)"));
    xhr.onabort = () => reject(new Error("Upload aborted"));

    xhr.send(file);
  });
}

function stageToLabel(stage: string) {
  const s = String(stage || "").toUpperCase();
  if (s === "DOWNLOAD") return "Downloading";
  if (s === "SPLIT") return "Splitting";
  if (s === "ZIP") return "Creating ZIP";
  if (s === "UPLOAD_OUT") return "Uploading ZIP";
  if (s === "DONE") return "Done";
  if (s === "QUEUE") return "Queued";
  return "Working";
}

export function useUploadFlow() {
  const [phase, setPhase] = useState<Phase>("IDLE");
  const [busy, setBusy] = useState(false);

  const [jobId, setJobId] = useState<string | null>(null);

  // progress
  const [uploadPct, setUploadPct] = useState(0);
  const [progressPct, setProgressPct] = useState(0);
  const [stageLabel, setStageLabel] = useState<string>("");

  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<ResultSummary | null>(null);

  const resetAll = useCallback(() => {
    setPhase("IDLE");
    setBusy(false);
    setJobId(null);
    setUploadPct(0);
    setProgressPct(0);
    setStageLabel("");
    setError(null);
    setResult(null);
    try {
      localStorage.removeItem(LS_JOB_ID);
    } catch {}
  }, []);

  // ✅ Refresh болсон ч jobId-г сэргээж үргэлжлүүлэх
  useEffect(() => {
    if (jobId) return;
    try {
      const saved = localStorage.getItem(LS_JOB_ID);
      if (saved) {
        setJobId(saved);
        setPhase((p) => (p === "IDLE" ? "PROCESSING" : p));
      }
    } catch {}
  }, [jobId]);

  // ✅ PROCESSING үеийн background polling (DONE болсон бол READY)
  useEffect(() => {
    if (!jobId) return;
    let stopped = false;

    const tick = async () => {
      try {
        const st = await fetch(`/api/jobs/status?jobId=${encodeURIComponent(jobId)}`, {
          cache: "no-store",
          headers: { "cache-control": "no-store" },
        }).then((r) => readJson<any>(r));

        if (!st.ok) throw new Error(st.error || "Status failed");

        const d = st.data || {};
        const status = String(d.status || "").toUpperCase();
        const stage = String(d.stage || "").toUpperCase();

        setStageLabel(stageToLabel(stage));

        const p = d.progress ?? d.progressPct ?? d.splitProgress ?? d.split_progress ?? 0;
        setProgressPct(clampPct(p));

        if (status === "FAILED") {
          setError(d.errorText || d.error_text || "Processing failed");
          setPhase("ERROR");
          stopped = true;
          return;
        }

        if (status === "CLEANED") {
          setError("Job cleaned (expired)");
          setPhase("ERROR");
          stopped = true;
          return;
        }

        if (status === "DONE") {
          setProgressPct(100);
          setResult({
            partsCount: d.partsCount ?? d.parts_count ?? null,
            maxPartMb: d.maxPartMb ?? d.max_part_mb ?? null,
            targetMb: d.targetMb ?? d.target_mb ?? d.splitMb ?? d.split_mb ?? DEFAULT_SPLIT_MB,
          });
          setPhase("READY");
          stopped = true;
          return;
        }

        // still running
        if (status === "PROCESSING" || status === "QUEUED") setPhase("PROCESSING");
      } catch (e: any) {
        setError(e?.message || "Processing failed");
        setPhase("ERROR");
        stopped = true;
      }
    };

    tick();
    const id = window.setInterval(() => {
      if (stopped) return;
      tick();
    }, 1500);

    return () => window.clearInterval(id);
  }, [jobId]);

  /**
   * 1) Upload ONLY:
   * - POST /api/jobs/create -> presigned PUT URL + jobId
   * - PUT upload to R2 (XHR progress)
   * - POST /api/jobs/upload -> status UPLOADED
   */
  const uploadOnly = useCallback(async (file: File, splitMb: number) => {
    setBusy(true);
    setError(null);
    setResult(null);
    setUploadPct(0);
    setProgressPct(0);
    setStageLabel("");
    setPhase("UPLOADING");

    try {
      const createResp = await fetch("/api/jobs/create", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          userId: DEV_USER_ID,
          fileName: file.name,
          fileType: file.type || "application/pdf",
          fileSizeBytes: file.size,
          splitMb,
        }),
      }).then((r) => readJson<{ jobId: string; upload: { url: string } }>(r));

      if (!createResp.ok) throw new Error(createResp.error || "Create job failed");

      const createdJobId = createResp.data!.jobId;
      const uploadUrl = createResp.data!.upload.url;

      setJobId(createdJobId);
      try {
        localStorage.setItem(LS_JOB_ID, createdJobId);
      } catch {}

      await putFileWithProgress(uploadUrl, file, (p) => setUploadPct(clampPct(p)));
      setUploadPct(100);

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
   * - POST /api/jobs/start { jobId, splitMb }
   */
  const startProcessing = useCallback(
    async (opts: { splitMb: number }) => {
      if (!jobId) throw new Error("Missing jobId");

      setBusy(true);
      setError(null);
      setResult(null);
      setProgressPct(0);
      setStageLabel("Queued");
      setPhase("PROCESSING");

      try {
        const startRes = await fetch("/api/jobs/start", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ jobId, splitMb: opts.splitMb }),
        }).then((r) => readJson<{}>(r));

        if (!startRes.ok) throw new Error(startRes.error || "Start failed");
        return;
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
      // best effort
    }
  }, [jobId, resetAll]);

  return {
    phase,
    busy,
    jobId,
    uploadPct,
    progressPct,
    stageLabel,
    error,
    result,
    downloadUrl,

    uploadOnly,
    startProcessing,
    confirmDone,
    newFile,
    resetAll,
  };
}
