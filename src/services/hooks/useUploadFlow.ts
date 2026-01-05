"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { DEFAULT_SPLIT_MB } from "@/config/constants";
import { JobService } from "@/services/JobService";

// NOTE: MVP/demo user. Paid auth + quota enforcement will be added later.
const DEV_USER_ID = "00000000-0000-0000-0000-000000000001";
const LS_JOB_ID = "goodpdf_last_job_id";

type Phase = "IDLE" | "UPLOADING" | "UPLOADED" | "PROCESSING" | "READY" | "ERROR";

type ResultSummary = {
  partsCount?: number | null;
  maxPartMb?: number | null;
  targetMb?: number | null;
};

function clampPct(n: any) {
  const x = Number(n);
  if (!Number.isFinite(x)) return 0;
  return Math.max(0, Math.min(100, Math.round(x)));
}

function stageToLabel(stage?: string | null) {
  const s = String(stage || "").toUpperCase();
  if (!s) return "Working";
  if (s === "QUEUE") return "Queued";
  if (s === "DOWNLOAD") return "Downloading";
  if (s === "SPLIT") return "Splitting";
  if (s === "ZIP") return "Creating ZIP";
  if (s === "UPLOAD_OUT") return "Uploading ZIP";
  if (s === "DONE") return "Done";
  if (s === "FAILED") return "Failed";
  if (s === "CLEANUP") return "Cleaning";
  return "Working";
}

function statusToPhase(status?: string | null): Phase {
  const s = String(status || "").toUpperCase();
  if (s === "DONE") return "READY";
  if (s === "FAILED") return "ERROR";
  if (s === "UPLOADED") return "UPLOADED";
  if (s === "QUEUED" || s === "PROCESSING") return "PROCESSING";
  if (s === "CLEANED") return "IDLE";
  return "PROCESSING";
}

export function useUploadFlow() {
  const [phase, setPhase] = useState<Phase>("IDLE");
  const [busy, setBusy] = useState(false);

  const [jobId, setJobId] = useState<string | null>(null);

  const [uploadPct, setUploadPct] = useState(0);
  const [progressPct, setProgressPct] = useState(0);

  const [stageLabel, setStageLabel] = useState<string>("");

  const [error, setError] = useState<string | null>(null);

  const [result, setResult] = useState<ResultSummary | null>(null);

  const [downloadUrl, setDownloadUrl] = useState<string | null>(null);

  const pollTimer = useRef<number | null>(null);

  const stopPolling = useCallback(() => {
    if (pollTimer.current) {
      window.clearInterval(pollTimer.current);
      pollTimer.current = null;
    }
  }, []);

  const resetAll = useCallback(() => {
    stopPolling();

    setPhase("IDLE");
    setBusy(false);

    setJobId(null);
    try {
      localStorage.removeItem(LS_JOB_ID);
    } catch {}

    setUploadPct(0);
    setProgressPct(0);
    setStageLabel("");

    setError(null);
    setResult(null);
    setDownloadUrl(null);
  }, [stopPolling]);

  // Resume last jobId (best effort). Do NOT auto-start processing.
  useEffect(() => {
    try {
      const saved = localStorage.getItem(LS_JOB_ID);
      if (saved && !jobId) {
        setJobId(saved);
      }
    } catch {}
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const pollOnce = useCallback(
    async (jid: string) => {
      const st = await JobService.status(jid);

      const nextPhase = statusToPhase(st.status);
      setPhase(nextPhase);

      setProgressPct(clampPct(st.progress));
      setStageLabel(stageToLabel(st.stage));

      // result summary (shown in READY card)
      setResult({
        partsCount: st.partsCount ?? null,
        maxPartMb: st.maxPartMb ?? null,
        targetMb: st.targetMb ?? null,
      });

      if (nextPhase === "READY") {
        // backend may already return signed URL; if not, fallback to route url
        setDownloadUrl(st.downloadUrl || JobService.downloadUrl(jid));
        setBusy(false);
        stopPolling();
        return;
      }

      if (nextPhase === "ERROR") {
        setBusy(false);
        stopPolling();
        setError(st.errorText || "Failed");
        return;
      }

      // keep polling
      setBusy(true);
    },
    [stopPolling]
  );

  const startPolling = useCallback(
    (jid: string) => {
      stopPolling();
      // immediate poll
      pollOnce(jid).catch((e) => {
        setBusy(false);
        setPhase("ERROR");
        setError(e?.message || "Status failed");
      });

      pollTimer.current = window.setInterval(() => {
        pollOnce(jid).catch((e) => {
          setBusy(false);
          setPhase("ERROR");
          setError(e?.message || "Status failed");
          stopPolling();
        });
      }, 1500);
    },
    [pollOnce, stopPolling]
  );

  /**
   * Upload only (NO splitMb from UI)
   * - Uses splitMbFallback only if backend schema needs it.
   */
  const uploadOnly = useCallback(
    async (file: File, splitMbFallback: number = DEFAULT_SPLIT_MB) => {
      stopPolling();
      setError(null);
      setResult(null);
      setDownloadUrl(null);

      setBusy(true);
      setPhase("UPLOADING");
      setUploadPct(0);
      setProgressPct(0);
      setStageLabel("");

      try {
        const created = await JobService.createJob({
          file,
          userId: DEV_USER_ID,
          // compress does not exist in product; keep legacy field harmless
          quality: "ORIGINAL",
          splitMbFallback,
        });

        setJobId(created.jobId);
        try {
          localStorage.setItem(LS_JOB_ID, created.jobId);
        } catch {}

        await JobService.uploadToR2(created.uploadUrl, file, (pct) => {
          setUploadPct(clampPct(pct));
        });

        // ✅ DB дээр status=UPLOADED болгоно (Start ажиллахын тулд хэрэгтэй)
        await JobService.markUploaded(created.jobId);

        setBusy(false);
        setPhase("UPLOADED");
        setUploadPct(100);
      } catch (e: any) {
        setBusy(false);
        setPhase("ERROR");
        setError(e?.message || "Upload failed");
      }
    },
    [stopPolling]
  );

  /**
   * Start processing (✅ splitMb is REQUIRED from Start tab)
   */
  const startProcessing = useCallback(
    async (splitMb: number) => {
      const jid = jobId;
      if (!jid) {
        setPhase("ERROR");
        setError("Job is missing. Please upload again.");
        return;
      }

      setError(null);
      setResult(null);
      setDownloadUrl(null);

      setBusy(true);
      setPhase("PROCESSING");
      setProgressPct(0);
      setStageLabel("Queued");

      try {
        await JobService.start(jid, splitMb);
        startPolling(jid);
      } catch (e: any) {
        setBusy(false);
        setPhase("ERROR");
        setError(e?.message || "Start failed");
      }
    },
    [jobId, startPolling]
  );

  /**
   * Confirm done (starts cleanup timer on server side)
   */
  const confirmDone = useCallback(async () => {
    const jid = jobId;
    if (!jid) return;
    try {
      await JobService.done(jid);
    } catch {
      // best effort
    }
  }, [jobId]);

  /**
   * New file (UI convenience)
   */
  const newFile = useCallback(() => {
    resetAll();
  }, [resetAll]);

  // Derived
  const isReady = useMemo(() => phase === "READY", [phase]);

  return {
    // state
    phase,
    busy,
    isReady,
    jobId,

    uploadPct,
    progressPct,
    stageLabel,
    error,
    result,
    downloadUrl,

    // actions
    uploadOnly,
    startProcessing,
    confirmDone,
    newFile,
    resetAll,
  };
}
