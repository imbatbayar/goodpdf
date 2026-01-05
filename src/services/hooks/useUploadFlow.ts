"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { DEFAULT_SPLIT_MB } from "@/config/constants";

// NOTE: MVP/demo user. Paid auth + quota enforcement will be added later.
const DEV_USER_ID = "00000000-0000-0000-0000-000000000001";
const LS_JOB_ID = "goodpdf_last_job_id";

type Phase = "IDLE" | "UPLOADING" | "UPLOADED" | "PROCESSING" | "READY" | "ERROR";

type ResultSummary = {
  partsCount?: number | null;
  maxPartMb?: number | null;
  targetMb?: number | null;
};

type ApiResp<T> = { ok: boolean; data?: T; error?: string };

function clampPct(n: number) {
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, Math.min(100, Math.round(n)));
}

async function readJson<T>(r: Response): Promise<ApiResp<T>> {
  const text = await r.text();
  try {
    return JSON.parse(text) as ApiResp<T>;
  } catch {
    return { ok: false, error: text || `HTTP ${r.status}` };
  }
}

async function putFileWithProgress(
  url: string,
  file: File,
  onProgress?: (pct: number) => void
) {
  // XHR upload progress (fetch doesn't reliably expose upload progress)
  await new Promise<void>((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("PUT", url, true);
    xhr.upload.onprogress = (evt) => {
      if (!evt.lengthComputable) return;
      const pct = Math.round((evt.loaded / evt.total) * 100);
      onProgress?.(pct);
    };
    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) resolve();
      else reject(new Error(`Upload failed: ${xhr.status}`));
    };
    xhr.onerror = () => reject(new Error("Upload failed: network error"));
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

function normalizeStatus(raw: any) {
  return String(raw || "").toUpperCase();
}

function isTerminalStatus(status: string) {
  return ["DONE", "READY", "FAILED", "CANCELLED", "CANCELED", "CLEANED"].includes(status);
}

/**
 * Polling интервалыг ачаалал багатайгаар автоматаар сунгана.
 * - Эхэнд хурдан, дараа нь удаан.
 */
function nextPollDelayMs(attempt: number) {
  // attempt: 0,1,2...
  if (attempt < 10) return 1500; // ~15s
  if (attempt < 30) return 3000; // ~60s
  if (attempt < 60) return 7000; // ~3.5m
  return 12000; // цаашдаа 12s
}

export function useUploadFlow() {
  const [phase, setPhase] = useState<Phase>("IDLE");
  const [busy, setBusy] = useState(false);

  const [jobId, setJobId] = useState<string | null>(null);
  const [uploadPct, setUploadPct] = useState(0);
  const [progressPct, setProgressPct] = useState(0);
  const [stageLabel, setStageLabel] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<ResultSummary | null>(null);

  // ---- polling internals ----
  const pollAttemptRef = useRef(0);
  const pollTimerRef = useRef<number | null>(null);
  const inFlightAbortRef = useRef<AbortController | null>(null);

  const stopPolling = useCallback(() => {
    if (pollTimerRef.current) {
      window.clearTimeout(pollTimerRef.current);
      pollTimerRef.current = null;
    }
    pollAttemptRef.current = 0;
    if (inFlightAbortRef.current) {
      inFlightAbortRef.current.abort();
      inFlightAbortRef.current = null;
    }
  }, []);

  const resetAll = useCallback(() => {
    stopPolling();
    setJobId(null);
    setPhase("IDLE");
    setBusy(false);
    setUploadPct(0);
    setProgressPct(0);
    setStageLabel("");
    setError(null);
    setResult(null);
    try {
      localStorage.removeItem(LS_JOB_ID);
    } catch {}
  }, [stopPolling]);

  const applyReady = useCallback((d: any) => {
    setProgressPct(100);
    setResult({
      partsCount: d.partsCount ?? d.parts_count ?? null,
      maxPartMb: d.maxPartMb ?? d.max_part_mb ?? null,
      targetMb: d.targetMb ?? d.target_mb ?? d.splitMb ?? d.split_mb ?? DEFAULT_SPLIT_MB,
    });
    setPhase("READY");
    stopPolling();
  }, [stopPolling]);

  const applyFailed = useCallback((d: any, fallbackMsg?: string) => {
    setError(d?.errorText || d?.error_text || fallbackMsg || "Processing failed");
    setPhase("ERROR");
    stopPolling();
  }, [stopPolling]);

  const fetchStatusOnce = useCallback(
    async (jid: string) => {
      // өмнөх request байвал таслана (overlap хамгаалалт)
      if (inFlightAbortRef.current) inFlightAbortRef.current.abort();
      const ac = new AbortController();
      inFlightAbortRef.current = ac;

      const st = await fetch(`/api/jobs/status?jobId=${encodeURIComponent(jid)}`, {
        cache: "no-store",
        signal: ac.signal,
      }).then((r) => readJson<any>(r));

      // request дууссан тул controller-оо цэвэрлэнэ (өөр request эхлээгүй бол)
      if (inFlightAbortRef.current === ac) inFlightAbortRef.current = null;

      if (!st.ok) throw new Error(st.error || "Status failed");
      return st.data || {};
    },
    []
  );

  // ✅ Refresh болсон ч jobId-г сэргээж үргэлжлүүлэх (terminal бол сэргээхгүй)
  useEffect(() => {
    if (jobId) return;

    let cancelled = false;

    (async () => {
      try {
        const saved = localStorage.getItem(LS_JOB_ID);
        if (!saved) return;

        const d = await fetchStatusOnce(saved);
        const status = normalizeStatus(d.status);

        // Terminal job бол localStorage цэвэрлэнэ
        if (isTerminalStatus(status)) {
          try {
            localStorage.removeItem(LS_JOB_ID);
          } catch {}
          return;
        }

        if (cancelled) return;

        setJobId(saved);

        // ✅ Start дараагүй (UPLOADED) үед polling хийхгүй, зөвхөн UPLOADED гэж сэргээнэ
        if (status === "UPLOADED" || status === "UPLOADING") {
          setPhase("UPLOADED");
          stopPolling();
          return;
        }

        // ✅ Processing/Queued үед л PROCESSING болгож, polling асаах суурь тавина
        if (status === "PROCESSING" || status === "QUEUED") {
          setPhase("PROCESSING");
          return;
        }

        // status нь өөр байвал default - UPLOADED гэж тавиад polling-гүй байлгая
        setPhase("UPLOADED");
        stopPolling();
      } catch {
        try {
          localStorage.removeItem(LS_JOB_ID);
        } catch {}
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [jobId, fetchStatusOnce, stopPolling]);

  // ✅ PROCESSING үед л polling ажиллана (UPLOADED/READY үед огт polling хийхгүй)
  useEffect(() => {
    if (!jobId) return;

    // зөвхөн PROCESSING үед polling
    if (phase !== "PROCESSING") {
      stopPolling();
      return;
    }

    let disposed = false;

    const scheduleNext = () => {
      if (disposed) return;
      const delay = nextPollDelayMs(pollAttemptRef.current);
      pollTimerRef.current = window.setTimeout(() => void tick(), delay);
    };

    const tick = async () => {
      if (disposed) return;

      // Tab background үед polling pause (ачаалал бууруулна)
      if (typeof document !== "undefined" && document.visibilityState === "hidden") {
        scheduleNext();
        return;
      }

      try {
        const d = await fetchStatusOnce(jobId);
        pollAttemptRef.current += 1;

        const status = normalizeStatus(d.status);
        const stage = String(d.stage || "");

        setStageLabel(stageToLabel(stage));
        setProgressPct(clampPct(Number(d.progress ?? d.splitProgress ?? d.split_progress ?? 0)));

        if (status === "FAILED") {
          applyFailed(d, "Processing failed");
          return;
        }

        if (status === "DONE" || status === "READY") {
          applyReady(d);
          return;
        }

        // still running
        if (status === "PROCESSING" || status === "QUEUED") {
          setPhase("PROCESSING");
          scheduleNext();
          return;
        }

        // Хачин/тодорхой бус status ирвэл polling-ийг зогсоогоод хэрэглэгч Start дахин дардаг болгоё
        setPhase("UPLOADED");
        stopPolling();
      } catch (e: any) {
        // Abort бол чимээгүй өнгөрөөнө (шинэ tick эхэлж байгаа)
        if (String(e?.name || "").toLowerCase().includes("abort")) {
          scheduleNext();
          return;
        }
        applyFailed({}, e?.message || "Processing failed");
      }
    };

    // эхний tick — шууд 1 удаа
    void tick();

    // visibility change үед буцаад foreground болоход 1 удаа tick хийх (гэхдээ PROCESSING үед л)
    const onVis = () => {
      if (disposed) return;
      if (document.visibilityState === "visible") {
        // жижиг debounce шиг: өмнөх timer байвал устгаад шууд нэг tick
        if (pollTimerRef.current) window.clearTimeout(pollTimerRef.current);
        pollTimerRef.current = null;
        void tick();
      }
    };
    document.addEventListener("visibilitychange", onVis);

    return () => {
      disposed = true;
      document.removeEventListener("visibilitychange", onVis);
      stopPolling();
    };
  }, [jobId, phase, fetchStatusOnce, applyReady, applyFailed, stopPolling]);

  const uploadOnly = useCallback(
    async (file: File, splitMb = DEFAULT_SPLIT_MB) => {
      stopPolling();
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
            filename: file.name,
            bytes: file.size,
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

        // ✅ UPLOADED дээр polling хийхгүй
        setPhase("UPLOADED");
        stopPolling();
        return createdJobId;
      } catch (e: any) {
        setError(e?.message || "Upload failed");
        setPhase("ERROR");
        stopPolling();
        throw e;
      } finally {
        setBusy(false);
      }
    },
    [stopPolling]
  );

  const startProcessing = useCallback(
    async () => {
      if (!jobId) return;
      setBusy(true);
      setError(null);

      try {
        const startRes = await fetch("/api/jobs/start", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ jobId }),
        }).then((r) => readJson<{}>(r));

        if (!startRes.ok) throw new Error(startRes.error || "Start failed");

        // ✅ Start дармагц PROCESSING -> polling асна (useEffect дээр)
        pollAttemptRef.current = 0;
        setPhase("PROCESSING");
      } catch (e: any) {
        setError(e?.message || "Processing failed");
        setPhase("ERROR");
        stopPolling();
        throw e;
      } finally {
        setBusy(false);
      }
    },
    [jobId, stopPolling]
  );

  const downloadUrl = useMemo(() => {
    if (!jobId) return null;
    if (phase !== "READY") return null;
    return `/api/jobs/download?jobId=${encodeURIComponent(jobId)}`;
  }, [jobId, phase]);

  const confirmDone = useCallback(async () => {
    if (!jobId) return;
    try {
      await fetch("/api/jobs/done", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ jobId }),
      }).then((r) => r.json());
    } catch {
      // best effort
    } finally {
      resetAll();
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
