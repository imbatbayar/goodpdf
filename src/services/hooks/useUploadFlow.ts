"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { DEFAULT_SPLIT_MB } from "@/config/constants";
import { JobService } from "@/services/JobService";

// NOTE: MVP/demo user. Paid auth + quota enforcement will be added later.
const DEV_USER_ID = "00000000-0000-0000-0000-000000000001";
const LS_JOB_ID = "goodpdf_last_job_id";
const LS_OWNER_TOKEN = "goodpdf_last_owner_token";
const PREFER_DIRECT_UPLOAD =
  String(process.env.NEXT_PUBLIC_PREFER_DIRECT_UPLOAD || "false").toLowerCase() ===
  "true";

/**
 * Pricing / Plans (canonical)
 * Rule: 1 successfully started file = 1 usage.
 */
type PlanTier = "BASIC" | "PRO" | "BUSINESS";
type Plan = {
  tier: PlanTier;
  name: "Basic" | "Pro" | "Business";
  expiryDays: 30 | 60 | 90;
  fileLimit: number; // 30 / 100 / 300
  cpuMinutesLimit: number; // 60 / 240 / 720
  priceMnt: number; // 5900 / 9900 / 19900
  activatedAt: number; // epoch ms (window anchor)
};

type Usage = {
  used: number;
  remaining: number;
  fileLimit: number;
  expiryDays: number;
  cpuMinutesLimit: number;
};

const LS_PLAN = "goodpdf_plan_v1";
const LS_JOB_EVENTS = "goodpdf_job_events_v1"; // number[] epoch ms (each = 1 Start)

type Mode = "SYSTEM" | "MANUAL";

type Phase = "IDLE" | "UPLOADING" | "UPLOADED" | "PROCESSING" | "READY" | "ERROR";

type ResultSummary = {
  partsCount?: number | null;
  maxPartMb?: number | null;
  targetMb?: number | null;
};

function nowMs() {
  return Date.now();
}

function safeJsonParse<T>(raw: string | null): T | null {
  if (!raw) return null;
  try {
    return JSON.parse(raw) as T;
  } catch {
    return null;
  }
}

function readPlan(): Plan | null {
  try {
    const p = safeJsonParse<Plan>(localStorage.getItem(LS_PLAN));
    if (!p) return null;
    if (
      !p.tier ||
      !p.name ||
      !p.expiryDays ||
      !p.fileLimit ||
      !p.cpuMinutesLimit ||
      !p.priceMnt ||
      !p.activatedAt
    ) {
      return null;
    }
    return p;
  } catch {
    return null;
  }
}

function readJobEvents(): number[] {
  try {
    const arr = safeJsonParse<number[]>(localStorage.getItem(LS_JOB_EVENTS));
    if (!Array.isArray(arr)) return [];
    return arr.filter((t) => Number.isFinite(t) && t > 0);
  } catch {
    return [];
  }
}

function writeJobEvents(events: number[]) {
  try {
    localStorage.setItem(LS_JOB_EVENTS, JSON.stringify(events));
  } catch {}
}

function planWindowMs(days: number) {
  return days * 24 * 60 * 60 * 1000;
}

function computeUsage(plan: Plan | null): Usage | null {
  if (!plan) return null;

  const cutoff = nowMs() - planWindowMs(plan.expiryDays);
  const used = readJobEvents().filter((t) => t >= cutoff).length;
  const remaining = Math.max(0, plan.fileLimit - used);

  return {
    used,
    remaining,
    fileLimit: plan.fileLimit,
    expiryDays: plan.expiryDays,
    cpuMinutesLimit: plan.cpuMinutesLimit,
  };
}

/**
 * Record 1 job event. Keep events bounded to 365 days to avoid growth.
 */
function recordJobEvent(ts: number) {
  const cutoff = ts - planWindowMs(365);
  const events = readJobEvents().filter((t) => t >= cutoff);
  events.push(ts);
  writeJobEvents(events);
}

function clampPct(n: any) {
  const x = Number(n);
  if (!Number.isFinite(x)) return 0;
  return Math.max(0, Math.min(100, Math.round(x)));
}

function stageToLabel(stage?: string | null) {
  const s = String(stage || "").toUpperCase();
  if (!s) return "Working";
  if (s === "QUEUE") return "Queued";
  if (s === "QUEUE_HEAVY") return "Queued (heavy)";
  if (s === "DOWNLOAD") return "Preparing input";
  if (s === "ANALYZE") return "Analyzing pages";
  if (s === "PREFLIGHT") return "Preparing plan";
  if (s === "DEFAULT") return "System optimization";
  if (s === "MANUAL") return "Manual optimization";
  if (s.startsWith("COMPRESS_")) return "Compressing";
  if (s === "PREPROCESS") return "Optimizing PDF";
  if (s === "SPLIT") return "Splitting";
  if (s === "OVERSIZE_SAFE_SPLIT") return "Safe splitting";
  if (s === "PART_FIT_9MB_FAST" || s === "PART_SURGERY") return "Fitting parts near 9MB";
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
  if (s === "CLEANED" || s === "CANCELED") return "IDLE";
  return "PROCESSING";
}

const JITTER_MS = 200;

/**
 * Polling delay: 2s base, 3s after 30s, 5s cap after 90s; jitter ±200ms.
 * Only used while phase === PROCESSING; terminal states stop polling.
 */
function nextPollDelayMs(processingElapsedMs: number) {
  let base = 2000;
  if (processingElapsedMs >= 90_000) base = 5000;
  else if (processingElapsedMs >= 30_000) base = 3000;
  const jitter = (Math.random() * 2 - 1) * JITTER_MS;
  return Math.max(500, base + jitter);
}

function maxBackoffSteps(partsCount?: number | null) {
  const n = typeof partsCount === "number" && Number.isFinite(partsCount) ? partsCount : 0;
  return Math.min(12, Math.max(6, 2 + Math.ceil(n / 6)));
}

export function useUploadFlow() {
  const [phase, setPhase] = useState<Phase>("IDLE");
  const [busy, setBusy] = useState(false);

  const [jobId, setJobId] = useState<string | null>(null);

  const [uploadPct, setUploadPct] = useState(0);
  const [progressPct, setProgressPct] = useState(0);
  const [stageCode, setStageCode] = useState<string>("");

  const [stageLabel, setStageLabel] = useState<string>("");
  const [messageLine, setMessageLine] = useState<string>("");

  const [error, setError] = useState<string | null>(null);

  const [warning, setWarning] = useState<string | null>(null);

  const [result, setResult] = useState<ResultSummary | null>(null);

  const [downloadUrl, setDownloadUrl] = useState<string | null>(null);

  // SSE (primary) and polling (fallback)
  const eventSourceRef = useRef<EventSource | null>(null);
  const pollTimer = useRef<number | null>(null);
  const pollInFlight = useRef(false);
  const pollingJobId = useRef<string | null>(null);
  const processingStartedAtRef = useRef<number>(0);

  // Latest-state refs for timers (avoid stale closures)
  const phaseRef = useRef<Phase>("IDLE");
  const stageRef = useRef<string>("");
  const partsCountRef = useRef<number | null>(null);
  const processingStartedRef = useRef(false);

  useEffect(() => {
    phaseRef.current = phase;
  }, [phase]);

  useEffect(() => {
    stageRef.current = stageLabel;
  }, [stageLabel]);

  useEffect(() => {
    partsCountRef.current = result?.partsCount ?? null;
  }, [result?.partsCount]);

  const closeEventSource = useCallback(() => {
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }
  }, []);

  const stopPolling = useCallback(() => {
    closeEventSource();
    if (pollTimer.current) {
      window.clearTimeout(pollTimer.current);
      pollTimer.current = null;
    }
    pollInFlight.current = false;
    pollingJobId.current = null;
  }, [closeEventSource]);

  const resetAll = useCallback(() => {
    stopPolling();

    setPhase("IDLE");
    setBusy(false);

    setJobId(null);
    try {
      localStorage.removeItem(LS_JOB_ID);
    } catch {}
    try {
      localStorage.removeItem(LS_OWNER_TOKEN);
    } catch {}

    setUploadPct(0);
    setProgressPct(0);
    setStageLabel("");
    setStageCode("");

    setError(null);
    setWarning(null);
    setResult(null);
    setDownloadUrl(null);
    processingStartedRef.current = false;
  }, [stopPolling]);

  // Resume last jobId (best effort). Do NOT auto-start processing.
  useEffect(() => {
    try {
      const saved = localStorage.getItem(LS_JOB_ID);
      if (saved && !jobId) {
        setJobId(saved);
      }
    } catch {}

    // owner token best-effort (JobService will read localStorage later)
    try {
      localStorage.getItem(LS_OWNER_TOKEN);
    } catch {}

    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Optional usage snapshot (for later pricing UI)
  const usage = useMemo(() => {
    return computeUsage(readPlan());
  }, [phase]);

  const assertJobQuotaOrThrow = useCallback(() => {
    const plan = readPlan();
    if (!plan) return; // MVP: if no plan set, allow
    const u = computeUsage(plan);
    if (!u) return;

    if (u.remaining <= 0) {
      throw new Error(
        `File limit reached (${u.used}/${u.fileLimit}) for ${u.expiryDays} days. Please upgrade your plan.`
      );
    }
  }, []);

  const applyStatusToState = useCallback(
    (data: any, jid: string) => {
      const st = data || {};
      let nextPhase = statusToPhase(st.status);
      if (
        processingStartedRef.current &&
        (nextPhase === "UPLOADED" || nextPhase === "IDLE")
      ) {
        nextPhase = "PROCESSING";
      }
      setPhase(nextPhase);

      const rawStage = String(st.stage || "").toUpperCase();
      let nextProgress = clampPct(st.progressPct ?? st.progress ?? 0);
      if (nextPhase === "PROCESSING") nextProgress = Math.min(99, nextProgress);
      setProgressPct((prev) => {
        if (nextPhase === "PROCESSING") return Math.max(prev, nextProgress);
        return nextProgress;
      });
      setStageCode((prev) => rawStage || prev);
      setStageLabel(stageToLabel(rawStage));
      setMessageLine(String(st.message || ""));

      setResult({
        partsCount: st.partsCount ?? null,
        maxPartMb: st.maxPartMb ?? null,
        targetMb: st.targetMb ?? null,
      });
      setWarning(st.warningText ?? null);

      if (nextPhase === "READY") {
        processingStartedRef.current = false;
        setDownloadUrl(JobService.downloadUrl(jid));
        setBusy(false);
        closeEventSource();
        stopPolling();
        return;
      }
      if (nextPhase === "ERROR") {
        processingStartedRef.current = false;
        setBusy(false);
        closeEventSource();
        stopPolling();
        setError(st.errorText || "Failed");
        return;
      }
      setBusy(true);
    },
    [closeEventSource, stopPolling]
  );

  const pollOnce = useCallback(
    async (jid: string) => {
      const st = await JobService.status(jid);
      applyStatusToState(
        {
          status: st.status,
          stage: st.stage,
          progressPct: (st as any).progressPct ?? st.progress,
          partsCount: st.partsCount,
          maxPartMb: st.maxPartMb,
          targetMb: st.targetMb,
          errorText: st.errorText,
          warningText: (st as any).warningText,
        },
        jid
      );
    },
    [applyStatusToState]
  );

  const scheduleNextPoll = useCallback(
    (jid: string) => {
      if (phaseRef.current !== "PROCESSING" || pollingJobId.current !== jid) return;
      if (typeof document !== "undefined" && document.visibilityState !== "visible") return;

      const elapsed = Date.now() - processingStartedAtRef.current;
      const delay = nextPollDelayMs(elapsed);

      pollTimer.current = window.setTimeout(async () => {
        if (phaseRef.current !== "PROCESSING" || pollingJobId.current !== jid) return;
        if (typeof document !== "undefined" && document.visibilityState !== "visible") return;

        if (pollInFlight.current) {
          scheduleNextPoll(jid);
          return;
        }

        pollInFlight.current = true;
        try {
          await pollOnce(jid);

          if (phaseRef.current === "PROCESSING" && pollingJobId.current === jid) {
            scheduleNextPoll(jid);
          }
        } catch (e: any) {
          setBusy(false);
          setPhase("ERROR");
          setError(e?.message || "Status failed");
          stopPolling();
        } finally {
          pollInFlight.current = false;
        }
      }, delay);
    },
    [pollOnce, stopPolling]
  );

  const startPolling = useCallback(
    (jid: string) => {
      if (pollingJobId.current === jid) return;
      stopPolling();
      pollingJobId.current = jid;

      pollInFlight.current = true;
      pollOnce(jid)
        .then(() => {
          if (phaseRef.current === "PROCESSING" && pollingJobId.current === jid) {
            scheduleNextPoll(jid);
          }
        })
        .catch((e: any) => {
          setBusy(false);
          setPhase("ERROR");
          setError(e?.message || "Status failed");
          stopPolling();
        })
        .finally(() => {
          pollInFlight.current = false;
        });
    },
    [pollOnce, scheduleNextPoll, stopPolling]
  );

  const startStatusStream = useCallback(
    (jid: string) => {
      if (pollingJobId.current === jid || eventSourceRef.current) return;
      stopPolling();
      pollingJobId.current = jid;

      const streamUrl = JobService.streamUrl(jid);
      if (!streamUrl.includes("ownerToken=")) {
        startPolling(jid);
        return;
      }
      try {
        const evt = new EventSource(streamUrl);
        eventSourceRef.current = evt;
        evt.onmessage = (e) => {
          try {
            const data = JSON.parse(e.data);
            applyStatusToState(data, jid);
            const status = String(data?.status || "").toUpperCase();
            if (status === "DONE" || status === "FAILED") {
              closeEventSource();
            }
          } catch (_) {}
        };
        evt.onerror = () => {
          closeEventSource();
          if (phaseRef.current === "PROCESSING" && pollingJobId.current === jid) {
            startPolling(jid);
          }
        };
      } catch (_) {
        startPolling(jid);
      }
    },
    [applyStatusToState, closeEventSource, startPolling, stopPolling]
  );

  /**
   * Upload only (NO job counting here)
   */
  const uploadOnly = useCallback(
    async (file: File, splitMbFallback: number = DEFAULT_SPLIT_MB) => {
      stopPolling();
      setError(null);
      setWarning(null);
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
          quality: "ORIGINAL",
          splitMbFallback,
        });

        setJobId(created.jobId);
        try {
          localStorage.setItem(LS_JOB_ID, created.jobId);
        } catch {}

        try {
          if (created?.ownerToken) {
            localStorage.setItem(LS_OWNER_TOKEN, created.ownerToken);
          }
        } catch {}

        // Default path stays presigned PUT (fast for internet uploads).
        // Optional direct path can be enabled via env for single-node ingest optimization.
        if (PREFER_DIRECT_UPLOAD) {
          try {
            await JobService.uploadFileDirect(created.jobId, file);
            setUploadPct(100);
          } catch {
            await JobService.uploadToR2(created.uploadUrl, file, (pct) => {
              setUploadPct(clampPct(pct));
            });
            await JobService.markUploaded(created.jobId);
          }
        } else {
          await JobService.uploadToR2(created.uploadUrl, file, (pct) => {
            setUploadPct(clampPct(pct));
          });
          await JobService.markUploaded(created.jobId);
        }

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
   * Start processing
   * ✅ Job is counted HERE (on Start success)
   */
  const startProcessing = useCallback(
    async ({
      mode,
      splitMb,
      precheckMode,
    }: {
      mode: Mode;
      splitMb: number;
      precheckMode?: "NORMAL" | "HEAVY" | "EXTREME";
    }) => {
      const jid = jobId;
      if (!jid) {
        setPhase("ERROR");
        setError("Job is missing. Please upload again.");
        return;
      }

      // Enforce job quota BEFORE starting expensive processing
      try {
        assertJobQuotaOrThrow();
      } catch (e: any) {
        setBusy(false);
        setPhase("ERROR");
        setError(e?.message || "Quota reached");
        return;
      }

      setError(null);
      setWarning(null);
      setResult(null);
      setDownloadUrl(null);

      setBusy(true);

      // Sync refs immediately so startPolling won't stall
      phaseRef.current = "PROCESSING";
      stageRef.current = "Queued";

      setPhase("PROCESSING");
      setProgressPct(0);
      setStageLabel("Queued");
      processingStartedRef.current = true;
      processingStartedAtRef.current = Date.now();

      try {
        await JobService.start(jid, splitMb, precheckMode);

        // ✅ COUNT 1 JOB only after Start succeeded
        recordJobEvent(nowMs());

        startStatusStream(jid);
      } catch (e: any) {
        processingStartedRef.current = false;
        setBusy(false);
        setPhase("ERROR");
        setError(e?.message || "Start failed");
      }
    },
    [assertJobQuotaOrThrow, jobId, startStatusStream]
  );

  /**
   * Confirm done (starts cleanup timer on server side)
   * ❌ DOES NOT count job.
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

  const newFile = useCallback(() => {
    resetAll();
  }, [resetAll]);

  const refreshStatus = useCallback(async () => {
    const jid = jobId;
    if (!jid || phaseRef.current !== "PROCESSING") return;
    try {
      await pollOnce(jid);
    } catch (_) {}
  }, [jobId, pollOnce]);

  useEffect(() => {
    const onVis = () => {
      if (document.visibilityState === "hidden") {
        stopPolling();
        return;
      }
      if (document.visibilityState === "visible" && jobId && phaseRef.current === "PROCESSING") {
        if (pollingJobId.current !== jobId) {
          startStatusStream(jobId);
        } else {
          if (eventSourceRef.current) {
            startStatusStream(jobId);
          } else {
            scheduleNextPoll(jobId);
          }
        }
      }
    };
    document.addEventListener("visibilitychange", onVis);
    return () => document.removeEventListener("visibilitychange", onVis);
  }, [jobId, startStatusStream, scheduleNextPoll, stopPolling]);

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
    stageCode,
    error,
    warning,
    result,
    downloadUrl,

    // optional usage
    usage,

    // actions
    uploadOnly,
    startProcessing,
    confirmDone,
    newFile,
    resetAll,
    refreshStatus,
  };
}

/* ------------------------------------------------------------------
  Plan helpers (pricing/auth screens later)
------------------------------------------------------------------- */

export function setActivePlan(tier: PlanTier) {
  const map: Record<PlanTier, Omit<Plan, "activatedAt">> = {
    BASIC: {
      tier: "BASIC",
      name: "Basic",
      expiryDays: 30,
      fileLimit: 30,
      cpuMinutesLimit: 60,
      priceMnt: 5900,
    },
    PRO: {
      tier: "PRO",
      name: "Pro",
      expiryDays: 60,
      fileLimit: 100,
      cpuMinutesLimit: 240,
      priceMnt: 9900,
    },
    BUSINESS: {
      tier: "BUSINESS",
      name: "Business",
      expiryDays: 90,
      fileLimit: 300,
      cpuMinutesLimit: 720,
      priceMnt: 19900,
    },
  };

  const p = map[tier];
  try {
    localStorage.setItem(LS_PLAN, JSON.stringify({ ...p, activatedAt: nowMs() }));
  } catch {}
}

export function getCurrentPlan(): Plan | null {
  return readPlan();
}

export function getCurrentUsage(): Usage | null {
  return computeUsage(readPlan());
}
