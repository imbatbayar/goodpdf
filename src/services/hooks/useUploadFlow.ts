"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { DEFAULT_SPLIT_MB } from "@/config/constants";
import { JobService } from "@/services/JobService";

// NOTE: MVP/demo user. Paid auth + quota enforcement will be added later.
const DEV_USER_ID = "00000000-0000-0000-0000-000000000001";
const LS_JOB_ID = "goodpdf_last_job_id";
const LS_OWNER_TOKEN = "goodpdf_last_owner_token";

/**
 * Pricing / Plans (LOCKED)
 * - 90 days:  25 jobs —  7,000₮
 * - 180 days: 100 jobs — 29,000₮
 * - 365 days: 250 jobs — 59,000₮
 *
 * Rule: 1 job = Start (processing) success.
 */
type PlanTier = "P90" | "P180" | "P365";
type Plan = {
  tier: PlanTier;
  days: 90 | 180 | 365;
  quotaJobs: number; // 25 / 100 / 250
  priceMnt: number; // 7000 / 29000 / 59000
  activatedAt: number; // epoch ms (window anchor)
};

type Usage = {
  used: number;
  remaining: number;
  quota: number;
  windowDays: number;
};

const LS_PLAN = "goodpdf_plan_v1";
const LS_JOB_EVENTS = "goodpdf_job_events_v1"; // number[] epoch ms (each = 1 Start)

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
    if (!p.tier || !p.days || !p.quotaJobs || !p.priceMnt || !p.activatedAt) return null;
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

  const cutoff = nowMs() - planWindowMs(plan.days);
  const used = readJobEvents().filter((t) => t >= cutoff).length;
  const remaining = Math.max(0, plan.quotaJobs - used);

  return { used, remaining, quota: plan.quotaJobs, windowDays: plan.days };
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
  if (s === "DOWNLOAD") return "Downloading";
  if (s === "PREPROCESS") return "Optimizing PDF";
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

/**
 * Adaptive polling delay (production-safe)
 */
function nextPollDelayMs(phase: Phase, stageLabel: string, backoffStep: number) {
  let ms = 900;
  if (phase !== "PROCESSING") ms = 1200;

  const st = (stageLabel || "").toLowerCase();
  if (st.includes("queued")) ms = 1100;
  if (st.includes("downloading")) ms = 1200;
  if (st.includes("optimizing")) ms = 1400;
  if (st.includes("splitting")) ms = 1100;
  if (st.includes("creating zip") || st.includes("uploading zip")) ms = 1600;

  ms += Math.min(1200, Math.max(0, backoffStep) * 200);

  if (typeof document !== "undefined" && document.visibilityState === "hidden") {
    ms = Math.max(ms, 2500);
  }

  ms += Math.floor(Math.random() * 181);
  return ms;
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

  const [stageLabel, setStageLabel] = useState<string>("");

  const [error, setError] = useState<string | null>(null);

  const [result, setResult] = useState<ResultSummary | null>(null);

  const [downloadUrl, setDownloadUrl] = useState<string | null>(null);

  // Polling refs (NO overlap, setTimeout-based)
  const pollTimer = useRef<number | null>(null);
  const pollInFlight = useRef(false);
  const pollBackoff = useRef(0);

  // Latest-state refs for timers (avoid stale closures)
  const phaseRef = useRef<Phase>("IDLE");
  const stageRef = useRef<string>("");
  const partsCountRef = useRef<number | null>(null);

  useEffect(() => {
    phaseRef.current = phase;
  }, [phase]);

  useEffect(() => {
    stageRef.current = stageLabel;
  }, [stageLabel]);

  useEffect(() => {
    partsCountRef.current = result?.partsCount ?? null;
  }, [result?.partsCount]);

  const stopPolling = useCallback(() => {
    if (pollTimer.current) {
      window.clearTimeout(pollTimer.current);
      pollTimer.current = null;
    }
    pollInFlight.current = false;
    pollBackoff.current = 0;
  }, []);

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
        `Quota reached (${u.used}/${u.quota}). Please upgrade or wait for your ${u.windowDays}-day window to refresh.`
      );
    }
  }, []);

  const pollOnce = useCallback(
    async (jid: string) => {
      const st = await JobService.status(jid);

      const nextPhase = statusToPhase(st.status);
      setPhase(nextPhase);

      setProgressPct(clampPct(st.progress));
      setStageLabel(stageToLabel(st.stage));

      setResult({
        partsCount: st.partsCount ?? null,
        maxPartMb: st.maxPartMb ?? null,
        targetMb: st.targetMb ?? null,
      });

      if (nextPhase === "READY") {
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

      setBusy(true);
    },
    [stopPolling]
  );

  const scheduleNextPoll = useCallback(
    (jid: string) => {
      if (phaseRef.current !== "PROCESSING") return;

      const delay = nextPollDelayMs(phaseRef.current, stageRef.current, pollBackoff.current);

      pollTimer.current = window.setTimeout(async () => {
        if (phaseRef.current !== "PROCESSING") return;

        if (pollInFlight.current) {
          scheduleNextPoll(jid);
          return;
        }

        pollInFlight.current = true;
        try {
          await pollOnce(jid);

          if (phaseRef.current === "PROCESSING") {
            const cap = maxBackoffSteps(partsCountRef.current);
            pollBackoff.current = Math.min(cap, pollBackoff.current + 1);
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
      stopPolling();
      pollBackoff.current = 0;

      pollInFlight.current = true;
      pollOnce(jid)
        .then(() => {
          pollBackoff.current = 0;
          if (phaseRef.current === "PROCESSING") scheduleNextPoll(jid);
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

  /**
   * Upload only (NO job counting here)
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

        await JobService.uploadToR2(created.uploadUrl, file, (pct) => {
          setUploadPct(clampPct(pct));
        });

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
   * Start processing
   * ✅ Job is counted HERE (on Start success)
   */
  const startProcessing = useCallback(
    async (splitMb: number) => {
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
      setResult(null);
      setDownloadUrl(null);

      setBusy(true);

      // Sync refs immediately so startPolling won't stall
      phaseRef.current = "PROCESSING";
      stageRef.current = "Queued";

      setPhase("PROCESSING");
      setProgressPct(0);
      setStageLabel("Queued");

      try {
        await JobService.start(jid, splitMb);

        // ✅ COUNT 1 JOB only after Start succeeded
        recordJobEvent(nowMs());

        startPolling(jid);
      } catch (e: any) {
        setBusy(false);
        setPhase("ERROR");
        setError(e?.message || "Start failed");
      }
    },
    [assertJobQuotaOrThrow, jobId, startPolling]
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

  useEffect(() => {
    const onVis = () => {
      if (document.visibilityState === "visible") {
        if (jobId && phaseRef.current === "PROCESSING") {
          startPolling(jobId);
        }
      }
    };
    document.addEventListener("visibilitychange", onVis);
    return () => document.removeEventListener("visibilitychange", onVis);
  }, [jobId, startPolling]);

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

    // optional usage
    usage,

    // actions
    uploadOnly,
    startProcessing,
    confirmDone,
    newFile,
    resetAll,
  };
}

/* ------------------------------------------------------------------
  Plan helpers (pricing/auth screens later)
------------------------------------------------------------------- */

export function setActivePlan(tier: PlanTier) {
  const map: Record<PlanTier, Omit<Plan, "activatedAt">> = {
    P90: { tier: "P90", days: 90, quotaJobs: 25, priceMnt: 7000 },
    P180: { tier: "P180", days: 180, quotaJobs: 100, priceMnt: 29000 },
    P365: { tier: "P365", days: 365, quotaJobs: 250, priceMnt: 59000 },
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
