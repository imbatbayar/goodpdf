"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { ScreenShell } from "@/components/screens/_ScreenShell";
import { FileDropzone } from "@/components/blocks/FileDropzone";
import { Button } from "@/components/ui/Button";
import { Progress } from "@/components/ui/Progress";
import { useUploadFlow } from "@/services/hooks/useUploadFlow";
import { JobService } from "@/services/JobService";
import { Card } from "@/components/blocks/Card";

type DownloadUX = "IDLE" | "PREPARE" | "SUCCESS";
type Step = "PICK" | "SETTINGS" | "RUN";
type StepState = "pending" | "active" | "done";
type PipelineRow = { key: string; label: string; stages: string[]; state: StepState; pct: number };
type PrecheckInfo = {
  tokenCost: 1 | 2 | 3;
  mode: "NORMAL" | "HEAVY" | "EXTREME";
  etaMinLow: number;
  etaMinHigh: number;
  estimatedCpuMin?: number;
  fileSizeMb: number;
  pages: number | null;
  avgMbPerPage: number | null;
  confidence?: "HIGH" | "MEDIUM" | "LOW";
  confidenceNote?: string;
  recommendation?: string;
  reason: string[];
};

const LS_PRECHECK_CALIBRATION = "goodpdf_precheck_calibration_v1";

const SYSTEM_TICK_MESSAGES = [
  "Scanning pages…",
  "Reading object streams…",
  "Normalizing page tree…",
  "Preparing split map…",
  "Optimizing output compatibility…",
  "Packing files (store)…",
  "Verifying parts…",
  "Finalizing…",
];
const STAGE_MESSAGES: Record<string, string[]> = {
  PREP: [
    "Preparing workspace",
    "Loading input metadata",
    "Validating input structure",
    "Building execution plan",
    "Checking compatibility constraints",
    "Initializing processing context",
  ],
  COMPRESS: [
    "Selecting compression profile",
    "Optimizing image streams",
    "Balancing quality and size",
    "Applying hard-fit pass",
    "Normalizing content",
    "Rechecking output size",
  ],
  SPLIT: [
    "Planning part boundaries",
    "Packing pages near target",
    "Checking oversized pages",
    "Refining split ranges",
    "Verifying part sizes",
    "Final split validation",
  ],
  ZIP: [
    "Collecting result parts",
    "Building zip package",
    "Writing zip index",
    "Streaming output archive",
    "Final integrity check",
    "Preparing download",
  ],
};
const STAGE_TICK_MS: Record<string, number> = {
  PREP: 90,
  COMPRESS: 120,
  SPLIT: 110,
  ZIP: 150,
};
const STAGE_COUNTER_CAP: Record<string, number> = {
  PREP: 1400,
  COMPRESS: 2200,
  SPLIT: 1600,
  ZIP: 1200,
};

const PIPELINE_STEPS: Array<{ key: string; label: string; stages: string[] }> = [
  { key: "prep", label: "Prepare", stages: ["ANALYZE", "PREFLIGHT", "DOWNLOAD", "QUEUE"] },
  {
    key: "compress",
    label: "Compress",
    stages: [
      "DEFAULT",
      "MANUAL",
      "SCAN_REBUILD",
      "COMPRESS_TURBO_PRIMARY",
      "COMPRESS_FORCE5_MAX",
      "COMPRESS_FORCE5_LAST",
      "COMPRESS_DEFAULT_ULTRA",
      "COMPRESS_DEFAULT_LASTMILE",
      "COMPRESS_MANUAL",
      "PART_FIT_9MB_FAST",
      "PART_SURGERY",
      "COMPRESS_HARDFIT",
      "COMPRESS_HARDFIT_NUCLEAR",
      "PART_FIT_9MB",
    ],
  },
  { key: "split", label: "Split", stages: ["SPLIT", "OVERSIZE_SAFE_SPLIT"] },
  { key: "zip", label: "ZIP", stages: ["ZIP", "UPLOAD_OUT", "DONE"] },
];

function stageIndex(stageCode: string) {
  const code = String(stageCode || "").toUpperCase();
  const idx = PIPELINE_STEPS.findIndex((s) => s.stages.includes(code));
  return idx >= 0 ? idx : -1;
}

function calcPipelineRows(stageCode: string, globalPct: number, phase: string) {
  const idx = stageIndex(stageCode);
  const doneAll = phase === "READY";
  const idle = idx < 0 && !doneAll;
  return PIPELINE_STEPS.map((s, i) => {
    const state: StepState = doneAll
      ? "done"
      : idle
      ? "pending"
      : i < idx
      ? "done"
      : i === idx
      ? "active"
      : "pending";
    const pct =
      state === "done" ? 100 : state === "pending" ? 0 : Math.max(3, Math.min(99, globalPct));
    return { ...s, state, pct };
  });
}

function stateRank(s: StepState) {
  if (s === "pending") return 0;
  if (s === "active") return 1;
  return 2;
}

function activeStageKey(rows: PipelineRow[]) {
  const active = rows.find((r) => r.state === "active");
  return active?.key?.toUpperCase() || "PREP";
}
function kickoffPctForIndex(i: number) {
  // 3/4/5%-ийн зөөлөн эхлэл (stage transition UI feel)
  return 3 + (i % 3);
}
function stageWorkingLabel(key: string) {
  const k = String(key || "").toUpperCase();
  if (k === "PREP") return "Prepare workspace";
  if (k === "COMPRESS") return "Compress";
  if (k === "SPLIT") return "Split";
  if (k === "ZIP") return "Zipping";
  return "Prepare workspace";
}

function parseMbInt(s: string) {
  const t = String(s || "").trim();
  if (!t) return null;
  const n = Number(t);
  if (!Number.isFinite(n)) return null;
  if (!Number.isInteger(n)) return null;
  return n;
}

function validateMb(mb: number | null) {
  // parseMbInt() нь integer л зөвшөөрдөг тул эндхийн UX мессеж ч тэрийгээ дагана.
  if (mb == null) return { ok: false, msg: "Enter a whole number (MB)" };
  if (mb <= 0) return { ok: false, msg: "Must be greater than 0" };
  if (mb > 500) return { ok: false, msg: "Maximum is 500 MB" };
  return { ok: true, msg: "" };
}

function clampPct(v: number) {
  const n = Number(v || 0);
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, Math.min(100, Math.round(n)));
}

function stageDotClass(state: StepState) {
  if (state === "done") return "bg-emerald-600 text-white";
  if (state === "active") return "bg-(--primary) text-white";
  return "bg-zinc-200 text-zinc-700";
}

function confidenceBadge(conf?: "HIGH" | "MEDIUM" | "LOW") {
  if (conf === "HIGH") return { label: "High confidence", cls: "bg-emerald-100 text-emerald-800" };
  if (conf === "LOW") return { label: "Low confidence", cls: "bg-amber-100 text-amber-800" };
  return { label: "Medium confidence", cls: "bg-zinc-200 text-zinc-700" };
}

export function UploadScreen() {
  const [step, setStep] = useState<Step>("PICK");
  const [file, setFile] = useState<File | null>(null);
  const [mode, setMode] = useState<"SYSTEM" | "MANUAL">("SYSTEM");

  // SYSTEM mode constraints (portal/email compatibility)
  const SYSTEM_MAX_PART_MB = 9;
  const SYSTEM_MAX_PARTS = 5;
  // Sentinel value stored in split_mb to trigger SYSTEM pipeline in worker (no DB schema changes)
  const SYSTEM_SENTINEL_SPLIT_MB = 499.99;

  const [splitMbText, setSplitMbText] = useState<string>("9");

  const [dlUx, setDlUx] = useState<DownloadUX>("IDLE");
  const [downloadError, setDownloadError] = useState<string | null>(null);
  const [precheck, setPrecheck] = useState<PrecheckInfo | null>(null);
  const [precheckLoading, setPrecheckLoading] = useState(false);
  const [precheckError, setPrecheckError] = useState<string | null>(null);
  const [precheckPct, setPrecheckPct] = useState(0);
  const precheckTimerRef = useRef<number | null>(null);
  const processingStartedAtRef = useRef<number | null>(null);
  const precheckEstimateAtStartRef = useRef<number | null>(null);
  const calibrationAppliedForRunRef = useRef(false);

  // PROCESSING үеийн “хиймэл” progress + нэг мөр систем мессеж
  const [procFakePct, setProcFakePct] = useState(1);
  const [procTick, setProcTick] = useState(0);
  const procPctTimerRef = useRef<number | null>(null);
  const procTickTimerRef = useRef<number | null>(null);
  const [stableRunPct, setStableRunPct] = useState(0);
  const stageActivityRef = useRef<string>("PREP");
  const [stageCounter, setStageCounter] = useState(0);
  const [stageMsgIdx, setStageMsgIdx] = useState(0);
  const stageTickerRef = useRef<number | null>(null);
  const stageTickMsRef = useRef<number>(120);
  const [stickyRows, setStickyRows] = useState<PipelineRow[]>(
    PIPELINE_STEPS.map((s) => ({ ...s, state: "pending" as StepState, pct: 0 })),
  );
  const [displayRows, setDisplayRows] = useState<PipelineRow[]>(
    PIPELINE_STEPS.map((s) => ({ ...s, state: "pending" as StepState, pct: 0 })),
  );
  const rowHeartbeatAtRef = useRef<number[]>(
    PIPELINE_STEPS.map(() => Date.now()),
  );

  const flow = useUploadFlow();

  const splitMb = useMemo(() => parseMbInt(splitMbText), [splitMbText]);
  const splitValid = useMemo(() => validateMb(splitMb), [splitMb]);

  useEffect(() => {
    if (flow.phase === "IDLE" || flow.phase === "UPLOADING") {
      if (step !== "PICK") setStep("PICK");
      return;
    }
    if (flow.phase === "UPLOADED") {
      if (step !== "SETTINGS") setStep("SETTINGS");
      return;
    }
    if (
      flow.phase === "PROCESSING" ||
      flow.phase === "READY" ||
      flow.phase === "ERROR"
    ) {
      if (step !== "RUN") setStep("RUN");
      return;
    }
  }, [flow.phase, step]);

  const fileMeta = useMemo(() => {
    if (!file) return null;
    const mb = file.size / (1024 * 1024);
    return { name: file.name, mb: Math.round(mb * 100) / 100 };
  }, [file]);

  const canStart =
    !!file &&
    !flow.busy &&
    flow.phase === "UPLOADED" &&
    !!precheck &&
    !precheckLoading &&
    (mode === "SYSTEM" || splitValid.ok);

  const canDownload = !flow.busy && flow.phase === "READY";

  const stopProcFakeTimers = () => {
    if (procPctTimerRef.current) {
      window.clearInterval(procPctTimerRef.current);
      procPctTimerRef.current = null;
    }
    if (procTickTimerRef.current) {
      window.clearInterval(procTickTimerRef.current);
      procTickTimerRef.current = null;
    }
  };
  const stopPrecheckTimer = () => {
    if (precheckTimerRef.current) {
      window.clearInterval(precheckTimerRef.current);
      precheckTimerRef.current = null;
    }
  };
  const stopStageTicker = () => {
    if (stageTickerRef.current) {
      window.clearInterval(stageTickerRef.current);
      stageTickerRef.current = null;
    }
  };
  const startStageTicker = (tickMs: number) => {
    stopStageTicker();
    stageTickMsRef.current = tickMs;
    stageTickerRef.current = window.setInterval(() => {
      setStageCounter((n) => {
        const key = stageActivityRef.current || "PREP";
        const cap = STAGE_COUNTER_CAP[key] || 1800;
        return n >= cap ? 0 : n + 1;
      });
      setStageMsgIdx((i) => i + 1);
    }, tickMs);
  };

  const hardReset = () => {
    setFile(null);
    setSplitMbText("");
    setStep("PICK");

    setDlUx("IDLE");
    setDownloadError(null);
    setPrecheck(null);
    setPrecheckLoading(false);
    setPrecheckError(null);
    setPrecheckPct(0);
    processingStartedAtRef.current = null;
    precheckEstimateAtStartRef.current = null;
    calibrationAppliedForRunRef.current = false;

    // processing fake-ийг цэвэрлэнэ
    setProcFakePct(1);
    setProcTick(0);
    setStableRunPct(0);
    setStageCounter(0);
    setStageMsgIdx(0);
    stageActivityRef.current = "PREP";
    setStickyRows(
      PIPELINE_STEPS.map((s) => ({ ...s, state: "pending" as StepState, pct: 0 })),
    );
    setDisplayRows(
      PIPELINE_STEPS.map((s) => ({ ...s, state: "pending" as StepState, pct: 0 })),
    );
    rowHeartbeatAtRef.current = PIPELINE_STEPS.map(() => Date.now());
    stopProcFakeTimers();
    stopPrecheckTimer();
    stopStageTicker();

    flow.resetAll();
  };

  useEffect(() => {
    let cancelled = false;
    const run = async () => {
      if (flow.phase !== "UPLOADED" || !flow.jobId) return;
      setPrecheckPct(4);
      setPrecheckLoading(true);
      setPrecheckError(null);
      try {
        const info = (await JobService.precheck(flow.jobId)) as PrecheckInfo;
        if (cancelled) return;
        setPrecheck(info);
        setPrecheckPct(100);
      } catch (e: any) {
        if (cancelled) return;
        setPrecheck(null);
        setPrecheckError(e?.message || "Precheck failed");
        setPrecheckPct(0);
      } finally {
        if (!cancelled) setPrecheckLoading(false);
      }
    };
    run();
    return () => {
      cancelled = true;
    };
  }, [flow.phase, flow.jobId]);

  useEffect(() => {
    if (flow.phase !== "UPLOADED" || !precheckLoading) {
      stopPrecheckTimer();
      return;
    }
    if (!precheckTimerRef.current) {
      precheckTimerRef.current = window.setInterval(() => {
        setPrecheckPct((p) => (p >= 95 ? 95 : p + 1));
      }, 220);
    }
    return () => {
      stopPrecheckTimer();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [flow.phase, precheckLoading]);

  // ✅ PROCESSING үед “хиймэл” progress + систем мессежийг ажиллуулна
  useEffect(() => {
    const real = typeof flow.progressPct === "number" ? flow.progressPct : 0;
    const shouldRun = flow.phase === "PROCESSING" && !flow.error;

    // PROCESSING биш бол / real progress орж ирвэл fake-г зогсооно
    if (!shouldRun) {
      stopProcFakeTimers();
      // real progress байвал fake-ийг 0 болгохгүй (UI огцом үсрэхээс хамгаална)
      return;
    }

    // эхлэхэд 1% дээр барина
    setProcFakePct((p) => (p <= 0 ? 1 : p));

    if (!procPctTimerRef.current) {
      procPctTimerRef.current = window.setInterval(() => {
        setProcFakePct((p) => {
          // Slow trickle while processing to avoid "stuck then jump" perception.
          if (p >= 95) return 95;
          return Math.min(95, p + 1);
        });
      }, 2000);
    }

    if (!procTickTimerRef.current) {
      procTickTimerRef.current = window.setInterval(() => {
        setProcTick((t) => t + 1);
      }, 900);
    }

    return () => {
      // phase солигдох үед цэвэрлэнэ
      stopProcFakeTimers();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [flow.phase, flow.progressPct, flow.error]);

  const doStart = async () => {
    if (!file) return;

    const splitMbToSend =
      mode === "SYSTEM" ? SYSTEM_SENTINEL_SPLIT_MB : splitMb;

    if (mode === "MANUAL" && (!splitValid.ok || splitMbToSend == null)) return;

    setStep("RUN");
    processingStartedAtRef.current = Date.now();
    precheckEstimateAtStartRef.current =
      typeof precheck?.estimatedCpuMin === "number" ? precheck.estimatedCpuMin : null;
    calibrationAppliedForRunRef.current = false;

    // PROCESSING эхлэхэд fake-ийг шинэчлэх
    setProcFakePct(1);
    setProcTick(0);
    setStableRunPct(0);
    setStageCounter(0);
    setStageMsgIdx(0);
    stageActivityRef.current = "PREP";
    setStickyRows(
      PIPELINE_STEPS.map((s) => ({ ...s, state: "pending" as StepState, pct: 0 })),
    );
    setDisplayRows(
      PIPELINE_STEPS.map((s) => ({ ...s, state: "pending" as StepState, pct: 0 })),
    );
    rowHeartbeatAtRef.current = PIPELINE_STEPS.map(() => Date.now());

    try {
      await flow.startProcessing({
        mode: mode === "SYSTEM" ? ("SYSTEM" as any) : ("MANUAL" as any),
        splitMb: splitMbToSend as number,
        precheckMode: precheck?.mode,
      });

    } catch {}
  };

  useEffect(() => {
    if (flow.phase !== "READY") return;
    if (calibrationAppliedForRunRef.current) return;
    const startedAt = processingStartedAtRef.current;
    const estimated = precheckEstimateAtStartRef.current;
    if (!startedAt || !estimated || estimated <= 0) return;

    const actualMin = Math.max(0.2, (Date.now() - startedAt) / 60_000);
    const ratio = Math.max(0.65, Math.min(2.2, actualMin / estimated));
    try {
      const prev = Number(localStorage.getItem(LS_PRECHECK_CALIBRATION) || "1");
      const safePrev = Number.isFinite(prev) ? Math.max(0.65, Math.min(2.2, prev)) : 1;
      const next = Math.max(0.65, Math.min(2.2, safePrev * 0.75 + ratio * 0.25));
      localStorage.setItem(LS_PRECHECK_CALIBRATION, String(next));
    } catch {}
    calibrationAppliedForRunRef.current = true;
  }, [flow.phase]);

  const startDownloadAndPrepare = async () => {
    if (!flow.jobId) return;

    setDlUx("PREPARE");
    setDownloadError(null);

    try {
      await JobService.downloadZip(flow.jobId);
      try {
        await flow.confirmDone();
      } catch {}
      setDlUx("SUCCESS");
      window.setTimeout(() => hardReset(), 1000);
    } catch (e: any) {
      setDlUx("IDLE");
      setDownloadError(e?.message || "Download failed");
    }
  };

  const FileMetaCard = () =>
    fileMeta ? (
      <Card>
        <div className="flex items-center justify-between gap-3">
          <div className="min-w-0 font-semibold text-zinc-900">
            <div className="truncate">{fileMeta.name}</div>
          </div>
          <div className="text-sm text-zinc-500">{fileMeta.mb}MB</div>
        </div>
      </Card>
    ) : null;

  // UI progress:
  // - Top "Processing %": real backend progress only
  // - 4 pipeline rows: real + soft heartbeat to avoid frozen feeling
  const realProgress = typeof flow.progressPct === "number" ? flow.progressPct : 0;
  const processingProgress = clampPct(realProgress);
  const pipelineProgress =
    flow.phase === "PROCESSING"
      ? Math.max(realProgress, procFakePct)
      : realProgress;

  const uiLine =
        (SYSTEM_TICK_MESSAGES[procTick % SYSTEM_TICK_MESSAGES.length] || "");
  const pipelineRowsRaw = useMemo(
    () => calcPipelineRows(flow.stageCode || "", clampPct(pipelineProgress), flow.phase),
    [flow.phase, flow.stageCode, pipelineProgress]
  );
  const showProcessingCard = step === "RUN" && flow.phase !== "READY" && !flow.error;

  useEffect(() => {
    if (step !== "RUN") {
      setStableRunPct(0);
      return;
    }
    if (flow.phase === "READY") {
      setStableRunPct(100);
      return;
    }
    if (flow.phase === "PROCESSING") {
      setStableRunPct((prev) => Math.max(prev, processingProgress));
    }
  }, [step, flow.phase, processingProgress]);

  useEffect(() => {
    if (step !== "RUN") return;
    setStickyRows((prev) => {
      const target = pipelineRowsRaw.map((r) => ({ ...r }));
      const noActive = target.every((r) => r.state === "pending" && r.pct === 0);
      if (flow.phase === "PROCESSING" && noActive && target.length > 0) {
        target[0].state = "active";
        target[0].pct = Math.max(1, Math.min(25, clampPct(pipelineProgress)));
      }
      return prev.map((oldRow, i) => {
        const nxt = target[i] || oldRow;
        const pct = flow.phase === "READY" ? 100 : Math.max(oldRow.pct, nxt.pct);
        const state =
          flow.phase === "READY"
            ? "done"
            : stateRank(nxt.state as StepState) > stateRank(oldRow.state)
              ? (nxt.state as StepState)
              : oldRow.state;
        return { ...oldRow, state, pct };
      });
    });
  }, [step, flow.phase, pipelineProgress, pipelineRowsRaw]);

  // Smooth row percentages to avoid abrupt jumps.
  useEffect(() => {
    if (step !== "RUN") return;
    const timer = window.setInterval(() => {
      setDisplayRows((prev) =>
        prev.map((row, i) => {
          const target = stickyRows[i] || row;
          const prevDone = i > 0 ? (prev[i - 1]?.pct || 0) >= 100 : false;
          const bridgeKick =
            flow.phase === "PROCESSING" &&
            target.state === "pending" &&
            row.pct <= 0 &&
            prevDone;
          const isStageActive =
            flow.phase === "PROCESSING" &&
            (target.state === "active" || row.state === "active" || bridgeKick);
          let nextPct = row.pct;
          if (flow.phase === "READY") {
            nextPct = 100;
          } else if (target.state === "done") {
            nextPct = Math.min(100, row.pct + 1);
          } else if (isStageActive) {
            const total = Math.max(1, stickyRows.length);
            const span = 100 / total;
            const overall = clampPct(pipelineProgress);
            const base = i * span;
            const derived = Math.max(
              0,
              Math.min(97, Math.round(((overall - base) / span) * 100)),
            );
            // Controlled active growth: +1% every 2s from current/stuck value.
            // This avoids premature 90%+ spikes while still preventing "frozen" feeling.
            const softCap = Math.min(90, Math.max(24, derived + 6));
            const now = Date.now();
            const lastAt = rowHeartbeatAtRef.current[i] || 0;
            if (row.pct <= 0) {
              // Immediately show life when a stage becomes active.
              nextPct = Math.min(softCap, kickoffPctForIndex(i));
              rowHeartbeatAtRef.current[i] = now;
            } else if (now - lastAt >= 2_000 && row.pct < softCap) {
              nextPct = Math.min(softCap, row.pct + 1);
              rowHeartbeatAtRef.current[i] = now;
            }
          }
          const nextState: StepState =
            flow.phase === "READY"
              ? "done"
              : target.state === "done"
                ? "done"
                : isStageActive
                  ? "active"
                  : target.state;
          return {
            ...row,
            state: nextState,
            pct: Math.max(0, Math.min(100, Math.round(nextPct))),
          };
        }),
      );
    }, 90);
    return () => window.clearInterval(timer);
  }, [step, stickyRows, flow.phase, pipelineProgress]);

  // Hard heartbeat: if backend is silent, active stage still advances +1% every 2s.
  useEffect(() => {
    if (step !== "RUN" || flow.phase !== "PROCESSING" || flow.error) return;
    const timer = window.setInterval(() => {
      setDisplayRows((prev) =>
        prev.map((row) => {
          if (row.state !== "active") return row;
          if (row.pct >= 95) return row;
          return { ...row, pct: Math.min(95, row.pct + 1) };
        }),
      );
    }, 2_000);
    return () => window.clearInterval(timer);
  }, [step, flow.phase, flow.error]);

  useEffect(() => {
    if (step !== "RUN" || flow.phase !== "PROCESSING" || flow.error) {
      stopStageTicker();
      return;
    }
    if (!stageTickerRef.current) {
      const key = stageActivityRef.current || "PREP";
      startStageTicker(STAGE_TICK_MS[key] || 120);
    }
    return () => {
      stopStageTicker();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [step, flow.phase, flow.error]);

  const activeStage = activeStageKey(stickyRows);
  useEffect(() => {
    if (flow.phase !== "PROCESSING") return;
    if (stageActivityRef.current !== activeStage) {
      stageActivityRef.current = activeStage;
      setStageCounter(0);
      setStageMsgIdx(0);
      if (stageTickerRef.current) {
        startStageTicker(STAGE_TICK_MS[activeStage] || 120);
      }
    }
  }, [activeStage, flow.phase]);
  const stageMsgs = STAGE_MESSAGES[activeStage] || STAGE_MESSAGES.PREP;
  const liveStageLine = stageMsgs[stageMsgIdx % stageMsgs.length] || uiLine;


  return (
    <ScreenShell
      title="Split PDF by Size"
      subtitle={
        mode === "SYSTEM"
          ? "System-fit: maximize compatibility (≤9MB each, ≤5 files). Visual quality may be reduced."
          : "Preserve quality while splitting your PDF into parts up to your target size."
      }
    >
      <div className="mx-auto w-full max-w-2xl px-4 pb-10">
        <div className="mt-1 flex justify-center">
          <StepHeader step={step} />
        </div>

        <div className="mt-5 grid gap-4">
          {step === "PICK" && (
            <>
              <div className="grid gap-3">
                <FileDropzone
                  onPick={async (f) => {
                    setFile(f);
                    try {
                      await flow.uploadOnly(f);
                      setStep("SETTINGS");
                    } catch {}
                  }}
                />

                <UploadStatusStrip
                  phase={flow.phase}
                  pct={flow.uploadPct}
                  error={flow.error}
                />
              </div>

              {!file ? (
                <Card className="rounded-xl border border-zinc-200 bg-white p-5 shadow-sm">
                  <div className="grid gap-3">
                    <div className="text-sm leading-relaxed text-zinc-700">
                      <b>goodPDF</b> preserves quality while splitting your PDF
                      into parts up to your target size, then packs everything
                      into a single ZIP for easy download.
                    </div>

                    <div className="text-sm leading-relaxed text-zinc-600">
                      Your files are processed securely and are{" "}
                      <b>automatically deleted within 10 minutes</b>. We do not
                      store, inspect, reuse, or analyze your documents — ever.
                    </div>
                  </div>
                </Card>
              ) : null}
            </>
          )}

          {step === "SETTINGS" && (
            <>
              {fileMeta ? <FileMetaCard /> : null}

              <Card>
                <div className="grid gap-3">
                  <div className="rounded-xl border border-zinc-200 bg-zinc-50 p-3">
                    {precheckLoading ? (
                      <div className="grid gap-2">
                        <div className="flex items-center justify-between text-sm">
                          <div className="font-semibold text-zinc-900">Preparing start check</div>
                          <div className="text-zinc-600">{clampPct(precheckPct)}%</div>
                        </div>
                        <Progress value={clampPct(precheckPct)} />
                        <div className="text-xs text-zinc-600">
                          Checking file complexity, token usage and ETA...
                        </div>
                      </div>
                    ) : precheck ? (
                      <div className="grid gap-2">
                        <div className="flex items-center justify-between gap-3 text-sm">
                          <div className="font-semibold text-zinc-900">Estimated processing</div>
                          <div className="rounded-full bg-zinc-900 px-2.5 py-1 text-xs font-semibold text-white">
                            {precheck.tokenCost} token{precheck.tokenCost > 1 ? "s" : ""}
                          </div>
                        </div>
                        <div className="text-xs text-zinc-700">
                          Mode: <b>{precheck.mode}</b> • Time: <b>~{precheck.etaMinLow}-{precheck.etaMinHigh} min</b>
                        </div>
                        <div>
                          {(() => {
                            const c = confidenceBadge(precheck.confidence);
                            return (
                              <span className={`inline-flex rounded-full px-2 py-0.5 text-[11px] font-semibold ${c.cls}`}>
                                {c.label}
                              </span>
                            );
                          })()}
                        </div>
                        {typeof precheck.estimatedCpuMin === "number" ? (
                          <div className="text-xs text-zinc-700">
                            Estimated CPU: <b>~{precheck.estimatedCpuMin} min</b>
                          </div>
                        ) : null}
                        {precheck.confidenceNote ? (
                          <div className="text-[11px] text-zinc-600">{precheck.confidenceNote}</div>
                        ) : null}
                        {precheck.recommendation ? (
                          <div className="text-xs text-zinc-700">{precheck.recommendation}</div>
                        ) : null}
                        <div className="text-xs text-zinc-600">
                          {precheck.reason.join(" • ")}
                        </div>
                        <div className="text-[11px] text-zinc-500">
                          {precheck.pages != null ? `Pages: ${precheck.pages}` : "Pages: —"} • Avg/page:{" "}
                          {precheck.avgMbPerPage != null ? `${precheck.avgMbPerPage}MB` : "—"}
                        </div>
                      </div>
                    ) : (
                      <div className="grid gap-2">
                        <div className="text-sm text-amber-700">
                          Could not estimate token/time yet.
                        </div>
                        {precheckError ? (
                          <div className="text-xs text-amber-700">{precheckError}</div>
                        ) : null}
                        <div className="text-xs text-zinc-600">
                          Please retry in a few seconds.
                        </div>
                      </div>
                    )}
                  </div>
                  <div className="flex items-center justify-between gap-3">
                    <div className="font-semibold text-zinc-900">
                      {mode === "SYSTEM" ? "System-fit" : "Target size per part"}
                    </div>
                    <div className="inline-flex rounded-full border border-zinc-200 bg-white p-1 shadow-sm">
                      <button
                        type="button"
                        onClick={() => setMode("SYSTEM")}
                        className={`px-3 py-1 text-xs font-semibold rounded-full transition focus-visible:outline-none ${
                          mode === "SYSTEM"
                            ? "bg-zinc-900 text-white"
                            : "text-zinc-700 hover:bg-zinc-50"
                        }`}
                        aria-pressed={mode === "SYSTEM"}
                      >
                        Default
                      </button>
                      <button
                        type="button"
                        onClick={() => setMode("MANUAL")}
                        className={`px-3 py-1 text-xs font-semibold rounded-full transition focus-visible:outline-none ${
                          mode === "MANUAL"
                            ? "bg-zinc-900 text-white"
                            : "text-zinc-700 hover:bg-zinc-50"
                        }`}
                        aria-pressed={mode === "MANUAL"}
                      >
                        Manual
                      </button>
                    </div>
                  </div>

                  {mode === "SYSTEM" ? (
                    <div className="rounded-xl border border-zinc-200 bg-white p-4 shadow-sm">
                      <div className="grid gap-1 text-sm text-zinc-700">
                        <div>
                          Max size: <b>≤{SYSTEM_MAX_PART_MB}MB</b>
                        </div>
                        <div>
                          Max split: <b>≤{SYSTEM_MAX_PARTS} files</b>
                        </div>
                      </div>
                      <div className="mt-2 text-xs text-zinc-500">
                        This mode prioritizes system compatibility over visual
                        quality. Files remain readable.
                      </div>
                    </div>
                  ) : (
                    <>
                      <div className="flex items-stretch overflow-hidden rounded-xl border border-zinc-200 bg-white shadow-sm">
                        <input
                          type="number"
                          inputMode="numeric"
                          min={1}
                          max={500}
                          step={1}
                          value={splitMbText}
                          onChange={(e) => setSplitMbText(e.target.value)}
                          placeholder="9"
                          className="no-focus w-full flex-1 bg-transparent px-4 py-2.5 text-base font-semibold text-zinc-900 outline-none focus-visible:outline-none"
                        />

                        <div className="w-px bg-zinc-200" />

                        <button
                          type="button"
                          onClick={() => {
                            const cur = parseInt(splitMbText || "", 10);
                            const base =
                              Number.isFinite(cur) && cur > 0 ? cur : 1;
                            const next = Math.max(1, base - 1);
                            setSplitMbText(String(next));
                          }}
                          className="w-12 select-none grid place-items-center text-xl font-semibold text-zinc-700 hover:bg-zinc-50 active:bg-zinc-100 focus-visible:outline-none"
                          aria-label="Decrease"
                        >
                          –
                        </button>

                        <div className="w-px bg-zinc-200" />

                        <button
                          type="button"
                          onClick={() => {
                            const cur = parseInt(splitMbText || "", 10);
                            const base =
                              Number.isFinite(cur) && cur > 0 ? cur : 1;
                            const next = Math.min(500, base + 1);
                            setSplitMbText(String(next));
                          }}
                          className="w-12 select-none grid place-items-center text-xl font-semibold text-zinc-700 hover:bg-zinc-50 active:bg-zinc-100 focus-visible:outline-none"
                          aria-label="Increase"
                        >
                          +
                        </button>

                        <div className="w-px bg-zinc-200" />

                        <div className="grid place-items-center bg-zinc-50 px-3 text-xs font-semibold text-zinc-600">
                          MB
                        </div>
                      </div>

                      {!splitValid.ok && splitMbText.trim().length > 0 ? (
                        <div className="text-xs font-semibold text-red-600">
                          {splitValid.msg}
                        </div>
                      ) : null}

                      {splitValid.ok ? (
                        <div className="text-xs text-zinc-500">
                          Parts will be generated up to <b>{splitMb}</b>MB.
                        </div>
                      ) : (
                        <div className="text-xs text-zinc-500">
                          Enter a whole number (MB).
                        </div>
                      )}
                    </>
                  )}
                </div>
              </Card>

              <div className="flex flex-wrap gap-2.5">
                <Button disabled={!canStart} onClick={doStart}>
                  {precheckLoading ? "Preparing..." : "Start"}
                </Button>

                <Button
                  variant="secondary"
                  disabled={flow.busy}
                  onClick={hardReset}
                >
                  Clear
                </Button>
              </div>

              <div className="text-left text-xs leading-5 text-zinc-500">
                {mode === "SYSTEM"
                  ? `Parts are generated to fit ≤${SYSTEM_MAX_PART_MB}MB each (≤${SYSTEM_MAX_PARTS} files)`
                  : "Parts are split up to your target size"}{" "}
                • Packed into a single ZIP • Auto-delete within 10 minutes
              </div>
            </>
          )}

          {step === "RUN" && (
            <>
              {showProcessingCard ? (
                <Card>
                  <div className="grid gap-3">
                    <div className="font-semibold text-zinc-900 flex items-center justify-between">
                      <span>Processing...</span>
                      <span>{stableRunPct}%</span>
                    </div>
                    <div className="text-xs text-zinc-500">
                      Working {stageWorkingLabel(activeStage)}{" "}
                      <span className="font-semibold">{String(stageCounter).padStart(3, "0")}</span>
                      {liveStageLine ? ` • ${liveStageLine}` : ""}
                    </div>

                    <div className="grid gap-2 pt-1">
                      {displayRows.map((r) => (
                        <div
                          key={r.key}
                          className="rounded-lg border border-zinc-200 bg-white p-2.5"
                        >
                          <div className="flex items-center justify-between gap-2">
                            <div className="flex items-center gap-2 text-xs font-semibold text-zinc-800">
                              <span
                                className={`grid h-5 w-5 place-items-center rounded-full text-[11px] ${stageDotClass(
                                  r.state
                                )}`}
                              >
                                {r.state === "done" ? "✓" : r.state === "active" ? "…" : "•"}
                              </span>
                              {r.label}
                            </div>
                            <div className="text-[11px] text-zinc-500">{r.pct}%</div>
                          </div>
                          <div className="mt-1.5">
                            <Progress value={r.pct} />
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                </Card>
              ) : null}

              {flow.error ? (
                <Card>
                  <div className="grid gap-2">
                    <div className="font-semibold text-zinc-900">
                      Something went wrong
                    </div>
                    <div className="whitespace-pre-wrap text-xs text-zinc-500">
                      {flow.error}
                    </div>

                    <div className="flex flex-wrap gap-2.5 pt-1">
                      <Button
                        variant="secondary"
                        onClick={() => setStep("PICK")}
                      >
                        Back
                      </Button>
                      <Button variant="ghost" onClick={hardReset}>
                        Clear
                      </Button>
                    </div>
                  </div>
                </Card>
              ) : null}

              {flow.phase === "READY" ? (
                <Card>
                  <div className="grid gap-3">
                    <div className="font-semibold text-zinc-900">Ready ✅</div>

                    <div className="rounded-lg border border-emerald-200 bg-emerald-50 p-2 text-xs text-emerald-800">
                      Processing 100% ✓
                    </div>

{flow.warning ? (
  <div className="rounded-lg border border-amber-200 bg-amber-50 p-3 text-xs text-amber-900">
    {flow.warning}
  </div>
) : null}


                    <div className="grid gap-2 text-sm">
                      <div>
                        <span className="text-zinc-500">Split into:</span>{" "}
                        {flow.result?.partsCount != null
                          ? `${flow.result.partsCount} parts`
                          : "—"}
                      </div>

                      <div>
                        <span className="text-zinc-500">Max part size:</span>{" "}
                        {flow.result?.maxPartMb != null
                          ? `${flow.result.maxPartMb}MB`
                          : "—"}
                      </div>

                      <div className="text-xs text-zinc-500">
                        Target size:{" "}
                        <b>
                          {mode === "SYSTEM"
                            ? `${SYSTEM_MAX_PART_MB}MB`
                            : `${splitMbText || "—"}MB`}
                        </b>
                      </div>
                    </div>

                    <div className="grid gap-3 pt-1">
                      {dlUx === "IDLE" ? (
                        <Button
                          disabled={!canDownload || !flow.jobId}
                          onClick={startDownloadAndPrepare}
                        >
                          Download
                        </Button>
                      ) : null}
                      {downloadError ? (
                        <div className="rounded-lg border border-red-200 bg-red-50 p-2 text-xs text-red-700">
                          {downloadError}
                        </div>
                      ) : null}

                      {dlUx === "PREPARE" ? (
                        <>
                          <Button disabled>Download</Button>
                          <div className="text-xs text-zinc-500">
                            Download
                          </div>
                        </>
                      ) : null}

                      {dlUx === "SUCCESS" ? (
                        <div className="text-sm font-semibold text-zinc-900">
                          Done ✅
                        </div>
                      ) : null}
                    </div>

                    <div className="text-left text-xs leading-5 text-zinc-500">
                      Privacy-first processing • Auto-delete within 10 minutes
                    </div>
                  </div>
                </Card>
              ) : null}
            </>
          )}
        </div>
      </div>
    </ScreenShell>
  );
}

function UploadStatusStrip({
  phase,
  pct,
  error,
}: {
  phase: string;
  pct: number;
  error?: string | null;
}) {
  const showUploading = phase === "UPLOADING";
  const showUploaded = phase === "UPLOADED";
  const showError = !!error;

  const pctClamped = clampPct(pct);

  const visible = showUploading || showUploaded || showError;
  if (!visible) return null;

  const leftLabel = showError
    ? "Upload failed"
    : showUploaded
    ? "Uploaded"
    : showUploading
    ? "Uploading…"
    : "";

  const rightLabel = showUploading ? `${pctClamped}%` : showUploaded ? "100%" : "";

  return (
    <div
      className={[
        "w-full rounded-full border px-3 py-2.5 flex items-center gap-3",
        "bg-white/95 backdrop-blur shadow-sm",
        showError ? "border-red-200" : "border-zinc-200",
      ].join(" ")}
      aria-live="polite"
    >
      <div
        className={[
          "min-w-23 text-xs font-semibold",
          showError ? "text-red-600" : "text-zinc-600",
        ].join(" ")}
      >
        {leftLabel}
      </div>

      <div className="flex-1">
        <div className="h-1.5 w-full overflow-hidden rounded-full bg-zinc-100">
          <div
            className="h-full rounded-full bg-(--primary) transition-[width,opacity] duration-200 ease-out"
            style={{
              width: showUploading ? `${pctClamped}%` : showUploaded ? "100%" : "0%",
              opacity: showUploading || showUploaded ? 1 : 0,
            }}
          />
        </div>
      </div>

      <div className="w-12 text-right text-xs font-semibold text-zinc-600 tabular-nums">
        {rightLabel}
      </div>
    </div>
  );
}

function StepHeader({ step }: { step: Step }) {
  const Item = ({
    n,
    label,
    active,
  }: {
    n: number;
    label: string;
    active: boolean;
  }) => (
    <div
      className={[
        "flex items-center gap-2.5 rounded-2xl border px-3 py-2 text-sm font-semibold",
        active
          ? "border-zinc-200 bg-[rgba(31,122,74,.10)] text-zinc-900"
          : "border-zinc-200 bg-white/70 text-zinc-500",
      ].join(" ")}
      aria-current={active ? "step" : undefined}
    >
      <span
        className={[
          "grid h-5.5 w-5.5 place-items-center rounded-full text-xs font-semibold",
          active
            ? "bg-(--primary) text-white"
            : "bg-zinc-200 text-zinc-700",
        ].join(" ")}
      >
        {n}
      </span>
      {label}
    </div>
  );

  return (
    <div className="flex flex-nowrap gap-2.5 overflow-x-auto">
      <Item n={1} label="Upload" active={step === "PICK"} />
      <Item n={2} label="Start" active={step === "SETTINGS"} />
      <Item n={3} label="End" active={step === "RUN"} />
    </div>
  );
}
