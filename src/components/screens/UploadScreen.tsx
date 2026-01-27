"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { ScreenShell } from "@/components/screens/_ScreenShell";
import { FileDropzone } from "@/components/blocks/FileDropzone";
import { Button } from "@/components/ui/Button";
import { Progress } from "@/components/ui/Progress";
import { useUploadFlow } from "@/services/hooks/useUploadFlow";
import { Card } from "@/components/blocks/Card";

type DownloadUX = "IDLE" | "PREPARE" | "CONFIRM" | "SUCCESS";
type Step = "PICK" | "SETTINGS" | "RUN";

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

export function UploadScreen() {
  const [step, setStep] = useState<Step>("PICK");
  const [file, setFile] = useState<File | null>(null);

  const [splitMbText, setSplitMbText] = useState<string>("9");

  const [dlUx, setDlUx] = useState<DownloadUX>("IDLE");
  const [fakePct, setFakePct] = useState(0);
  const fakeTimerRef = useRef<number | null>(null);

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
    !!file && !flow.busy && flow.phase === "UPLOADED" && splitValid.ok;

  const canDownload = !flow.busy && flow.phase === "READY";

  const stopFakeTimer = () => {
    if (fakeTimerRef.current) {
      window.clearInterval(fakeTimerRef.current);
      fakeTimerRef.current = null;
    }
  };

  const hardReset = () => {
    setFile(null);
    setSplitMbText("");
    setStep("PICK");

    setDlUx("IDLE");
    setFakePct(0);
    stopFakeTimer();

    flow.resetAll();
  };

  useEffect(() => {
    return () => stopFakeTimer();
  }, []);

  const doStart = async () => {
    if (!file) return;
    if (!splitValid.ok || splitMb == null) return;

    setStep("RUN");
    try {
      await flow.startProcessing(splitMb);
    } catch {}
  };

  const startDownloadAndPrepare = () => {
    if (!flow.downloadUrl) return;

    const url = `${flow.downloadUrl}${
      flow.downloadUrl.includes("?") ? "&" : "?"
    }cb=${Date.now()}`;

    window.location.assign(url);

    setDlUx("PREPARE");
    setFakePct(0);
    stopFakeTimer();

    const startedAt = Date.now();
    fakeTimerRef.current = window.setInterval(() => {
      const elapsed = Date.now() - startedAt;
      const pct = Math.min(100, Math.round((elapsed / 5000) * 100));
      setFakePct(pct);
      if (pct >= 100) {
        stopFakeTimer();
        setDlUx("CONFIRM");
      }
    }, 50);
  };

  const confirmCleanupAndReset = async () => {
    setDlUx("SUCCESS");
    try {
      await flow.confirmDone();
    } catch {}
    window.setTimeout(() => hardReset(), 1000);
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

  return (
    <ScreenShell
      title="Split PDF by Size"
      subtitle="Preserve quality while splitting your PDF into parts up to your target size."
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
                  <div className="font-semibold text-zinc-900">
                    Target size per part
                  </div>

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
                        const base = Number.isFinite(cur) && cur > 0 ? cur : 1;
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
                        const base = Number.isFinite(cur) && cur > 0 ? cur : 1;
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
                </div>
              </Card>

              <div className="flex flex-wrap gap-2.5">
                <Button disabled={!canStart} onClick={doStart}>
                  Start
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
                Parts are split up to your target size • Packed into a single ZIP
                • Auto-delete within 10 minutes
              </div>
            </>
          )}

          {step === "RUN" && (
            <>
              {flow.phase === "PROCESSING" ? (
                <Card>
                  <div className="grid gap-3">
                    <div className="font-semibold text-zinc-900">
                      Processing…
                    </div>

                    <div className="grid gap-2">
                      <div className="text-xs text-zinc-500">
                        {flow.stageLabel || "Working"}
                      </div>
                      <Progress value={flow.progressPct} />
                      <div className="text-xs text-zinc-500">
                        {flow.progressPct}%
                      </div>
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
                        Target size: <b>{splitMbText || "—"}MB</b>
                      </div>
                    </div>

                    <div className="grid gap-3 pt-1">
                      {dlUx === "IDLE" ? (
                        <Button
                          disabled={!canDownload || !flow.downloadUrl}
                          onClick={startDownloadAndPrepare}
                        >
                          Download ZIP
                        </Button>
                      ) : null}

                      {dlUx === "PREPARE" ? (
                        <>
                          <Button disabled>Download ZIP</Button>
                          <div className="grid gap-2">
                            <Progress value={fakePct} />
                            <div className="text-xs text-zinc-500">
                              Secure cleanup in progress. Your files are already
                              scheduled for deletion and will be permanently
                              removed within 10 minutes.
                            </div>
                            <div className="text-xs text-zinc-500">
                              {fakePct}%
                            </div>
                          </div>
                        </>
                      ) : null}

                      {dlUx === "CONFIRM" ? (
                        <>
                          <Button onClick={confirmCleanupAndReset}>
                            Confirm
                          </Button>
                          <div className="text-xs text-zinc-500">
                            Secure cleanup in progress. Your files are already
                            scheduled for deletion and will be permanently
                            removed within 10 minutes.
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

  const rightLabel = showUploading
    ? `${pctClamped}%`
    : showUploaded
    ? "100%"
    : "";

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
              width: showUploading
                ? `${pctClamped}%`
                : showUploaded
                ? "100%"
                : "0%",
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
      <Item n={2} label="Split size" active={step === "SETTINGS"} />
      <Item n={3} label="Download" active={step === "RUN"} />
    </div>
  );
}
