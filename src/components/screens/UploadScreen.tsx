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
  if (mb == null) return { ok: false, msg: "Enter a number (MB)" };
  if (mb <= 0) return { ok: false, msg: "Must be greater than 0" };
  if (mb > 500) return { ok: false, msg: "Max is 500MB" };
  return { ok: true, msg: "" };
}

export function UploadScreen() {
  const [step, setStep] = useState<Step>("PICK");
  const [file, setFile] = useState<File | null>(null);

  // Start tab only
  const [splitMbText, setSplitMbText] = useState<string>("");

  // Download UX
  const [dlUx, setDlUx] = useState<DownloadUX>("IDLE");
  const [fakePct, setFakePct] = useState(0);
  const fakeTimerRef = useRef<number | null>(null);

  const flow = useUploadFlow();

  const splitMb = useMemo(() => parseMbInt(splitMbText), [splitMbText]);
  const splitValid = useMemo(() => validateMb(splitMb), [splitMb]);

  // Auto-sync UI step from backend phase
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

  // cleanup on unmount
  useEffect(() => {
    return () => stopFakeTimer();
  }, []);

  const doStart = async () => {
    if (!file) return;
    if (!splitValid.ok || splitMb == null) return;

    setStep("RUN");
    try {
      await flow.startProcessing(splitMb);
    } catch {
      // flow.error handles
    }
  };

  /**
   * Reliable download trigger across devices:
   * user click → browser navigation
   */
  const startDownloadAndPrepare = () => {
    if (!flow.downloadUrl) return;

    const url = `${flow.downloadUrl}${
      flow.downloadUrl.includes("?") ? "&" : "?"
    }cb=${Date.now()}`;

    window.location.assign(url);

    // fake progress (UI only)
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
    ) : (
      <Card>
        <div className="text-sm text-zinc-500">No file selected.</div>
      </Card>
    );

  return (
    <ScreenShell title="Split PDF" subtitle="Upload → Start → Download ZIP">
      <div className="grid max-w-[760px] gap-3">
        <StepHeader step={step} />

        {/* STEP 1: PICK (Upload) */}
        {step === "PICK" && (
          <>
            <FileDropzone
              onPick={async (f) => {
                setFile(f);
                try {
                  // single source of truth: pick -> upload immediately
                  await flow.uploadOnly(f);
                  setStep("SETTINGS");
                } catch {
                  // flow.error handles
                }
              }}
            />

            {fileMeta ? <FileMetaCard /> : null}

            {flow.phase === "UPLOADING" ? (
              <Card>
                <div className="grid gap-2.5">
                  <div className="font-semibold text-zinc-900">Uploading…</div>
                  <Progress value={flow.uploadPct} />
                  <div className="text-xs text-zinc-500">{flow.uploadPct}%</div>
                </div>
              </Card>
            ) : null}

            {flow.error ? (
              <Card>
                <div className="grid gap-2">
                  <div className="font-semibold text-zinc-900">Error</div>
                  <div className="whitespace-pre-wrap text-xs text-zinc-500">
                    {flow.error}
                  </div>
                </div>
              </Card>
            ) : null}

            <div className="flex flex-wrap gap-2.5">
              <Button
                variant="secondary"
                disabled={!file || flow.busy}
                onClick={hardReset}
              >
                Clear
              </Button>
            </div>
          </>
        )}

        {/* STEP 2: SETTINGS (Start) */}
        {step === "SETTINGS" && (
          <>
            <FileMetaCard />

            <Card>
              <div className="grid gap-2">
                <div className="font-semibold text-zinc-900">
                  Max size per file
                </div>

                <div className="flex items-center gap-2.5 rounded-2xl border border-zinc-200 bg-white px-3 py-2 shadow-sm">
                  <input
                    type="number"
                    inputMode="numeric"
                    min={1}
                    max={500}
                    step={1}
                    value={splitMbText}
                    onChange={(e) => setSplitMbText(e.target.value)}
                    placeholder="e.g. 9"
                    className="w-full flex-1 bg-transparent text-base font-semibold text-zinc-900 outline-none"
                  />

                  <span className="rounded-full border border-zinc-200 bg-zinc-50 px-2.5 py-1 text-xs font-semibold text-zinc-700">
                    MB
                  </span>
                </div>

                {/* helper / validation */}
                {splitMbText.trim().length === 0 ? (
                  <div className="text-xs text-zinc-500">
                    <i>e.g. 9</i>
                  </div>
                ) : null}

                {!splitValid.ok && splitMbText.trim().length > 0 ? (
                  <div className="text-xs font-semibold text-red-600">
                    {splitValid.msg}
                  </div>
                ) : null}

                {splitValid.ok ? (
                  <div className="text-xs text-zinc-500">
                    Each part will be up to <b>{splitMb}</b>MB.
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
          </>
        )}

        {/* STEP 3: RUN / READY / ERROR */}
        {step === "RUN" && (
          <>
            {flow.phase === "PROCESSING" ? (
              <Card>
                <div className="grid gap-2.5">
                  <div className="font-semibold text-zinc-900">Processing…</div>

                  <div className="grid gap-1.5">
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
                  <div className="font-semibold text-zinc-900">Error</div>
                  <div className="whitespace-pre-wrap text-xs text-zinc-500">
                    {flow.error}
                  </div>

                  <div className="flex flex-wrap gap-2.5 pt-1">
                    <Button variant="secondary" onClick={() => setStep("PICK")}>
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
                <div className="grid gap-2.5">
                  <div className="font-semibold text-zinc-900">Ready ✅</div>

                  <div className="grid gap-1.5 text-sm">
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

                  <div className="grid gap-2.5 pt-1">
                    {dlUx === "IDLE" ? (
                      <Button
                        disabled={!canDownload || !flow.downloadUrl}
                        onClick={startDownloadAndPrepare}
                      >
                        Download
                      </Button>
                    ) : null}

                    {dlUx === "PREPARE" ? (
                      <>
                        <Button disabled>Download</Button>
                        <div className="grid gap-1.5">
                          <Progress value={fakePct} />
                          <div className="text-xs text-zinc-500">
                            Download started. Getting ready to delete your files
                            from the server.
                          </div>
                          <div className="text-xs text-zinc-500">
                            {fakePct}%
                          </div>
                        </div>
                      </>
                    ) : null}

                    {dlUx === "CONFIRM" ? (
                      <>
                        <Button onClick={confirmCleanupAndReset}>Confirm</Button>
                        <div className="text-xs text-zinc-500">
                          Download started. Getting ready to delete your files
                          from the server.
                        </div>
                      </>
                    ) : null}

                    {dlUx === "SUCCESS" ? (
                      <div className="text-sm font-semibold text-zinc-900">
                        Good Job 🤩
                      </div>
                    ) : null}
                  </div>
                </div>
              </Card>
            ) : null}
          </>
        )}
      </div>
    </ScreenShell>
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
          "grid h-[22px] w-[22px] place-items-center rounded-full text-xs font-semibold",
          active
            ? "bg-[var(--primary)] text-white"
            : "bg-zinc-200 text-zinc-700",
        ].join(" ")}
      >
        {n}
      </span>
      {label}
    </div>
  );

  return (
    <div className="flex flex-wrap gap-2.5">
      <Item n={1} label="Upload" active={step === "PICK"} />
      <Item n={2} label="Start" active={step === "SETTINGS"} />
      <Item n={3} label="Download" active={step === "RUN"} />
    </div>
  );
}
