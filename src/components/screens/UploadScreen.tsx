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
  // require whole number MB
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

  // ✅ Size input is ONLY for Start tab
  const [splitMbText, setSplitMbText] = useState<string>("");

  // ✅ Download UX
  const [dlUx, setDlUx] = useState<DownloadUX>("IDLE");
  const [fakePct, setFakePct] = useState(0);
  const fakeTimerRef = useRef<number | null>(null);

  const flow = useUploadFlow();

  const splitMb = useMemo(() => parseMbInt(splitMbText), [splitMbText]);
  const splitValid = useMemo(() => validateMb(splitMb), [splitMb]);

  // ✅ Step auto-sync from backend phase
  useEffect(() => {
    if (flow.phase === "IDLE" || flow.phase === "UPLOADING") {
      if (step !== "PICK") setStep("PICK");
      return;
    }
    if (flow.phase === "UPLOADED") {
      if (step !== "SETTINGS") setStep("SETTINGS");
      return;
    }
    if (flow.phase === "PROCESSING" || flow.phase === "READY" || flow.phase === "ERROR") {
      if (step !== "RUN") setStep("RUN");
      return;
    }
  }, [flow.phase, step]);

  const fileMeta = useMemo(() => {
    if (!file) return null;
    const mb = file.size / (1024 * 1024);
    return { name: file.name, mb: Math.round(mb * 100) / 100 };
  }, [file]);

  // ✅ Upload tab: only depends on file + not busy
  const canUpload = !!file && !flow.busy && (flow.phase === "IDLE" || flow.phase === "ERROR");

  // ✅ Start tab: needs valid splitMb
  const canStart = !!file && !flow.busy && flow.phase === "UPLOADED" && splitValid.ok;

  const canDownload = !flow.busy && flow.phase === "READY";

  const hardReset = () => {
    setFile(null);
    setSplitMbText("");
    setStep("PICK");

    // reset download UX
    setDlUx("IDLE");
    setFakePct(0);
    if (fakeTimerRef.current) {
      window.clearInterval(fakeTimerRef.current);
      fakeTimerRef.current = null;
    }

    flow.resetAll();
  };

  // cleanup on unmount
  useEffect(() => {
    return () => {
      if (fakeTimerRef.current) {
        window.clearInterval(fakeTimerRef.current);
        fakeTimerRef.current = null;
      }
    };
  }, []);

  const doUpload = async () => {
    if (!file) return;
    try {
      // ✅ Upload stage does NOT include splitMb anymore
      await flow.uploadOnly(file);
      setStep("SETTINGS");
    } catch {
      // flow.error handles
    }
  };

  const doStart = async () => {
    if (!file) return;
    if (!splitValid.ok || splitMb == null) return;

    setStep("RUN");
    try {
      // ✅ Start stage MUST include splitMb
      await flow.startProcessing(splitMb);
    } catch {
      // flow.error handles
    }
  };

  /**
   * ✅ The ONLY reliable way across all devices (mobile + desktop):
   * user click → browser navigation (NO iframe, NO popup, NO background fetch)
   */
  const startDownloadAndPrepare = () => {
    if (!flow.downloadUrl) return;

    // ✅ cache-buster to avoid stale/cached responses
    const url = `${flow.downloadUrl}${flow.downloadUrl.includes("?") ? "&" : "?"}cb=${Date.now()}`;

    // ✅ This is the download trigger (works on iOS/Android/desktop)
    window.location.assign(url);

    // 2) 5s fake progress (UI only)
    setDlUx("PREPARE");
    setFakePct(0);

    if (fakeTimerRef.current) {
      window.clearInterval(fakeTimerRef.current);
      fakeTimerRef.current = null;
    }

    const startedAt = Date.now();
    fakeTimerRef.current = window.setInterval(() => {
      const elapsed = Date.now() - startedAt;
      const pct = Math.min(100, Math.round((elapsed / 5000) * 100));
      setFakePct(pct);
      if (pct >= 100) {
        if (fakeTimerRef.current) {
          window.clearInterval(fakeTimerRef.current);
          fakeTimerRef.current = null;
        }
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

  return (
    <ScreenShell title="Split PDF" subtitle="Upload → Start → Download ZIP">
      <div style={{ display: "grid", gap: 12, maxWidth: 760 }}>
        <StepHeader step={step} />

        {/* STEP 1: UPLOAD (PICK) — only dropzone + Upload/Clear */}
        {step === "PICK" && (
          <>
            <FileDropzone
              onPick={(f) => {
                setFile(f);
              }}
            />

            {fileMeta ? (
              <Card>
                <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
                  <div
                    style={{
                      fontWeight: 800,
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                      whiteSpace: "nowrap",
                    }}
                  >
                    {fileMeta.name}
                  </div>
                  <div style={{ color: "var(--muted)" }}>{fileMeta.mb}MB</div>
                </div>
              </Card>
            ) : null}

            {flow.phase === "UPLOADING" ? (
              <Card>
                <div style={{ display: "grid", gap: 10 }}>
                  <div style={{ fontWeight: 900 }}>Uploading…</div>
                  <Progress value={flow.uploadPct} />
                  <div style={{ fontSize: 12, color: "var(--muted)" }}>{flow.uploadPct}%</div>
                </div>
              </Card>
            ) : null}

            {flow.error ? (
              <Card>
                <div style={{ display: "grid", gap: 8 }}>
                  <div style={{ fontWeight: 900 }}>Error</div>
                  <div style={{ fontSize: 12, color: "var(--muted)", whiteSpace: "pre-wrap" }}>{flow.error}</div>
                </div>
              </Card>
            ) : null}

            <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
              <Button disabled={!canUpload} onClick={doUpload}>
                Upload
              </Button>
              <Button variant="secondary" disabled={!file || flow.busy} onClick={hardReset}>
                Clear
              </Button>
            </div>
          </>
        )}

        {/* STEP 2: START (SETTINGS) — file card + Max size + Start/Clear */}
        {step === "SETTINGS" && (
          <>
            {fileMeta ? (
              <Card>
                <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
                  <div
                    style={{
                      fontWeight: 800,
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                      whiteSpace: "nowrap",
                    }}
                  >
                    {fileMeta.name}
                  </div>
                  <div style={{ color: "var(--muted)" }}>{fileMeta.mb}MB</div>
                </div>
              </Card>
            ) : (
              <Card>
                <div style={{ color: "var(--muted)" }}>No file selected.</div>
              </Card>
            )}

            {/* ✅ Max size per file is HERE (Start tab) */}
            <Card>
              <div style={{ display: "grid", gap: 8 }}>
                <div style={{ fontWeight: 900 }}>Max size per file</div>

                <div
                  style={{
                    display: "flex",
                    alignItems: "center",
                    gap: 10,
                    border: "1px solid var(--border)",
                    borderRadius: 14,
                    padding: "10px 12px",
                    background: "rgba(255,255,255,.75)",
                  }}
                >
                  <input
                    type="number"
                    inputMode="numeric"
                    min={1}
                    max={500}
                    step={1}
                    value={splitMbText}
                    onChange={(e) => setSplitMbText(e.target.value)}
                    placeholder="e.g. 9"
                    style={{
                      flex: 1,
                      border: "none",
                      outline: "none",
                      fontSize: 16,
                      fontWeight: 800,
                      background: "transparent",
                    }}
                  />

                  <span
                    style={{
                      fontSize: 12,
                      fontWeight: 900,
                      padding: "6px 10px",
                      borderRadius: 999,
                      border: "1px solid var(--border)",
                      background: "rgba(15,23,42,.06)",
                    }}
                  >
                    MB
                  </span>
                </div>

                <div style={{ fontSize: 12, color: "var(--muted)" }}>Enter a number (MB)</div>

                {splitMbText.trim().length === 0 ? (
                  <div style={{ fontSize: 12, color: "var(--muted)" }}>
                    <i>e.g. 9</i>
                  </div>
                ) : null}

                {!splitValid.ok && splitMbText.trim().length > 0 ? (
                  <div style={{ fontSize: 12, color: "crimson", fontWeight: 800 }}>{splitValid.msg}</div>
                ) : null}

                {splitValid.ok ? (
                  <div style={{ fontSize: 12, color: "var(--muted)" }}>
                    Each part will be up to <b>{splitMb}</b>MB.
                  </div>
                ) : null}
              </div>
            </Card>

            <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
              <Button disabled={!canStart} onClick={doStart}>
                Start
              </Button>

              <Button variant="secondary" disabled={flow.busy} onClick={hardReset}>
                Clear
              </Button>
            </div>
          </>
        )}

        {/* STEP 3: RUN + READY */}
        {step === "RUN" && (
          <>
            {flow.phase === "PROCESSING" ? (
              <Card>
                <div style={{ display: "grid", gap: 10 }}>
                  <div style={{ fontWeight: 900 }}>Processing…</div>

                  <div style={{ display: "grid", gap: 6 }}>
                    <div style={{ fontSize: 12, color: "var(--muted)" }}>{flow.stageLabel || "Working"}</div>
                    <Progress value={flow.progressPct} />
                    <div style={{ fontSize: 12, color: "var(--muted)" }}>{flow.progressPct}%</div>
                  </div>
                </div>
              </Card>
            ) : null}

            {flow.error ? (
              <Card>
                <div style={{ display: "grid", gap: 8 }}>
                  <div style={{ fontWeight: 900 }}>Error</div>
                  <div style={{ fontSize: 12, color: "var(--muted)", whiteSpace: "pre-wrap" }}>{flow.error}</div>

                  <div style={{ display: "flex", gap: 10 }}>
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
                <div style={{ display: "grid", gap: 10 }}>
                  <div style={{ fontWeight: 900 }}>Ready ✅</div>

                  <div style={{ display: "grid", gap: 6, fontSize: 13 }}>
                    <div>
                      <span style={{ color: "var(--muted)" }}>Split into:</span>{" "}
                      {flow.result?.partsCount != null ? `${flow.result.partsCount} parts` : "—"}
                    </div>

                    <div>
                      <span style={{ color: "var(--muted)" }}>Max part size:</span>{" "}
                      {flow.result?.maxPartMb != null ? `${flow.result.maxPartMb}MB` : "—"}
                    </div>

                    <div style={{ fontSize: 12, color: "var(--muted)" }}>
                      Target size: <b>{splitMbText || "—"}MB</b>
                    </div>
                  </div>

                  <div style={{ display: "grid", gap: 10 }}>
                    {dlUx === "IDLE" ? (
                      <Button disabled={!canDownload || !flow.downloadUrl} onClick={startDownloadAndPrepare}>
                        Download
                      </Button>
                    ) : null}

                    {dlUx === "PREPARE" ? (
                      <>
                        <Button disabled>Download</Button>
                        <div style={{ display: "grid", gap: 6 }}>
                          <Progress value={fakePct} />
                          <div style={{ fontSize: 12, color: "var(--muted)" }}>
                            Download started. Getting ready to delete your files from the server.
                          </div>
                          <div style={{ fontSize: 12, color: "var(--muted)" }}>{fakePct}%</div>
                        </div>
                      </>
                    ) : null}

                    {dlUx === "CONFIRM" ? (
                      <>
                        <Button onClick={confirmCleanupAndReset}>Confirm</Button>
                        <div style={{ fontSize: 12, color: "var(--muted)" }}>
                          Download started. Getting ready to delete your files from the server.
                        </div>
                      </>
                    ) : null}

                    {dlUx === "SUCCESS" ? <div style={{ fontWeight: 900, fontSize: 14 }}>Good Job 🤩</div> : null}
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

function StepHeader({ step }: { step: "PICK" | "SETTINGS" | "RUN" }) {
  const item = (n: number, label: string, active: boolean) => (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: 8,
        padding: "10px 12px",
        borderRadius: 14,
        border: "1px solid var(--border)",
        background: active ? "rgba(31,122,74,.10)" : "rgba(255,255,255,.65)",
        fontWeight: 900,
        fontSize: 13,
        opacity: active ? 1 : 0.65,
      }}
    >
      <span
        style={{
          width: 22,
          height: 22,
          borderRadius: 999,
          display: "grid",
          placeItems: "center",
          fontSize: 12,
          background: active ? "var(--primary)" : "rgba(15,23,42,.10)",
          color: active ? "#fff" : "rgba(15,23,42,.8)",
        }}
      >
        {n}
      </span>
      {label}
    </div>
  );

  return (
    <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
      {item(1, "Upload", step === "PICK")}
      {item(2, "Start", step === "SETTINGS")}
      {item(3, "Download", step === "RUN")}
    </div>
  );
}
