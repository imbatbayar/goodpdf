"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { ScreenShell } from "@/components/screens/_ScreenShell";
import { FileDropzone } from "@/components/blocks/FileDropzone";
import { SplitSizeInput } from "@/components/blocks/SplitSizeInput";
import { Button } from "@/components/ui/Button";
import { Progress } from "@/components/ui/Progress";
import { DEFAULT_SPLIT_MB } from "@/config/constants";
import { useUploadFlow } from "@/services/hooks/useUploadFlow";
import { Card } from "@/components/blocks/Card";

type DownloadUX = "IDLE" | "PREPARE" | "CONFIRM" | "SUCCESS";
type Step = "PICK" | "SETTINGS" | "RUN";

export function UploadScreen() {
  const [step, setStep] = useState<Step>("PICK");
  const [file, setFile] = useState<File | null>(null);

  // ✅ Default-ууд (Split-only)
  const [splitMb, setSplitMb] = useState<number>(DEFAULT_SPLIT_MB);

  // ✅ Download UX (no popups)
  const [dlUx, setDlUx] = useState<DownloadUX>("IDLE");
  const [fakePct, setFakePct] = useState(0);
  const fakeTimerRef = useRef<number | null>(null);
  const iframeRef = useRef<HTMLIFrameElement | null>(null);

  const flow = useUploadFlow();

  // ✅ Phase-оос хамаараад UI step автоматаар зөв болох
  useEffect(() => {
    if (flow.phase === "IDLE") {
      if (step !== "PICK") setStep("PICK");
      return;
    }

    if (flow.phase === "UPLOADING") {
      if (step !== "PICK") setStep("PICK");
      return;
    }

    if (flow.phase === "UPLOADED") {
      if (step !== "SETTINGS") setStep("SETTINGS");
      return;
    }

    // PROCESSING / READY / ERROR -> RUN дээр харуулах
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

  const canUpload = !!file && !flow.busy && (flow.phase === "IDLE" || flow.phase === "ERROR");
  const canStart = !!file && !flow.busy && flow.phase === "UPLOADED";
  const canDownload = !flow.busy && flow.phase === "READY";

  const resetToPick = () => {
    setFile(null);
    setSplitMb(DEFAULT_SPLIT_MB);
    setStep("PICK");

    // reset download UX
    setDlUx("IDLE");
    setFakePct(0);
    if (fakeTimerRef.current) {
      window.clearInterval(fakeTimerRef.current);
      fakeTimerRef.current = null;
    }
    if (iframeRef.current) {
      iframeRef.current.remove();
      iframeRef.current = null;
    }

    flow.resetAll();
  };

  // ✅ Fake progress cleanup on unmount
  useEffect(() => {
    return () => {
      if (fakeTimerRef.current) {
        window.clearInterval(fakeTimerRef.current);
        fakeTimerRef.current = null;
      }
      if (iframeRef.current) {
        iframeRef.current.remove();
        iframeRef.current = null;
      }
    };
  }, []);

  const startDownloadAndPrepare = () => {
    if (!flow.downloadUrl) return;

    // 1) Trigger browser download WITHOUT navigation or popups
    // Hidden iframe follows redirects and initiates download while staying on the page.
    try {
      if (iframeRef.current) {
        iframeRef.current.remove();
        iframeRef.current = null;
      }
      const ifr = document.createElement("iframe");
      ifr.style.display = "none";
      ifr.src = flow.downloadUrl;
      document.body.appendChild(ifr);
      iframeRef.current = ifr;

      // Remove iframe later (avoid DOM leak)
      window.setTimeout(() => {
        try {
          ifr.remove();
        } catch {}
        if (iframeRef.current === ifr) iframeRef.current = null;
      }, 30000);
    } catch {
      // Fallback: same-tab navigation (last resort)
      window.location.href = flow.downloadUrl;
    }

    // 2) 5s fake progress 0→100
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
      await flow.confirmDone(); // best effort (server confirm)
    } catch {
      // best effort
    }
    window.setTimeout(() => {
      resetToPick();
    }, 1000);
  };

  const doUpload = async () => {
    if (!file) return;
    try {
      await flow.uploadOnly(file, splitMb);
      setStep("SETTINGS");
    } catch {
      // error нь flow.error дээр
    }
  };

  const doStart = async () => {
    if (!file) return;
    setStep("RUN");
    try {
      await flow.startProcessing();
    } catch {
      // error нь flow.error дээр
    }
  };

  return (
    <ScreenShell title="Split PDF" subtitle="Upload → Size → Start → Download ZIP">
      <div style={{ display: "grid", gap: 12, maxWidth: 760 }}>
        <StepHeader step={step} />

        {/* STEP 1: PICK */}
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

            {/* Upload progress card */}
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
                  <div style={{ fontSize: 12, color: "var(--muted)" }}>{flow.error}</div>
                </div>
              </Card>
            ) : null}

            <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
              <Button disabled={!canUpload} onClick={doUpload}>
                Upload
              </Button>
              <Button variant="secondary" disabled={!file || flow.busy} onClick={() => setStep("SETTINGS")}>
                Next
              </Button>
            </div>
          </>
        )}

        {/* STEP 2: SETTINGS */}
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

            <SplitSizeInput valueMb={splitMb} onChangeMb={setSplitMb} />

            <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
              <Button variant="secondary" disabled={flow.busy} onClick={() => setStep("PICK")}>
                Back
              </Button>

              {/* ✅ Upload хийгдээгүй бол Start идэвхгүй */}
              <Button disabled={!canStart} onClick={doStart}>
                Start
              </Button>

              <Button variant="ghost" disabled={flow.busy} onClick={resetToPick}>
                New file
              </Button>
            </div>

            {flow.phase !== "UPLOADED" ? (
              <div style={{ fontSize: 12, color: "var(--muted)" }}>Upload хийсний дараа Start идэвхжинэ.</div>
            ) : null}
          </>
        )}

        {/* STEP 3: RUN + READY */}
        {step === "RUN" && (
          <>
            {/* Processing progress card */}
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
                  <div style={{ fontSize: 12, color: "var(--muted)" }}>{flow.error}</div>
                  <div style={{ display: "flex", gap: 10 }}>
                    <Button variant="secondary" onClick={() => setStep("SETTINGS")}>
                      Back to settings
                    </Button>
                    <Button variant="ghost" onClick={resetToPick}>
                      New file
                    </Button>
                  </div>
                </div>
              </Card>
            ) : null}

            {/* ✅ READY summary + Download (no popups) */}
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

                    <div style={{ fontSize: 12, color: "var(--muted)" }}>Each part will be up to {splitMb}MB.</div>
                  </div>

                  <div style={{ display: "grid", gap: 10 }}>
                    {/* 1 button at a time: Download -> Confirm */}
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
      {item(2, "Size", step === "SETTINGS")}
      {item(3, "Download", step === "RUN")}
    </div>
  );
}
