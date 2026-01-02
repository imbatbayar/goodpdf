"use client";

import { useMemo, useState } from "react";
import { ScreenShell } from "@/components/screens/_ScreenShell";
import { FileDropzone } from "@/components/blocks/FileDropzone";
import { QualitySelector } from "@/components/blocks/QualitySelector";
import { SplitSizeInput } from "@/components/blocks/SplitSizeInput";
import { Button } from "@/components/ui/Button";
import { Progress } from "@/components/ui/Progress";
import type { QualityMode } from "@/domain/jobs/quality";
import { DEFAULT_SPLIT_MB } from "@/config/constants";
import { useUploadFlow } from "@/services/hooks/useUploadFlow";
import { Card } from "@/components/blocks/Card";
import { DoneConfirmModal } from "@/components/modals/DoneConfirmModal";

type Step = "PICK" | "SETTINGS" | "RUN";

export function UploadScreen() {
  const [step, setStep] = useState<Step>("PICK");

  const [file, setFile] = useState<File | null>(null);

  // ✅ Default-ууд
  const [quality, setQuality] = useState<QualityMode>("GOOD");
  const [splitMb, setSplitMb] = useState<number>(DEFAULT_SPLIT_MB);
  const [doneOpen, setDoneOpen] = useState(false);

  const flow = useUploadFlow();

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
    setQuality("GOOD");
    setSplitMb(DEFAULT_SPLIT_MB);
    setStep("PICK");
    flow.resetAll();
  };

  const doUpload = async () => {
    if (!file) return;
    try {
      await flow.uploadOnly(file, quality, splitMb);
      // upload дуусмагц settings дээр үлдээнэ (user өөрчилж болно)
      setStep("SETTINGS");
    } catch {
      // error нь flow.error дээр
    }
  };

  const doStart = async () => {
    if (!file) return;
    setStep("RUN");
    try {
      await flow.startProcessing({ quality, splitMb });
    } catch {
      // error нь flow.error дээр
    }
  };

  return (
    <ScreenShell title="Upload" subtitle="Step-by-step flow (Pick → Upload → Settings → Run → Download).">
      <DoneConfirmModal
          open={doneOpen}
          onClose={() => setDoneOpen(false)}
          onDone={async () => {
            await flow.confirmDone();
            setDoneOpen(false);
            resetToPick();
          }}
        />

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
                  <div style={{ fontWeight: 800, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                    {fileMeta.name}
                  </div>
                  <div style={{ color: "var(--muted)" }}>{fileMeta.mb}MB</div>
                </div>
              </Card>
            ) : null}

            {/* Upload progress card (энэ нь дараа popup болно) */}
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
                  <div style={{ fontWeight: 800, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
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

            <QualitySelector value={quality} onChange={setQuality} />
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
              <div style={{ fontSize: 12, color: "var(--muted)" }}>
                Upload хийсний дараа Start идэвхжинэ.
              </div>
            ) : null}
          </>
        )}

        {/* STEP 3: RUN + READY */}
        {step === "RUN" && (
          <>
            {/* Processing progress card (энэ нь дараа popup болно) */}
            {flow.phase === "PROCESSING" ? (
              <Card>
                <div style={{ display: "grid", gap: 10 }}>
                  <div style={{ fontWeight: 900 }}>Processing…</div>

                  <div style={{ display: "grid", gap: 6 }}>
                    <div style={{ fontSize: 12, color: "var(--muted)" }}>Compressing</div>
                    <Progress value={flow.compressPct} />
                    <div style={{ fontSize: 12, color: "var(--muted)" }}>{flow.compressPct}%</div>
                  </div>

                  <div style={{ display: "grid", gap: 6 }}>
                    <div style={{ fontSize: 12, color: "var(--muted)" }}>Splitting</div>
                    <Progress value={flow.splitPct} />
                    <div style={{ fontSize: 12, color: "var(--muted)" }}>{flow.splitPct}%</div>
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

            {/* ✅ READY summary + Download */}
            {flow.phase === "READY" ? (
              <Card>
                <div style={{ display: "grid", gap: 10 }}>
                  <div style={{ fontWeight: 900 }}>Ready ✅</div>

                  <div style={{ display: "grid", gap: 6, fontSize: 13 }}>
                    {quality === "GOOD" ? (
                      <div>
                        <span style={{ color: "var(--muted)" }}>Compressed:</span>{" "}
                        {flow.result?.compressedMb != null ? `${flow.result.compressedMb}MB` : "—"}
                      </div>
                    ) : null}

                    <div>
                      <span style={{ color: "var(--muted)" }}>Split into:</span>{" "}
                      {flow.result?.partsCount != null ? `${flow.result.partsCount} parts` : "—"}
                    </div>

                    <div>
                      <span style={{ color: "var(--muted)" }}>Max part size:</span>{" "}
                      {flow.result?.maxPartMb != null ? `${flow.result.maxPartMb}MB` : "—"}
                    </div>

                    <div style={{ fontSize: 12, color: "var(--muted)" }}>
                      Each part will be up to {splitMb}MB.
                    </div>
                  </div>

                  <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                    <Button
                      disabled={!canDownload || !flow.downloadUrl}
                      onClick={() => {
                        // ✅ Download-ыг шинэ tab дээр эхлүүлнэ (popup энэ tab дээр үлдэнэ)
                        window.open(flow.downloadUrl!, "_blank", "noopener,noreferrer");
                        setDoneOpen(true);
                      }}
                    >
                      Download
                    </Button>

                    {/* Done popup-г дараагийн алхам дээр оруулна. Одоохондоо confirmDone хийж reset. */}
                    <Button variant="secondary" disabled={flow.busy} onClick={async () => {
                      await flow.confirmDone();
                      resetToPick();
                    }}>
                      Done
                    </Button>

                    <Button variant="ghost" disabled={flow.busy} onClick={resetToPick}>
                      New file
                    </Button>
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
      {item(2, "Compress & Split", step === "SETTINGS")}
      {item(3, "Download", step === "RUN")}
    </div>
  );
}
