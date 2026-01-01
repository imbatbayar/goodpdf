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

type Step = "PICK" | "SETTINGS" | "RUN";

export function UploadScreen() {
  const [step, setStep] = useState<Step>("PICK");

  const [file, setFile] = useState<File | null>(null);
  const [quality, setQuality] = useState<QualityMode>("GOOD");
  const [splitMb, setSplitMb] = useState<number>(DEFAULT_SPLIT_MB);

  const flow = useUploadFlow();

  const fileMeta = useMemo(() => {
    if (!file) return null;
    const mb = file.size / (1024 * 1024);
    return { name: file.name, mb: Math.round(mb * 100) / 100 };
  }, [file]);

  const canNext = !!file && !flow.busy;
  const canStart = !!file && !flow.busy;

  const start = async () => {
    if (!file) return;
    setStep("RUN");
    await flow.start({ file, quality, splitMb });
  };

  // Done болчихоод Back дарвал дахин PICK руу буцна
  const resetToPick = () => {
    setFile(null);
    setQuality("GOOD");
    setSplitMb(DEFAULT_SPLIT_MB);
    setStep("PICK");
  };

  return (
    <ScreenShell title="Upload" subtitle="Step-by-step flow (Pick → Settings → Run).">
      <div style={{ display: "grid", gap: 12, maxWidth: 760 }}>
        <StepHeader step={step} />

        {/* STEP 1: PICK */}
        {step === "PICK" && (
          <>
            <FileDropzone
              onPick={(f) => {
                setFile(f);
                setStep("SETTINGS"); // файл сонгомогц дараагийн алхам руу автоматаар
              }}
            />
            <div style={{ display: "flex", gap: 10 }}>
              <Button disabled={!canNext} onClick={() => setStep("SETTINGS")}>
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
              <Button disabled={!canStart} onClick={start}>
                Start
              </Button>
            </div>
          </>
        )}

        {/* STEP 3: RUN */}
        {step === "RUN" && (
          <>
            <Card>
              <div style={{ display: "grid", gap: 10 }}>
                <div style={{ fontWeight: 900 }}>{flow.stepLabel || "Working…"}</div>
                <Progress value={flow.progress} />
                <div style={{ fontSize: 12, color: "var(--muted)" }}>
                  {flow.progress < 34 ? "1/3 Upload" : flow.progress < 85 ? "2/3 Compress" : "3/3 Split & ZIP"}
                </div>
              </div>
            </Card>

            {flow.done ? (
              <Card>
                <div style={{ display: "grid", gap: 10 }}>
                  <div style={{ fontWeight: 900 }}>Done 🎉</div>
                  <div style={{ fontSize: 12, color: "var(--muted)" }}>
                    Download товч дарж ZIP-ээ татна.
                  </div>

                  <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                    <Button
                      onClick={async () => {
                        await flow.confirmDone();
                        resetToPick(); // ✅ step="PICK" болгож upload дэлгэц рүү буцаана
                      }}
                    >
                      Done
                    </Button>

                    <Button variant="secondary" onClick={() => flow.downloadFallback()}>
                      Download
                    </Button>
                    <Button variant="ghost" onClick={resetToPick}>
                      New file
                    </Button>
                  </div>
                </div>
              </Card>
            ) : null}

            {!flow.busy && !flow.done ? (
              <div style={{ display: "flex", gap: 10 }}>
                <Button variant="secondary" onClick={() => setStep("SETTINGS")}>
                  Back to settings
                </Button>
              </div>
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
