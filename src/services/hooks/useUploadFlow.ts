"use client";

import { useState } from "react";
import type { QualityMode } from "@/domain/jobs/quality";
import { JobService } from "@/services/JobService";

const DEV_USER_ID = "00000000-0000-0000-0000-000000000001";

type StartArgs = { file: File; quality: QualityMode; splitMb: number };

export function useUploadFlow() {
  const [busy, setBusy] = useState(false);
  const [progress, setProgress] = useState(0);
  const [stepLabel, setStepLabel] = useState("");
  const [done, setDone] = useState(false);
  const [jobId, setJobId] = useState<string | null>(null);

  // ✅ Нэг дор reset хийх helper
  const resetToUpload = () => {
    setBusy(false);
    setProgress(0);
    setStepLabel("");
    setDone(false);
    setJobId(null);
  };

  const start = async (args: StartArgs) => {
    setBusy(true);
    setDone(false);
    setProgress(0);
    setStepLabel("Preparing…");
    setJobId(null);

    try {
      const js = new JobService();

      await js.startJob(
        {
          ...args,
          userId: DEV_USER_ID, // MVP
        } as any,
        {
          onStep: (s) => setStepLabel(s),
          onProgress: (p) => setProgress(p),
          onJobId: (id) => setJobId(id),
        }
      );

      // ✅ Автоматаар download хийхгүй (browser block хийдэг)
      setDone(true);
    } catch (e: any) {
      alert(e?.message || "Failed");
      // алдаа гарвал upload руу буцаая
      resetToUpload();
      return;
    } finally {
      setBusy(false);
      setStepLabel("");
    }
  };

  /**
   * ✅ DONE дармагц:
   * 1) /api/jobs/done -> cleanup
   * 2) UI-г цэвэр upload руу reset
   */
  const confirmDone = async () => {
    if (!jobId) return;
    try {
      setBusy(true);
      await new JobService().confirmDone(jobId);
      // ✅ хамгийн чухал: UI reset
      resetToUpload();
    } catch (e: any) {
      alert(e?.message || "Confirm failed");
    } finally {
      setBusy(false);
    }
  };

  /**
   * ✅ Download (redirect) — fetch хийхгүй
   */
  const downloadFallback = async () => {
    if (!jobId) return;
    try {
      const url = await new JobService().getDownloadUrl(jobId);
      window.location.href = url;
    } catch (e: any) {
      alert(e?.message || "Download failed");
    }
  };

  /**
   * ✅ New file / Back:
   * - одоогийн job байвал cancel (best-effort cleanup)
   * - upload руу reset
   */
  const newFile = async () => {
    const id = jobId;
    resetToUpload();
    if (!id) return;

    try {
      await fetch("/api/jobs/cancel", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ jobId: id }),
      }).then((r) => r.json());
    } catch {
      // best-effort; ямар ч байсан UI upload руу буцсан байх ёстой
    }
  };

  return {
    busy,
    progress,
    stepLabel,
    done,
    jobId,
    start,
    confirmDone,
    downloadFallback,
    newFile, // ✅ UI дээр "New file" товчийг үүгээр дууд
  };
}
