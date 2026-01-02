"use client";

import { useCallback, useMemo, useState } from "react";
import type { QualityMode } from "@/domain/jobs/quality";
import { JobService } from "@/services/JobService";
import { DEFAULT_SPLIT_MB } from "@/config/constants";

const DEV_USER_ID = "00000000-0000-0000-0000-000000000001";

type Phase = "IDLE" | "UPLOADING" | "UPLOADED" | "PROCESSING" | "READY" | "ERROR";

type ResultSummary = {
  compressedMb?: number | null; // ORIGINAL дээр null байж болно
  partsCount?: number | null;
  maxPartMb?: number | null;
  targetMb?: number | null;
};

export function useUploadFlow() {
  const [phase, setPhase] = useState<Phase>("IDLE");
  const [busy, setBusy] = useState(false);

  const [jobId, setJobId] = useState<string | null>(null);

  // progress
  const [uploadPct, setUploadPct] = useState(0);
  const [compressPct, setCompressPct] = useState(0);
  const [splitPct, setSplitPct] = useState(0);

  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<ResultSummary | null>(null);

  const resetAll = useCallback(() => {
    setPhase("IDLE");
    setBusy(false);
    setJobId(null);
    setUploadPct(0);
    setCompressPct(0);
    setSplitPct(0);
    setError(null);
    setResult(null);
  }, []);

  /**
   * 1) Upload ONLY:
   * - create job -> presigned PUT
   * - upload to R2 (XHR progress)
   * - markUploaded -> status UPLOADED
   */
  const uploadOnly = useCallback(async (file: File, quality: QualityMode, splitMb: number) => {
    setBusy(true);
    setError(null);
    setResult(null);
    setUploadPct(0);
    setCompressPct(0);
    setSplitPct(0);
    setPhase("UPLOADING");

    try {
      const js = new JobService();

      const created = await js.createJob(
        {
          file,
          userId: DEV_USER_ID,
          quality,
          splitMb,
        },
        {
          onJobId: (id) => setJobId(id),
          onProgress: (p) => setUploadPct(Math.round(p)),
        }
      );

      // upload (XHR progress)
      await js.uploadToR2SignedUrl(created.upload.url, file, {
        onProgress: (p) => setUploadPct(Math.round(p)),
      });

      await js.markUploaded(created.jobId);

      setPhase("UPLOADED");
      setUploadPct(100);
      return created.jobId;
    } catch (e: any) {
      setError(e?.message || "Upload failed");
      setPhase("ERROR");
      throw e;
    } finally {
      setBusy(false);
    }
  }, []);

  /**
   * 2) Start PROCESSING:
   * - POST /api/jobs/start { jobId, quality, splitMb }
   * - poll /api/jobs/status until DONE
   *
   * NOTE: одоогоор stage-progress бүрэн гаргаагүй байгаа тул:
   * - compressPct: 0 (done үед 100)
   * - splitPct: status route-ийн progress-ийг ашиглаж байгаа (ерөнхий progress)
   * Дараагийн алхам дээр worker -> compress_progress / split_progress гэж “жинхэнэ” болгоно.
   */
  const startProcessing = useCallback(async (opts: { quality: QualityMode; splitMb: number }) => {
    if (!jobId) throw new Error("Missing jobId");

    setBusy(true);
    setError(null);
    setResult(null);
    setCompressPct(0);
    setSplitPct(0);
    setPhase("PROCESSING");

    try {
      // Move UPLOADED -> QUEUED (worker only picks QUEUED)
      const startRes = await fetch("/api/jobs/start", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          jobId,
          quality: opts.quality,
          splitMb: opts.splitMb,
        }),
      }).then((r) => r.json());

      if (!startRes?.ok) throw new Error(startRes?.error || "Start failed");

      // Poll until DONE
      // Use JobService.pollDone to keep behavior consistent (interval 1000ms)
      const js = new JobService();
      await js.pollDone(
        jobId,
        (p) => {
          // ерөнхий progress-ийг split дээр харуулъя (жинхэнэ stage-г дараа хийнэ)
          setSplitPct(Math.max(0, Math.min(100, Math.round(p))));
        },
        { intervalMs: 1000, maxSeconds: 10 * 60 }
      );

      // DONE болсон үед summary-г status endpoint-оос нэг удаа аваад үзье (байвал)
      try {
        const st = await fetch(`/api/jobs/status?jobId=${encodeURIComponent(jobId)}`, {
          cache: "no-store",
          headers: { "cache-control": "no-store" },
        }).then((r) => r.json());

        if (st?.ok) {
          const d = st.data || {};
          setResult({
            compressedMb: d.compressed_mb ?? d.compressedMb ?? null,
            partsCount: d.parts_count ?? d.partsCount ?? null,
            maxPartMb: d.max_part_mb ?? d.maxPartMb ?? null,
            targetMb: d.target_mb ?? d.targetMb ?? opts.splitMb ?? DEFAULT_SPLIT_MB,
          });
        }
      } catch {
        // best effort
        setResult({ targetMb: opts.splitMb });
      }

      setCompressPct(100);
      setSplitPct(100);
      setPhase("READY");
    } catch (e: any) {
      setError(e?.message || "Processing failed");
      setPhase("ERROR");
      throw e;
    } finally {
      setBusy(false);
    }
  }, [jobId]);

  const downloadUrl = useMemo(() => {
    if (!jobId) return null;
    return new JobService().getDownloadUrl(jobId);
  }, [jobId]);

  const triggerDownload = useCallback(() => {
    if (!downloadUrl) return;
    new JobService().triggerDownload(downloadUrl);
  }, [downloadUrl]);

  const confirmDone = useCallback(async () => {
    if (!jobId) return;
    setBusy(true);
    try {
      await new JobService().confirmDone(jobId);
      resetAll();
    } catch (e: any) {
      setError(e?.message || "Confirm failed");
      setPhase("ERROR");
    } finally {
      setBusy(false);
    }
  }, [jobId, resetAll]);

  const newFile = useCallback(async () => {
    const id = jobId;
    resetAll();
    if (!id) return;

    try {
      await fetch("/api/jobs/cancel", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ jobId: id }),
      }).then((r) => r.json());
    } catch {
      // best effort
    }
  }, [jobId, resetAll]);

  return {
    // state
    phase,
    busy,
    jobId,
    uploadPct,
    compressPct,
    splitPct,
    error,
    result,
    downloadUrl,

    // actions
    uploadOnly,
    startProcessing,
    triggerDownload,
    confirmDone,
    newFile,
    resetAll,
  };
}
