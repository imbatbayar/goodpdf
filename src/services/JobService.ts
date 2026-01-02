import type { QualityMode } from "@/domain/jobs/quality";

type StartArgs = {
  file: File;
  quality: QualityMode;
  splitMb: number;

  // MVP: auth холбогдоогүй тул түр userId дамжуулж байна
  userId: string;
};

type Callbacks = {
  onStep?: (s: string) => void;
  onProgress?: (p: number) => void;
  onJobId?: (id: string) => void;
};

export class JobService {
  async startJob(args: StartArgs, cb: Callbacks) {
    cb.onStep?.("Creating job…");
    cb.onProgress?.(2);

    // 1) Create job (✅ presigned URL хэрэггүй болсон)
    const createRes = await fetch("/api/jobs/create", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        userId: args.userId,
        quality: args.quality,
        splitMb: args.splitMb,
        fileName: args.file.name,
        fileSize: args.file.size,
      }),
    }).then((r) => r.json());

    if (!createRes?.ok) throw new Error(createRes?.error || "Create job failed");

    const { jobId } = createRes.data as { jobId: string };
    cb.onJobId?.(jobId);

    // 2) Upload (✅ UI -> API -> R2). CORS асуудалгүй.
    cb.onStep?.("Uploading…");
    cb.onProgress?.(10);

    const form = new FormData();
    // ⚠️ route.ts дээр formData.get("file") гэж авдаг тул нэр нь "file" байна
    form.append("file", args.file, args.file.name);

    const upRes = await fetch(`/api/jobs/upload-file?jobId=${encodeURIComponent(jobId)}`, {
      method: "POST",
      body: form,
    }).then((r) => r.json());

    if (!upRes?.ok) throw new Error(upRes?.error || "Upload failed");

    cb.onProgress?.(33);

    // 3) Poll status until DONE
    cb.onStep?.("Processing…");
    await this.pollDone(jobId, (p) => cb.onProgress?.(33 + p * 0.67));

    cb.onStep?.("Done.");
    cb.onProgress?.(100);

    return { jobId };
  }

  async confirmDone(jobId: string) {
    const res = await fetch("/api/jobs/done", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ jobId }),
    }).then((r) => r.json());

    if (!res?.ok) throw new Error(res?.error || "Confirm failed");
    return res;
  }

  /**
   * ✅ Download URL авах
   * Анхаар: redirect-ийг fetch-ээр "барих" гэж оролдохгүй (CORS дээр унадаг).
   * UI энэ URL-ийг user click дээр triggerDownload() ашиглаад нээнэ.
   */
  async getDownloadUrl(jobId: string): Promise<string> {
    return `/api/jobs/download?jobId=${encodeURIComponent(jobId)}`;
  }

  /**
   * ✅ Browser download-г user click дээрээс trigger хийх helper
   */
  triggerDownload(url: string) {
    window.location.href = url;
  }

  private async pollDone(jobId: string, onPct: (pct: number) => void) {
    for (let i = 0; i < 240; i++) {
      const res = await fetch(`/api/jobs/status?jobId=${encodeURIComponent(jobId)}`, {
        cache: "no-store",
        headers: { "cache-control": "no-store" },
      }).then((r) => r.json());

      if (!res?.ok) throw new Error(res?.error || "Status failed");

      const { status, progress } = res.data as { status: string; progress: number };

      onPct(progress ?? Math.min(99, (i / 240) * 100));

      if (status === "DONE" || status === "DONE_CONFIRMED") return;
      if (status === "FAILED") throw new Error("Processing failed");

      await new Promise((x) => setTimeout(x, 250));
    }
    throw new Error("Timed out");
  }
}
