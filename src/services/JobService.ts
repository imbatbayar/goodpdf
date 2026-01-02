import type { QualityMode } from "@/domain/jobs/quality";

type CreateArgs = {
  file: File;

  // MVP: auth холбогдоогүй тул түр userId дамжуулж байна
  userId: string;

  // Upload хийсний дараа user Good/Original, splitMb-аа өөрчилж болно.
  // Гэхдээ одоогийн create API чинь quality/splitMb-г хадгалдаг тул түр дамжуулна.
  // (Дараагийн алхам дээр upload дараа нь start хийх API гаргаад бүр гоё болгоно.)
  quality: QualityMode; // "GOOD" | "ORIGINAL"
  splitMb: number;
};

type Callbacks = {
  onStep?: (s: string) => void;

  // 0..100
  onProgress?: (p: number) => void;

  onJobId?: (id: string) => void;
};

type CreateJobResponse = {
  jobId: string;
  upload: { url: string };
};

export class JobService {
  /**
   * 1) Job үүсгэнэ + presigned PUT URL авна
   */
  async createJob(args: CreateArgs, cb?: Pick<Callbacks, "onStep" | "onProgress" | "onJobId">) {
    cb?.onStep?.("Creating job…");
    cb?.onProgress?.(0);

    const res = await fetch("/api/jobs/create", {
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

    if (!res?.ok) throw new Error(res?.error || "Create job failed");

    const data = res.data as CreateJobResponse;
    cb?.onJobId?.(data.jobId);

    return data;
  }

  /**
   * 2) UI -> R2 direct PUT (signed URL) + жинхэнэ upload progress (XHR)
   */
  async uploadToR2SignedUrl(uploadUrl: string, file: File, cb?: Pick<Callbacks, "onStep" | "onProgress">) {
    cb?.onStep?.("Uploading…");
    cb?.onProgress?.(0);

    await new Promise<void>((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.open("PUT", uploadUrl, true);

      // ⚠️ create/route.ts дээр ContentType-ийг presign дээр bind хийгээгүй (зөв).
      // Тиймээс энд content-type тавихгүй байж болно.
      // Хэрвээ тавимаар бол: xhr.setRequestHeader("content-type", "application/pdf");
      // Гэхдээ CORS + signed headers зөрөх эрсдэлтэй тул одоохондоо битгий.

      xhr.upload.onprogress = (evt) => {
        if (!evt.lengthComputable) return;
        const pct = Math.max(0, Math.min(100, (evt.loaded / evt.total) * 100));
        cb?.onProgress?.(pct);
      };

      xhr.onload = () => {
        if (xhr.status >= 200 && xhr.status < 300) {
          cb?.onProgress?.(100);
          resolve();
        } else {
          reject(new Error(`Upload failed (status ${xhr.status})`));
        }
      };

      xhr.onerror = () => reject(new Error("Upload failed (network)"));
      xhr.send(file);
    });
  }

  /**
   * 3) Upload дууссаныг серверт мэдэгдэж job -> UPLOADED болгоно
   */
  async markUploaded(jobId: string) {
    const res = await fetch(`/api/jobs/upload?jobId=${encodeURIComponent(jobId)}`, {
      method: "POST",
      headers: { "cache-control": "no-store" },
      cache: "no-store",
    }).then((r) => r.json());

    if (!res?.ok) throw new Error(res?.error || "Mark uploaded failed");
    return res;
  }

  /**
   * 4) DONE хүртэл polling
   * default interval = 1000ms (dev log spam багасгана)
   */
  async pollDone(
    jobId: string,
    onPct: (pct: number) => void,
    opts?: { maxSeconds?: number; intervalMs?: number }
  ) {
    const maxSeconds = opts?.maxSeconds ?? 10 * 60; // 10 минут
    const intervalMs = opts?.intervalMs ?? 1000;

    const maxTries = Math.ceil((maxSeconds * 1000) / intervalMs);

    for (let i = 0; i < maxTries; i++) {
      const res = await fetch(`/api/jobs/status?jobId=${encodeURIComponent(jobId)}`, {
        cache: "no-store",
        headers: { "cache-control": "no-store" },
      }).then((r) => r.json());

      if (!res?.ok) throw new Error(res?.error || "Status failed");

      const data = res.data as { status: string; progress?: number; stage?: string; stage_progress?: number };

      // Одоогийн системд progress л байгаа (ерөнхий).
      // Дараагийн алхам дээр stage + stage_progress-г worker бичдэг болгоно.
      const pct = typeof data.progress === "number" ? data.progress : Math.min(99, (i / maxTries) * 100);
      onPct(Math.max(0, Math.min(100, pct)));

      if (data.status === "DONE" || data.status === "DONE_CONFIRMED") return;
      if (data.status === "FAILED") throw new Error("Processing failed");

      await new Promise((x) => setTimeout(x, intervalMs));
    }

    throw new Error("Timed out");
  }

  /**
   * ✅ Download endpoint (энэ нь 307 redirect хийдэг)
   */
  getDownloadUrl(jobId: string) {
    return `/api/jobs/download?jobId=${encodeURIComponent(jobId)}`;
  }

  /**
   * ✅ User click дээр таталт эхлүүлэх
   */
  triggerDownload(url: string) {
    window.location.href = url;
  }

  /**
   * ✅ User Done дарсны дараа
   */
  async confirmDone(jobId: string) {
    const res = await fetch("/api/jobs/done", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ jobId }),
    }).then((r) => r.json());

    if (!res?.ok) throw new Error(res?.error || "Confirm failed");
    return res;
  }
}
