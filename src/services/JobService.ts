import type { QualityMode } from "@/domain/jobs/quality";

/**
 * JobService
 * - goodPDF нь одоогоор "split-only" (compress хийхгүй).
 * - API contract:
 *   POST /api/jobs/create   -> { ok, data:{ jobId, uploadUrl, ownerToken } }
 *   POST /api/jobs/upload   -> { ok, data:{ jobId, inputKey } }
 *   GET  /api/jobs/status?jobId=...
 *   POST /api/jobs/start    -> { ok, data:{ job:{...} } }
 *   GET  /api/jobs/download?jobId=...
 *   POST /api/jobs/done     -> { ok:true }
 *   POST /api/jobs/cancel   -> { ok:true } (best-effort)
 */

type JsonResp<T> = { ok: boolean; data?: T; error?: string };

type CreateJobResp = {
  jobId: string;
  uploadUrl: string; // presigned PUT URL
  ownerToken?: string; // ✅ security gate token (localStorage-д хадгална)
};

type StartJobResp = {
  job: {
    id: string;
    status: string;
    split_mb?: number | null;
    progress?: number | null;
    stage?: string | null;
  };
};

export type StatusResp = {
  status: string;
  progress: number;
  stage?: string | null;
  downloadUrl?: string | null;

  partsCount?: number | null;
  maxPartMb?: number | null;
  targetMb?: number | null;

  errorText?: string | null;
  errorCode?: string | null;
};

type CreateArgs = {
  file: File;

  // MVP: auth холбогдоогүй тул түр userId дамжуулж байна
  userId: string;

  // Legacy field (UI дээр харагдахгүй). Compress хийхгүй ч schema/хуучин кодтой нийцүүлэхэд үлдээв.
  quality?: QualityMode; // "GOOD" | "ORIGINAL" (ignored)

  // ✅ DB constraint-ийн төлөө create үед fallback утга өгч болно (Start дээр жинхэнэ утга хадгална)
  splitMbFallback?: number;
};

const LS_OWNER_TOKEN = "goodpdf_last_owner_token";

function getOwnerToken(): string {
  try {
    return localStorage.getItem(LS_OWNER_TOKEN) || "";
  } catch {
    return "";
  }
}

function ownerHeaders(extra?: Record<string, string>) {
  const tok = getOwnerToken();
  const h: Record<string, string> = { ...(extra || {}) };
  if (tok) h["x-owner-token"] = tok;
  return h;
}

function assertOk<T>(res: JsonResp<T>, fallbackMsg: string) {
  if (!res?.ok) throw new Error(res?.error || fallbackMsg);
  return res.data as T;
}

async function readJson<T>(r: Response): Promise<JsonResp<T>> {
  let j: any = null;
  try {
    j = await r.json();
  } catch {
    // ignore (non-json)
  }
  if (!r.ok) {
    return { ok: false, error: j?.error || j?.message || `HTTP ${r.status}` };
  }
  return (j ?? { ok: true }) as JsonResp<T>;
}

export class JobService {
  /**
   * 1) Create job (server DB + presigned uploadUrl)
   *    - split-only: splitMbFallback нь зөвхөн schema constraint-т зориулагдсан.
   *    - ⚠️ create дээр owner token header явуулахгүй (энэ endpoint token үүсгэнэ)
   */
  static async createJob(args: CreateArgs): Promise<CreateJobResp> {
    const res = await fetch("/api/jobs/create", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        userId: args.userId,
        fileName: args.file.name,
        fileSizeBytes: args.file.size,
        quality: args.quality || "ORIGINAL",
        splitMb: args.splitMbFallback,
      }),
    }).then((r) => readJson<any>(r));

    const data = assertOk(res, "Create failed") as any;

    const jobId = String(data?.jobId || "").trim();
    const uploadUrl = String(data?.uploadUrl || data?.upload?.url || "").trim();

    // ✅ ownerToken (expected after DB change)
    const ownerTokenRaw = data?.ownerToken ?? data?.owner_token ?? null;
    const ownerToken = ownerTokenRaw ? String(ownerTokenRaw).trim() : undefined;

    if (!jobId) throw new Error("Create failed: missing jobId");
    if (!uploadUrl) throw new Error("Create failed: missing uploadUrl");

    return { jobId, uploadUrl, ownerToken };
  }

  /**
   * 2) Upload file to R2 with presigned PUT
   *    - XHR ашиглавал progress авах боломжтой
   */
  static async uploadToR2(uploadUrl: string, file: File, onPct?: (pct: number) => void) {
    await new Promise<void>((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.open("PUT", uploadUrl, true);
      xhr.setRequestHeader("content-type", "application/pdf");

      xhr.upload.onprogress = (evt) => {
        if (!evt.lengthComputable) return;
        const pct = Math.round((evt.loaded / evt.total) * 100);
        onPct?.(Math.max(0, Math.min(100, pct)));
      };

      xhr.onerror = () => reject(new Error("Upload failed"));
      xhr.onload = () => {
        if (xhr.status >= 200 && xhr.status < 300) resolve();
        else reject(new Error(`Upload failed (HTTP ${xhr.status})`));
      };

      xhr.send(file);
    });
  }

  /**
   * 2.5) Mark uploaded (server knows inputKey & can move job stage)
   */
  static async markUploaded(jobId: string) {
    const res = await fetch("/api/jobs/upload", {
      method: "POST",
      headers: ownerHeaders({ "content-type": "application/json" }),
      body: JSON.stringify({ jobId }),
    }).then((r) => readJson<{ jobId: string; inputKey: string }>(r));

    assertOk(res, "Mark uploaded failed");
  }

  /**
   * 3) Start processing (✅ splitMb энд жинхэнээрээ ирнэ)
   */
  static async start(jobId: string, splitMb: number): Promise<StartJobResp> {
    const res = await fetch("/api/jobs/start", {
      method: "POST",
      headers: ownerHeaders({ "content-type": "application/json" }),
      body: JSON.stringify({ jobId, splitMb }),
    }).then((r) => readJson<StartJobResp>(r));

    return assertOk(res, "Start failed");
  }

  /**
   * 4) Poll status
   */
  static async status(jobId: string): Promise<StatusResp> {
    const res = await fetch(`/api/jobs/status?jobId=${encodeURIComponent(jobId)}`, {
      method: "GET",
      headers: ownerHeaders({ "cache-control": "no-store" }),
    }).then((r) => readJson<StatusResp>(r));

    return assertOk(res, "Status failed");
  }

  /**
   * 5) Confirm done (start cleanup timer)
   */
  static async done(jobId: string) {
    const res = await fetch("/api/jobs/done", {
      method: "POST",
      headers: ownerHeaders({ "content-type": "application/json" }),
      body: JSON.stringify({ jobId }),
    }).then((r) => readJson<{}>(r));

    assertOk(res, "Confirm failed");
  }

  /**
   * 6) Cancel job (best effort)
   */
  static async cancel(jobId: string) {
    const res = await fetch("/api/jobs/cancel", {
      method: "POST",
      headers: ownerHeaders({ "content-type": "application/json" }),
      body: JSON.stringify({ jobId }),
    }).then((r) => readJson<{}>(r));

    // cancel нь best effort — ok биш байсан ч throw хийхгүй
    if (!res?.ok) return;
  }

  /**
   * 7) Download URL (frontend convenience)
   * ⚠️ Browser navigation дээр header attach хийх боломжгүй.
   * Дараагийн алхамд download gating-ийг өөр аргаар шийднэ.
   */
  static downloadUrl(jobId: string) {
    return `/api/jobs/download?jobId=${encodeURIComponent(jobId)}`;
  }
}
