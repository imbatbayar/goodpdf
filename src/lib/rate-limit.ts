import { NextResponse } from "next/server";

type RateLimitOptions = {
  key: string;
  limit: number;
  windowMs: number;
};

type Bucket = {
  count: number;
  resetAt: number;
};

const STORE_KEY = "__goodpdf_rate_limit_store__";

function getStore(): Map<string, Bucket> {
  const g = globalThis as any;
  if (!g[STORE_KEY]) g[STORE_KEY] = new Map<string, Bucket>();
  return g[STORE_KEY] as Map<string, Bucket>;
}

function getClientIp(req: Request): string {
  const xff = req.headers.get("x-forwarded-for") || "";
  const first = xff.split(",")[0]?.trim();
  if (first) return first;
  return req.headers.get("x-real-ip") || "unknown";
}

export function checkRateLimit(req: Request, opts: RateLimitOptions) {
  const now = Date.now();
  const store = getStore();
  const clientKey = `${opts.key}:${getClientIp(req)}`;
  const prev = store.get(clientKey);

  if (!prev || now >= prev.resetAt) {
    const next: Bucket = { count: 1, resetAt: now + opts.windowMs };
    store.set(clientKey, next);
    return { ok: true, retryAfterSec: 0 };
  }

  if (prev.count >= opts.limit) {
    const retryAfterSec = Math.max(1, Math.ceil((prev.resetAt - now) / 1000));
    return { ok: false, retryAfterSec };
  }

  prev.count += 1;
  store.set(clientKey, prev);
  return { ok: true, retryAfterSec: 0 };
}

export function rateLimitResponse(retryAfterSec: number) {
  return NextResponse.json(
    { ok: false, error: "Too many requests. Please try again shortly." },
    {
      status: 429,
      headers: {
        "cache-control": "no-store",
        "retry-after": String(retryAfterSec),
      },
    },
  );
}
