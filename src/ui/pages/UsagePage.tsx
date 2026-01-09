"use client";

import Link from "next/link";

function Card({
  title,
  subtitle,
  children,
}: {
  title: string;
  subtitle?: string;
  children: React.ReactNode;
}) {
  return (
    <div className="rounded-2xl border border-zinc-200 bg-white p-6 shadow-sm">
      <div className="mb-4">
        <div className="text-sm font-semibold text-zinc-900">{title}</div>
        {subtitle ? (
          <div className="mt-1 text-xs text-zinc-500">{subtitle}</div>
        ) : null}
      </div>
      {children}
    </div>
  );
}

function StatPill({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-2xl border border-zinc-200 bg-white p-5 shadow-sm">
      <div className="text-xs font-medium text-zinc-500">{label}</div>
      <div className="mt-1 text-2xl font-semibold tracking-tight text-zinc-900">
        {value}
      </div>
    </div>
  );
}

function ProgressBar({ used, total }: { used: number; total: number }) {
  const pct = total <= 0 ? 0 : Math.max(0, Math.min(100, Math.round((used / total) * 100)));
  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between text-xs text-zinc-600">
        <span>
          Used <span className="font-semibold text-zinc-900">{used}</span> / {total}
        </span>
        <span>{pct}%</span>
      </div>
      <div className="h-2 w-full rounded-full bg-zinc-100">
        <div className="h-2 rounded-full bg-zinc-900 transition-all" style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
}

export function UsagePage() {
  // UI-only placeholder — PHASE D дээр localStorage → Supabase usage_log болно
  const freeTotal = 3;
  const freeUsed = 0; // placeholder
  const freeRemaining = Math.max(0, freeTotal - freeUsed);

  const freeLimit = "100MB";
  const paidLimit = "500MB";

  return (
    <div className="mx-auto w-full max-w-5xl px-4 py-10">
      {/* Header */}
      <div className="mb-8 space-y-2">
        <h1 className="text-3xl font-semibold tracking-tight text-zinc-900">Usage</h1>
        <p className="text-sm text-zinc-600">
          Одоогоор UI-only. Free uses-г PHASE D дээр бодитоор хасдаг болгоно.
        </p>
      </div>

      {/* Quick stats */}
      <div className="grid gap-4 md:grid-cols-3">
        <StatPill label="Free remaining" value={`${freeRemaining} uses`} />
        <StatPill label="Free limit" value={`${freeLimit} max`} />
        <StatPill label="Paid limit" value={`${paidLimit} max`} />
      </div>

      {/* Main cards */}
      <div className="mt-6 grid gap-4 md:grid-cols-2">
        <Card
          title="Free quota"
          subtitle="This is your current free allowance (demo)."
        >
          <ProgressBar used={freeUsed} total={freeTotal} />

          <div className="mt-4 rounded-xl border border-zinc-200 bg-zinc-50 p-4 text-sm text-zinc-700">
            <div className="font-semibold text-zinc-900">What counts as 1 use?</div>
            <div className="mt-1 text-zinc-600">
              Нэг удаа split хийж download хийснийг 1 use гэж үзнэ. (Phase D дээр бодит болно.)
            </div>
          </div>
        </Card>

        <Card
          title="Credits (coming soon)"
          subtitle="Paid plans use credits. Payment confirmed → credits grant."
        >
          <div className="rounded-xl border border-zinc-200 bg-white p-4">
            <div className="text-sm font-semibold text-zinc-900">Paid flow</div>
            <ol className="mt-2 list-decimal space-y-1 pl-5 text-sm text-zinc-600">
              <li>Plan сонгоно</li>
              <li>Payment confirmed (webhook)</li>
              <li>Credits нэмэгдэнэ</li>
              <li>Job эхлэхийг quota зөвшөөрнө</li>
            </ol>
          </div>

          <div className="mt-4 flex flex-col gap-2">
            <Link
              href="/pricing"
              className="inline-flex items-center justify-center rounded-xl bg-zinc-900 px-4 py-3 text-sm font-semibold text-white hover:bg-zinc-800"
            >
              Upgrade / Pricing
            </Link>
            <div className="text-xs text-zinc-500">
              Note: webhook + credits grant нь PHASE E дээр орно.
            </div>
          </div>
        </Card>
      </div>

      {/* Bottom note */}
      <div className="mt-8 rounded-2xl border border-zinc-200 bg-white p-5 text-sm text-zinc-600 shadow-sm">
        <div className="font-semibold text-zinc-900">Privacy</div>
        <div className="mt-1">
          Files are deleted automatically after Confirm + TTL. (Privacy-first retention.)
        </div>
      </div>
    </div>
  );
}
