"use client";

import Link from "next/link";

function CoinIcon({ className = "" }: { className?: string }) {
  return (
    <svg viewBox="0 0 24 24" className={className} fill="none" aria-hidden="true">
      {/* top coin */}
      <ellipse cx="12" cy="7" rx="7.5" ry="3.5" className="fill-amber-200/90" />
      {/* body */}
      <path
        d="M4.5 7v10c0 1.93 3.36 3.5 7.5 3.5s7.5-1.57 7.5-3.5V7"
        className="stroke-amber-100/90"
        strokeWidth="1.6"
      />
      {/* middle rings */}
      <path
        d="M19.5 12.2c0 1.93-3.36 3.5-7.5 3.5s-7.5-1.57-7.5-3.5"
        className="stroke-amber-100/70"
        strokeWidth="1.6"
      />
      <path
        d="M19.5 16.6c0 1.93-3.36 3.5-7.5 3.5s-7.5-1.57-7.5-3.5"
        className="stroke-amber-100/50"
        strokeWidth="1.6"
      />
      {/* subtle inner highlight */}
      <ellipse cx="12" cy="7" rx="5.2" ry="2.1" className="fill-amber-300/25" />
    </svg>
  );
}

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
        {subtitle ? <div className="mt-1 text-xs text-zinc-500">{subtitle}</div> : null}
      </div>
      {children}
    </div>
  );
}

function StatPill({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-2xl border border-zinc-200 bg-white p-5 shadow-sm">
      <div className="text-xs font-medium text-zinc-500">{label}</div>
      <div className="mt-1 text-2xl font-semibold tracking-tight text-zinc-900">{value}</div>
    </div>
  );
}

function ProgressBar({ used, total }: { used: number; total: number }) {
  const pct =
    total <= 0 ? 0 : Math.max(0, Math.min(100, Math.round((used / total) * 100)));

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between text-xs text-zinc-600">
        <span>
          Used <span className="font-semibold text-zinc-900">{used}</span> of {total}
        </span>
        <span>{pct}%</span>
      </div>

      <div className="h-2 w-full rounded-full bg-zinc-100">
        <div className="h-2 rounded-full bg-green-600 transition-all" style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
}

export function UsagePage() {
  // UI-only placeholder — will be real in PHASE D
  const freeTotal = 3;
  const freeUsed = 0; // placeholder
  const freeRemaining = Math.max(0, freeTotal - freeUsed);

  const freeLimit = "Up to 100 MB";
  const proLimit = "Up to 500 MB";

  return (
    <div className="mx-auto w-full max-w-5xl px-4 py-6">
      {/* Header */}
      <div className="mb-6 space-y-1">
        <h1 className="text-3xl font-semibold tracking-tight text-zinc-900">Usage</h1>
        <p className="text-sm text-zinc-600">Your current limits and usage.</p>
      </div>

      {/* Quick stats */}
      <div className="grid gap-4 md:grid-cols-3">
        <StatPill label="Free uses left" value={`${freeRemaining}`} />
        <StatPill label="Free file size" value={freeLimit} />
        <StatPill label="Pro file size" value={proLimit} />
      </div>

      {/* Main cards */}
      <div className="mt-6 grid gap-4 md:grid-cols-2">
        <Card title="Free usage" subtitle="Demo values for now.">
          <ProgressBar used={freeUsed} total={freeTotal} />

          <div className="mt-4 rounded-xl border border-zinc-200 bg-zinc-50 p-4 text-sm text-zinc-700">
            <div className="font-semibold text-zinc-900">What counts as 1 use?</div>
            <div className="mt-1 text-zinc-600">Each split + download counts as 1 use.</div>
          </div>
        </Card>

        <Card title="Credits" subtitle="Coming soon.">
          <div className="rounded-xl border border-zinc-200 bg-white p-4">
            <div className="text-sm font-semibold text-zinc-900">How it works</div>
            <div className="mt-2 text-sm text-zinc-600">
              Paid plans use credits. Credits are added after payment.
            </div>
          </div>

          <div className="mt-4">
            <Link
              href="/pricing"
              className="
                inline-flex w-full items-center justify-center gap-2
                rounded-xl px-4 py-3 text-sm font-extrabold
                bg-linear-to-r from-green-600 via-emerald-500 to-green-700
                hover:from-green-700 hover:via-emerald-600 hover:to-green-800
                transition
              "
            >
              <span
                className="
                  inline-flex h-6 w-6 items-center justify-center
                  rounded-md bg-white/20 ring-1 ring-white/30
                "
                aria-hidden="true"
              >
                <CoinIcon className="h-4 w-4" />
              </span>

              <span className="text-amber-200 drop-shadow-[0_1px_0_rgba(0,0,0,0.25)]">
                Upgrade to Pro
              </span>

              <span
                className="
                  ml-1 inline-flex items-center rounded-full
                  bg-white/20 px-2 py-0.5 text-[11px]
                  font-bold text-amber-100 ring-1 ring-white/25
                "
              >
                PRO
              </span>
            </Link>
          </div>
        </Card>
      </div>

      {/* Bottom note */}
      <div className="mt-8 rounded-2xl border border-zinc-200 bg-white p-5 text-sm text-zinc-600 shadow-sm">
        <div className="font-semibold text-zinc-900">Privacy</div>
        <div className="mt-1">Files are deleted automatically after download.</div>
      </div>
    </div>
  );
}
