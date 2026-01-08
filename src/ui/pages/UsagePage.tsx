"use client";

import Link from "next/link";

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-2xl border bg-[var(--card)] p-5">
      <div className="text-xs opacity-70 font-bold">{label}</div>
      <div className="text-2xl font-extrabold mt-1">{value}</div>
    </div>
  );
}

export function UsagePage() {
  // UI-only placeholder — PHASE D will make this real
  const freeRemaining = 3;

  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <h1 className="text-2xl font-extrabold">Usage</h1>
        <p className="text-sm opacity-80">
          Одоогоор UI-only. Free uses-г PHASE D дээр localStorage → Supabase usage_log руу бодитоор холбоно.
        </p>
      </div>

      <div className="grid gap-4 md:grid-cols-3">
        <Stat label="Free remaining" value={`${freeRemaining} uses`} />
        <Stat label="Free limit" value="100MB max" />
        <Stat label="Paid limit" value="500MB max (soon)" />
      </div>

      <div className="rounded-2xl border bg-[var(--card)] p-5 space-y-3">
        <div className="font-extrabold">Need more?</div>
        <p className="text-sm opacity-85 leading-6">
          Paid plan бол credits ашиглана. Payment confirmed → credits нэмэгдэнэ → job эхлэхийг quota зөвшөөрнө.
        </p>
        <Link
          href="/pricing"
          className="inline-flex items-center justify-center rounded-xl border px-4 py-2 font-extrabold hover:opacity-90"
        >
          Upgrade / Pricing
        </Link>
      </div>
    </div>
  );
}
