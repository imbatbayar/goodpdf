"use client";

import Link from "next/link";

type Plan = {
  key: "d10" | "d30" | "d90";
  name: string;
  price: string;
  period: string;
  bullets: string[];
  popular?: boolean;
  accent: "slate" | "green" | "emerald";
};

const PLANS: Plan[] = [
  {
    key: "d10",
    name: "10 days",
    price: "$9",
    period: "15 files • 3 GB downloads",
    accent: "slate",
    bullets: ["Split by size", "ZIP download", "Auto-delete after 10 minutes"],
  },
  {
    key: "d30",
    name: "30 days",
    price: "$19",
    period: "50 files • 8 GB downloads",
    accent: "green",
    popular: true,
    bullets: ["Split by size", "ZIP download", "Auto-delete after 10 minutes"],
  },
  {
    key: "d90",
    name: "90 days",
    price: "$39",
    period: "120 files • 20 GB downloads",
    accent: "emerald",
    bullets: ["Split by size", "ZIP download", "Auto-delete after 10 minutes"],
  },
];

function AccentLine({ accent }: { accent: Plan["accent"] }) {
  const cls =
    accent === "green"
      ? "bg-green-600"
      : accent === "emerald"
        ? "bg-emerald-700"
        : "bg-slate-500";
  return <div className={`mx-auto mt-2 h-1 w-12 rounded-full ${cls}`} />;
}

function CheckRow({ children }: { children: React.ReactNode }) {
  return (
    <li className="flex items-start gap-2 text-[13px] text-zinc-700">
      <span className="mt-0.5 inline-flex h-5 w-5 items-center justify-center rounded-full bg-zinc-100 text-zinc-900">
        ✓
      </span>
      <span className="leading-5">{children}</span>
    </li>
  );
}

export function PricingPage() {
  return (
    <div className="w-full">
      {/* tighter vertical padding so footer always visible in no-scroll mode */}
      <div className="mx-auto w-full max-w-6xl px-4 py-6">
        {/* Header */}
        <div className="mx-auto mb-6 max-w-2xl text-center">
          <h1 className="text-3xl font-semibold tracking-tight text-zinc-900">
            Plans
          </h1>
          <p className="mt-2 text-sm text-zinc-600">
            Simple limits. Download-based quotas.
          </p>
        </div>

        {/* Cards */}
        <div className="grid gap-5 md:grid-cols-3">
          {PLANS.map((p) => {
            const isPopular = !!p.popular;

            const btnCls =
              p.accent === "green"
                ? "bg-green-600 hover:bg-green-700"
                : p.accent === "emerald"
                  ? "bg-emerald-700 hover:bg-emerald-800"
                  : "bg-slate-700 hover:bg-slate-800";

            const cardShadow = isPopular
              ? "ring-1 ring-zinc-900/10 shadow-[0_18px_45px_rgba(0,0,0,0.12)]"
              : "shadow-[0_14px_40px_rgba(0,0,0,0.10)]";

            return (
              <div
                key={p.key}
                className={[
                  "relative overflow-hidden rounded-3xl bg-white p-6",
                  cardShadow,
                  isPopular ? "md:-translate-y-1" : "",
                ].join(" ")}
              >
                <div className="text-center">
                  <div className="text-base font-semibold text-zinc-900">
                    {p.name}
                  </div>

                  <AccentLine accent={p.accent} />

                  <div className="mt-5 flex items-end justify-center">
                    <div className="text-5xl font-semibold tracking-tight text-zinc-900">
                      {p.price}
                    </div>
                  </div>

                  <div className="mt-2 text-xs text-zinc-500">{p.period}</div>
                </div>

                <div className="mt-5">
                  <ul className="space-y-2">
                    {p.bullets.map((b) => (
                      <CheckRow key={b}>{b}</CheckRow>
                    ))}
                  </ul>
                </div>

                <div className="mt-5">
                  <button
                    className={[
                      "w-full rounded-2xl px-5 py-3 text-sm font-semibold text-white shadow-sm transition",
                      btnCls,
                    ].join(" ")}
                    onClick={() => {
                      // TODO (Phase 3): route to checkout
                    }}
                  >
                    Choose
                  </button>

                  <div className="mt-2 text-center text-xs text-zinc-500">
                    Billing coming soon.
                  </div>
                </div>
              </div>
            );
          })}
        </div>

        {/* Tiny footer links (tight) */}
        <div className="mx-auto mt-5 flex max-w-2xl flex-wrap items-center justify-center gap-x-4 gap-y-1 text-[11px] text-zinc-500">
          <span>Privacy-first: auto-delete after 10 minutes.</span>
          <Link
            href="/privacy"
            className="font-semibold text-zinc-900 underline underline-offset-4 hover:text-zinc-700"
          >
            Privacy
          </Link>
          <Link
            href="/terms"
            className="font-semibold text-zinc-900 underline underline-offset-4 hover:text-zinc-700"
          >
            Terms
          </Link>
        </div>
      </div>
    </div>
  );
}
