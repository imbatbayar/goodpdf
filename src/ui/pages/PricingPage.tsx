"use client";

import Link from "next/link";
import { useCountry } from "@/ui/state/country";

type Plan = {
  key: "starter" | "plus" | "pro";
  name: string;
  price: string;
  tagline: string;
  credits: string;
  features: string[];
  popular?: boolean;
};

const MN_PLANS: Plan[] = [
  {
    key: "starter",
    name: "Starter",
    price: "₮9,900",
    tagline: "For light usage",
    credits: "10 credits",
    features: ["Max 500MB per split (placeholder)", "Standard queue (later)"],
  },
  {
    key: "plus",
    name: "Plus",
    price: "₮24,900",
    tagline: "Best value for most users",
    credits: "25 credits",
    features: ["Higher limits (later)", "Faster processing (later)"],
    popular: true,
  },
  {
    key: "pro",
    name: "Pro",
    price: "₮79,900",
    tagline: "For teams & power users",
    credits: "100 credits",
    features: ["Best limits (later)", "Support (later)"],
  },
];

function CheckItem({ children }: { children: React.ReactNode }) {
  return (
    <li className="flex gap-2 text-sm text-zinc-700">
      <span className="mt-0.5 inline-flex h-5 w-5 items-center justify-center rounded-full bg-emerald-50 text-emerald-700">
        ✓
      </span>
      <span className="leading-6">{children}</span>
    </li>
  );
}

function OutlineButton({
  children,
  href,
}: {
  children: React.ReactNode;
  href: string;
}) {
  return (
    <Link
      href={href}
      className="inline-flex items-center justify-center rounded-xl border border-zinc-200 bg-white px-3 py-2 text-sm font-semibold text-zinc-900 shadow-sm hover:bg-zinc-50"
    >
      {children}
    </Link>
  );
}

export function PricingPage() {
  const country = useCountry();
  const isMN = country === "Mongolia";

  const paymentTitle = isMN ? "QPay (TEST)" : "Card (TEST)";
  const paymentDesc = isMN
    ? "Монгол хэрэглэгчдэд зориулав."
    : "International cards (placeholder).";

  const privacyText = isMN
    ? "Files are deleted automatically after Confirm + TTL."
    : "Files are deleted automatically after Confirm + TTL.";

  const plans = MN_PLANS;

  return (
    <div className="mx-auto w-full max-w-6xl px-4 py-10">
      {/* Top header */}
      <div className="mb-8 flex flex-col gap-3">
        <div className="flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
          <div>
            <h1 className="text-4xl font-semibold tracking-tight text-zinc-900">
              Pricing
            </h1>
            <p className="mt-2 text-sm text-zinc-600">
              UI-only demo. Billing will be enabled in Phase E.
            </p>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <span className="inline-flex items-center rounded-full bg-zinc-100 px-3 py-1 text-sm text-zinc-800">
              Country: <span className="ml-2 font-semibold">{country}</span>
            </span>
            <OutlineButton href="/account">View account</OutlineButton>
          </div>
        </div>
      </div>

      {/* Payment banner (trust style) */}
      <div className="mb-8 overflow-hidden rounded-2xl border border-zinc-200 bg-white shadow-sm">
        <div className="flex flex-col gap-4 p-6 sm:flex-row sm:items-center sm:justify-between">
          <div className="flex items-start gap-3">
            <div className="inline-flex h-11 w-11 items-center justify-center rounded-xl bg-zinc-900 text-white shadow-sm">
              $
            </div>
            <div>
              <div className="flex flex-wrap items-center gap-2">
                <p className="text-sm font-semibold text-zinc-900">
                  Payment method
                </p>
                <span className="inline-flex items-center rounded-full bg-emerald-50 px-2.5 py-1 text-xs font-semibold text-emerald-700">
                  {paymentTitle}
                </span>
              </div>
              <p className="mt-1 text-sm text-zinc-600">{paymentDesc}</p>
              <p className="mt-1 text-xs text-zinc-500">{privacyText}</p>
            </div>
          </div>

          <div className="rounded-xl border border-zinc-200 bg-zinc-50 px-4 py-3 text-xs text-zinc-600">
            <div className="font-semibold text-zinc-900">Note</div>
            webhook + credits grant нь Phase E дээр орно.
          </div>
        </div>

        <div className="h-px bg-zinc-200" />

        <div className="flex flex-col gap-2 px-6 py-4 text-xs text-zinc-500 sm:flex-row sm:items-center sm:justify-between">
          <span>Free: 3 uses • max 100MB</span>
          <span>Paid: credits • higher limits (soon)</span>
        </div>
      </div>

      {/* Plans */}
      <div className="grid gap-5 md:grid-cols-3">
        {plans.map((p) => {
          const popular = !!p.popular;

          return (
            <div
              key={p.key}
              className={[
                "relative rounded-2xl border bg-white p-6 shadow-sm",
                popular
                  ? "border-zinc-900 shadow-md ring-1 ring-zinc-900/10"
                  : "border-zinc-200",
              ].join(" ")}
            >
              {/* subtle gradient for popular */}
              {popular ? (
                <div className="pointer-events-none absolute inset-0 rounded-2xl bg-linear-to-b from-zinc-50 to-white" />
              ) : null}

              <div className="relative">
                <div className="mb-5 flex items-start justify-between gap-3">
                  <div>
                    <p className="text-lg font-semibold text-zinc-900">
                      {p.name}
                    </p>
                    <p className="mt-1 text-sm text-zinc-600">{p.tagline}</p>
                  </div>

                  {popular ? (
                    <span className="inline-flex items-center rounded-full bg-zinc-900 px-3 py-1 text-xs font-semibold text-white">
                      Most popular
                    </span>
                  ) : null}
                </div>

                <div className="mb-5">
                  <div className="flex items-end gap-2">
                    <div className="text-4xl font-semibold tracking-tight text-zinc-900">
                      {p.price}
                    </div>
                  </div>
                  <div className="mt-1 text-sm text-zinc-600">
                    <span className="font-semibold text-zinc-900">
                      {p.credits}
                    </span>{" "}
                    per purchase
                  </div>
                </div>

                <ul className="mb-6 space-y-2">
                  <CheckItem>{p.credits}</CheckItem>
                  {p.features.map((f) => (
                    <CheckItem key={f}>{f}</CheckItem>
                  ))}
                </ul>

                <button
                  className={[
                    "w-full rounded-xl px-4 py-3 text-sm font-semibold transition shadow-sm",
                    popular
                      ? "bg-zinc-900 text-white hover:bg-zinc-800"
                      : "bg-zinc-100 text-zinc-900 hover:bg-zinc-200",
                  ].join(" ")}
                  onClick={() => alert(`Demo: ${p.name} → ${paymentTitle}`)}
                >
                  {isMN ? "Pay with QPay" : "Pay with Card"}
                </button>

                <div className="mt-3 flex items-center justify-between text-xs text-zinc-500">
                  <span>
                    {isMN
                      ? "QPay key орж ирэхээр webhook холбоно."
                      : "Card provider (later)."}
                  </span>
                  <Link
                    href="/usage"
                    className="font-semibold text-zinc-900 underline underline-offset-4 hover:text-zinc-700"
                  >
                    See usage
                  </Link>
                </div>
              </div>
            </div>
          );
        })}
      </div>

      {/* Footer note */}
      <div className="mt-10 rounded-2xl border border-zinc-200 bg-white p-6 text-sm text-zinc-600 shadow-sm">
        <div className="font-semibold text-zinc-900">What happens next?</div>
        <div className="mt-2 leading-6">
          Phase D дээр free uses бодитоор хасагдана. Phase E дээр payment webhook
          + credits grant орж ирнэ.
        </div>
      </div>
    </div>
  );
}
