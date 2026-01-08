"use client";

import Link from "next/link";
import { useCountry } from "@/ui/state/country";

function PlanCard({
  title,
  price,
  note,
  bullets,
  cta,
}: {
  title: string;
  price: string;
  note: string;
  bullets: string[];
  cta: string;
}) {
  return (
    <div className="rounded-2xl border bg-[var(--card)] p-5 space-y-4">
      <div>
        <div className="text-sm opacity-75 font-bold">{title}</div>
        <div className="text-3xl font-extrabold leading-tight">{price}</div>
        <div className="text-xs opacity-70 mt-1">{note}</div>
      </div>
      <ul className="text-sm space-y-2">
        {bullets.map((b) => (
          <li key={b} className="flex gap-2">
            <span aria-hidden>✓</span>
            <span className="opacity-90">{b}</span>
          </li>
        ))}
      </ul>
      <button
        className="w-full rounded-xl border px-4 py-2 font-extrabold hover:opacity-90"
        onClick={() => alert("Demo only — payment will be wired in PHASE E")}
      >
        {cta}
      </button>
    </div>
  );
}

export function PricingPage() {
  const country = useCountry();
  const isMN = country === "Mongolia";

  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <h1 className="text-2xl font-extrabold">Pricing</h1>
        <p className="text-sm opacity-80">
          UI-only (demo). Country: <b>{country}</b> — change in{" "}
          <Link className="underline" href="/account">
            Account
          </Link>
          .
        </p>
      </div>

      <div className="rounded-2xl border bg-[var(--card)] p-5 space-y-3">
        <div className="font-extrabold">Payment method</div>
        {isMN ? (
          <div className="text-sm leading-6 opacity-85">
            <div>
              <b>QPay (TEST)</b> — Монгол хэрэглэгчдэд.
            </div>
            <div>Privacy: файлууд Confirm хийснээс хойш TTL хугацаанд автоматаар устна.</div>
          </div>
        ) : (
          <div className="text-sm leading-6 opacity-85">
            <div>
              <b>Card (TEST)</b> — International users.
            </div>
            <div>
              Privacy: files are auto-deleted after Confirm (TTL policy).
            </div>
          </div>
        )}
      </div>

      <div className="grid gap-4 md:grid-cols-3">
        <PlanCard
          title="Starter"
          price={isMN ? "₮9,900" : "$4.99"}
          note="Demo price"
          bullets={["10 credits", "Max 500MB per split (placeholder)", "Priority queue (later)"]}
          cta={isMN ? "Pay with QPay" : "Pay with Card"}
        />
        <PlanCard
          title="Plus"
          price={isMN ? "₮24,900" : "$9.99"}
          note="Demo price"
          bullets={["25 credits", "Higher limits (later)", "Faster processing (later)"]}
          cta={isMN ? "Pay with QPay" : "Pay with Card"}
        />
        <PlanCard
          title="Pro"
          price={isMN ? "₮79,900" : "$29.99"}
          note="Demo price"
          bullets={["100 credits", "Best limits (later)", "Support (later)"]}
          cta={isMN ? "Pay with QPay" : "Pay with Card"}
        />
      </div>

      <div className="text-xs opacity-70 leading-5">
        Note: Энэ бол зөвхөн UI. Төлбөр баталгаажуулалт (webhook) + credits grant нь PHASE E дээр орно.
      </div>
    </div>
  );
}
