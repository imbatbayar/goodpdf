"use client";

import { CountrySelect } from "@/ui/blocks/CountrySelect";
import { saveCountry, useCountry } from "@/ui/state/country";

export function AccountPage() {
  const country = useCountry();

  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <h1 className="text-2xl font-extrabold">Account</h1>
        <p className="text-sm opacity-80">
          Одоогоор энэ хуудас зөвхөн UI. Auth, billing холбоос дараагийн
          шатанд орно.
        </p>
      </div>

      <div className="rounded-2xl border bg-[var(--card)] p-5 space-y-4">
        <div className="grid gap-3 md:grid-cols-2">
          <div>
            <div className="text-sm font-extrabold mb-2">Country</div>
            <CountrySelect
              value={country}
              onChange={(v) => {
                saveCountry(v);
              }}
            />
          </div>

          <div className="text-sm leading-6">
            <div className="font-extrabold mb-1">Payment method (auto)</div>
            {country === "Mongolia" ? (
              <ul className="list-disc pl-5 opacity-85">
                <li>QPay (test) — Монгол хэрэглэгчдэд</li>
                <li>Карт төлбөр дараа нэмэгдэнэ</li>
              </ul>
            ) : (
              <ul className="list-disc pl-5 opacity-85">
                <li>Card (test) — International users</li>
                <li>QPay only for Mongolia</li>
              </ul>
            )}
          </div>
        </div>

        <div className="rounded-xl border p-4 text-sm leading-6 bg-white/0">
          <div className="font-extrabold mb-1">Why this matters</div>
          <div className="opacity-85">
            Country сонгосноор Pricing дээр харагдах төлбөрийн арга, текст,
            мөн quota/credit-н UI автоматаар өөрчлөгдөнө.
          </div>
        </div>
      </div>
    </div>
  );
}
