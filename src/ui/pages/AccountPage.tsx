"use client";

import { useMemo } from "react";
import { CountrySelect } from "@/ui/blocks/CountrySelect";
import { useCountry, isCountryLocked, setCountryOnce } from "@/ui/state/country";

export function AccountPage() {
  const country = useCountry();

  // client-only lock state (SSR safe)
  const locked = useMemo(() => {
    if (typeof window === "undefined") return false;
    return isCountryLocked();
  }, []);

  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <h1 className="text-2xl font-extrabold">Account</h1>
        <p className="text-sm opacity-80">
          Одоогоор энэ хуудас зөвхөн UI. Auth, billing холбоос дараагийн шатанд орно.
        </p>
      </div>

      <div className="rounded-2xl border bg-[var(--card)] p-5 space-y-5">
        {/* Country block */}
        <div className="rounded-2xl border border-zinc-200 bg-white/0 p-4">
          <div className="flex items-start justify-between gap-3">
            <div>
              <div className="text-sm font-extrabold">Country</div>
              <div className="text-xs opacity-75 mt-1">
                {locked ? "Нэгэнт сонгогдсон тул түгжигдсэн." : "Зөвхөн 1 удаа сонгоно. Дараа нь түгжинэ."}
              </div>
            </div>

            {locked ? (
              <span className="inline-flex items-center rounded-full bg-zinc-900 px-3 py-1 text-xs font-medium text-white">
                Locked
              </span>
            ) : (
              <span className="inline-flex items-center rounded-full bg-amber-50 px-3 py-1 text-xs font-medium text-amber-700">
                Setup
              </span>
            )}
          </div>

          <div className="mt-4">
            {locked ? (
              <div className="flex flex-wrap items-center gap-2">
                <span className="inline-flex items-center rounded-xl bg-zinc-100 px-3 py-2 text-sm text-zinc-900">
                  {country}
                </span>
                <span className="text-xs opacity-70">
                  (Дахиж өөрчлөх боломжгүй)
                </span>
              </div>
            ) : (
              <div className="flex flex-col gap-3 sm:flex-row sm:items-center">
                <CountrySelect
                  value={country}
                  onChange={(v) => {
                    // 1 удаа: save + lock
                    setCountryOnce(v);
                  }}
                />
                <div className="text-xs opacity-70">
                  Сонгосон даруйд түгжинэ.
                </div>
              </div>
            )}
          </div>

          {!locked ? (
            <div className="mt-4 rounded-xl bg-amber-50 p-3 text-sm text-amber-900/90">
              <div className="font-extrabold mb-1">Анхаар</div>
              Энэ сонголт тухайн browser дээр permanent болно. (Дараа нь auth-той
              холбох үед хэрэглэгчийн account түвшинд түгжинэ.)
            </div>
          ) : (
            <div className="mt-4 rounded-xl bg-zinc-50 p-3 text-sm text-zinc-800/90">
              <div className="font-extrabold mb-1">Status</div>
              Payment method, pricing текст, quota/credit UI нь энэ country дээр
              суурилж автоматаар таарна.
            </div>
          )}
        </div>

        {/* Payment method */}
        <div className="grid gap-3 md:grid-cols-2">
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

          <div className="rounded-xl border p-4 text-sm leading-6 bg-white/0">
            <div className="font-extrabold mb-1">Why this matters</div>
            <div className="opacity-85">
              Country сонгосноор Pricing дээр харагдах төлбөрийн арга, текст,
              мөн quota/credit-н UI автоматаар өөрчлөгдөнө.
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
