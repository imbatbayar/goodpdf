"use client";

import * as React from "react";
import type { Country } from "@/ui/state/country";

const COUNTRIES: Country[] = [
  
  "United States",
  "United Kingdom",
  "Germany",
  "France",
  "Canada",
  "Australia",
  "Japan",
  "South Korea",
  "Singapore",
  "India",
  "Mongolia",
  "Other",
  
];

export function CountrySelect({
  value,
  onChange,
  disabled,
}: {
  value: Country;
  onChange: (c: Country) => void;
  disabled?: boolean;
}) {
  return (
    <div className="w-full">
      <select
        value={value}
        disabled={disabled}
        onChange={(e) => onChange(e.target.value as Country)}
        className={[
          "w-full rounded-xl border px-3 py-2 text-sm outline-none transition",
          disabled
            ? "cursor-not-allowed border-zinc-200 bg-zinc-100 text-zinc-500"
            : "border-zinc-300 bg-white text-zinc-900 focus:border-zinc-900",
        ].join(" ")}
      >
        {COUNTRIES.map((c) => (
          <option key={c} value={c}>
            {c === "Mongolia" ? "Mongolia (MN)" : c}
          </option>
        ))}
      </select>
    </div>
  );
}
