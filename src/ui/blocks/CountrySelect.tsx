"use client";

import { COUNTRIES, type Country } from "@/ui/state/country";

export function CountrySelect({
  value,
  onChange,
}: {
  value: Country;
  onChange: (v: Country) => void;
}) {
  return (
    <label style={{ display: "grid", gap: 6 }}>
      <div style={{ fontSize: 12, opacity: 0.75, fontWeight: 800 }}>Country</div>
      <select
        value={value}
        onChange={(e) => onChange(e.target.value as Country)}
        style={{
          width: "100%",
          padding: "10px 12px",
          borderRadius: 12,
          border: "1px solid var(--border)",
          background: "var(--card)",
          fontWeight: 800,
        }}
      >
        {COUNTRIES.map((c) => (
          <option key={c} value={c}>
            {c === "Mongolia" ? "Mongolia (MN)" : c}
          </option>
        ))}
      </select>
    </label>
  );
}
