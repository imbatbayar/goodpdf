"use client";

export type SegOption<T extends string> = { value: T; label: string };

export function Segmented<T extends string>({
  value,
  options,
  onChange,
}: {
  value: T;
  options: SegOption<T>[];
  onChange: (v: T) => void;
}) {
  return (
    <div style={{ display: "flex", border: "1px solid var(--border)", borderRadius: 14, overflow: "hidden", background: "var(--card)" }}>
      {options.map((o) => (
        <button
          key={o.value}
          onClick={() => onChange(o.value)}
          style={{
            flex: 1,
            padding: "10px 12px",
            background: value === o.value ? "var(--ink)" : "transparent",
            color: value === o.value ? "#fff" : "var(--ink)",
            border: 0,
            cursor: "pointer",
            fontWeight: 800,
          }}
        >
          {o.label}
        </button>
      ))}
    </div>
  );
}
