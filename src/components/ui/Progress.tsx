"use client";

export function Progress({ value }: { value: number }) {
  const pct = Math.max(0, Math.min(100, Math.round(value || 0)));

  return (
    <div
      className="h-[10px] w-full overflow-hidden rounded-full bg-[rgba(15,23,42,.08)]"
      role="progressbar"
      aria-valuemin={0}
      aria-valuemax={100}
      aria-valuenow={pct}
    >
      <div
        className="h-full rounded-full bg-[var(--primary)] transition-[width] duration-300 ease-out"
        style={{ width: `${pct}%` }}
      />
    </div>
  );
}
