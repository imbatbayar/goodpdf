"use client";

export function Progress({ value }: { value: number }) {
  const pct = Math.max(0, Math.min(100, Math.round(value || 0)));

  return (
    <div
      className="h-2 w-full overflow-hidden rounded-full bg-zinc-100"
      role="progressbar"
      aria-valuemin={0}
      aria-valuemax={100}
      aria-valuenow={pct}
    >
      <div
        className="h-full rounded-full bg-(--primary) transition-[width] duration-300 ease-out"
        style={{ width: `${pct}%` }}
      />
    </div>
  );
}
