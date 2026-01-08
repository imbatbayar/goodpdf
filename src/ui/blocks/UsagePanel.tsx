"use client";

export function UsagePanel({
  mode = "free",
  freeLeft = 3,
  maxMb = 100,
}: {
  mode?: "free" | "paid";
  freeLeft?: number;
  maxMb?: number;
}) {
  return (
    <div
      style={{
        border: "1px solid var(--border)",
        borderRadius: 14,
        padding: 12,
        background: "var(--card)",
        display: "grid",
        gap: 8,
      }}
    >
      <div style={{ display: "flex", justifyContent: "space-between", gap: 10 }}>
        <div style={{ fontWeight: 900 }}>Usage</div>
        <div style={{ fontSize: 11, fontWeight: 900, opacity: 0.75 }}>
          UI ONLY
        </div>
      </div>

      {mode === "free" ? (
        <div style={{ fontSize: 12, lineHeight: 1.35, opacity: 0.9 }}>
          <b>Free left:</b> {freeLeft}/3
          <br />
          <b>Max upload:</b> {maxMb}MB
          <br />
          <span style={{ opacity: 0.85 }}>
            Hover “Free 3 💰” дээр дэлгэрэнгүй гарна.
          </span>
        </div>
      ) : (
        <div style={{ fontSize: 12, lineHeight: 1.35, opacity: 0.9 }}>
          <b>Credits:</b> (coming soon)
          <br />
          <b>Max upload:</b> 500MB
        </div>
      )}
    </div>
  );
}
