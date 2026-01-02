"use client";

import { Progress } from "@/components/ui/Progress";

export function UploadProgressModal(props: { open: boolean; percent: number }) {
  const { open, percent } = props;
  if (!open) return null;

  const pct = Math.max(0, Math.min(100, percent));
  const done = pct >= 100;

  return (
    <div
      style={{
        position: "fixed",
        inset: 0,
        zIndex: 50,
        background: "rgba(2,6,23,.45)",
        display: "grid",
        placeItems: "center",
        padding: 16,
      }}
    >
      <div
        style={{
          width: "min(420px, 92vw)",
          borderRadius: 18,
          background: "white",
          border: "1px solid rgba(15,23,42,.12)",
          boxShadow: "0 20px 60px rgba(0,0,0,.20)",
          padding: 16,
          display: "grid",
          gap: 12,
        }}
      >
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 12 }}>
          <div style={{ fontWeight: 900, fontSize: 14 }}>{done ? "Uploaded ✅" : "Uploading…"}</div>
          <div style={{ fontSize: 12, color: "rgba(15,23,42,.65)", fontWeight: 800 }}>
            {Math.round(pct)}%
          </div>
        </div>

        <Progress value={pct} />

        <div style={{ fontSize: 12, color: "rgba(15,23,42,.65)", lineHeight: 1.4 }}>
          {done ? "Upload complete. You can continue to settings." : "Please keep this tab open until upload finishes."}
        </div>
      </div>
    </div>
  );
}
