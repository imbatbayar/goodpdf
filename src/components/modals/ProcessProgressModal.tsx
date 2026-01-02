"use client";

import { Progress } from "@/components/ui/Progress";

export function ProcessProgressModal(props: {
  open: boolean;
  compressPct: number; // 0..100
  splitPct: number; // 0..100
}) {
  const { open, compressPct, splitPct } = props;
  if (!open) return null;

  const c = Math.max(0, Math.min(100, compressPct));
  const s = Math.max(0, Math.min(100, splitPct));

  const cDone = c >= 100;
  const sDone = s >= 100;

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
          width: "min(460px, 92vw)",
          borderRadius: 18,
          background: "white",
          border: "1px solid rgba(15,23,42,.12)",
          boxShadow: "0 20px 60px rgba(0,0,0,.20)",
          padding: 16,
          display: "grid",
          gap: 14,
        }}
      >
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 12 }}>
          <div style={{ fontWeight: 900, fontSize: 14 }}>Processing…</div>
          <div style={{ fontSize: 12, color: "rgba(15,23,42,.65)", fontWeight: 800 }}>
            {Math.round((c + s) / 2)}%
          </div>
        </div>

        <div style={{ display: "grid", gap: 10 }}>
          <div style={{ display: "grid", gap: 6 }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <div style={{ fontSize: 12, fontWeight: 900, color: "rgba(15,23,42,.75)" }}>
                Compressing {cDone ? "✅" : ""}
              </div>
              <div style={{ fontSize: 12, color: "rgba(15,23,42,.65)", fontWeight: 800 }}>{Math.round(c)}%</div>
            </div>
            <Progress value={c} />
          </div>

          <div style={{ display: "grid", gap: 6 }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <div style={{ fontSize: 12, fontWeight: 900, color: "rgba(15,23,42,.75)" }}>
                Splitting {sDone ? "✅" : ""}
              </div>
              <div style={{ fontSize: 12, color: "rgba(15,23,42,.65)", fontWeight: 800 }}>{Math.round(s)}%</div>
            </div>
            <Progress value={s} />
          </div>
        </div>

        <div style={{ fontSize: 12, color: "rgba(15,23,42,.65)", lineHeight: 1.4 }}>
          Please keep this tab open while processing finishes.
        </div>
      </div>
    </div>
  );
}
