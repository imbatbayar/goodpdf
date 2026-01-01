"use client";

import { Card } from "@/components/blocks/Card";

export function FileDropzone({ onPick }: { onPick: (f: File) => void }) {
  return (
    <Card>
      <div style={{ display: "grid", gap: 10 }}>
        <div style={{ fontWeight: 800 }}>1 PDF</div>
        <input
          type="file"
          accept="application/pdf"
          onChange={(e) => {
            const f = e.target.files?.[0];
            if (f) onPick(f);
          }}
        />
        <div style={{ fontSize: 12, color: "var(--muted)" }}>One file per job.</div>
      </div>
    </Card>
  );
}
