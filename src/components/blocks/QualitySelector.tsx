"use client";

import { Segmented } from "@/components/ui/Segmented";
import type { QualityMode } from "@/domain/jobs/quality";
import { Card } from "@/components/blocks/Card";

export function QualitySelector({ value, onChange }: { value: QualityMode; onChange: (m: QualityMode) => void }) {
  return (
    <Card>
      <div style={{ display: "grid", gap: 10 }}>
        <div style={{ fontWeight: 800 }}>Quality</div>
        <Segmented
          value={value}
          onChange={onChange}
          options={[
            { value: "ORIGINAL", label: "Original" },
            { value: "GOOD", label: "Good" },
            { value: "MAX", label: "Max" },
          ]}
        />
        <div style={{ fontSize: 12, color: "var(--muted)" }}>
          Good is default for readable, official PDFs.
        </div>
      </div>
    </Card>
  );
}
