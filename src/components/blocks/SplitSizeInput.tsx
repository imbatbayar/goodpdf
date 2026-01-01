"use client";

import { Input } from "@/components/ui/Input";
import { Card } from "@/components/blocks/Card";

export function SplitSizeInput({ valueMb, onChangeMb }: { valueMb: number; onChangeMb: (n: number) => void }) {
  return (
    <Card>
      <div style={{ display: "grid", gap: 10 }}>
        <div style={{ fontWeight: 800 }}>Part size (MB)</div>
        <Input
          type="number"
          min={1}
          max={50}
          value={String(valueMb)}
          onChange={(e) => onChangeMb(Number(e.target.value))}
        />
        <div style={{ fontSize: 12, color: "var(--muted)" }}>Each part will be up to {valueMb}MB.</div>
      </div>
    </Card>
  );
}
