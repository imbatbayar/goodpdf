import { Card } from "@/components/blocks/Card";

export function ScreenShell({ title, subtitle, children }: { title: string; subtitle?: string; children: React.ReactNode }) {
  return (
    <div style={{ maxWidth: 980, margin: "0 auto", display: "grid", gap: 16 }}>
      <div style={{ display: "grid", gap: 6 }}>
        <h2 style={{ fontSize: 28 }}>{title}</h2>
        {subtitle ? <div style={{ color: "var(--muted)" }}>{subtitle}</div> : null}
      </div>
      {children}
    </div>
  );
}
