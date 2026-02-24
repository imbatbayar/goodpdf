import { ScreenShell } from "@/components/screens/_ScreenShell";
import { Card } from "@/components/blocks/Card";
import { PLANS } from "@/domain/plans/plans";

export function PricingScreen() {
  return (
    <ScreenShell title="Plans" subtitle="Token-aware plans with CPU protection on heavy files.">
      <div style={{ display: "grid", gap: 12, gridTemplateColumns: "repeat(auto-fit, minmax(240px, 1fr))" }}>
        {PLANS.map((p) => (
          <Card key={p.code}>
            <div style={{ display: "grid", gap: 6 }}>
              <div style={{ fontWeight: 900, fontSize: 18 }}>{p.name}</div>
              <div style={{ color: "var(--muted)" }}>
                {p.fileLimit} files • {p.cpuMinutesLimit} CPU min • {p.expiryDays} days
              </div>
              <div style={{ fontWeight: 900, fontSize: 22 }}>{p.priceMnt.toLocaleString()}₮</div>
              <div style={{ fontSize: 12, color: "var(--muted)" }}>
                Heavy jobs consume more CPU minutes.
              </div>
            </div>
          </Card>
        ))}
      </div>
    </ScreenShell>
  );
}
