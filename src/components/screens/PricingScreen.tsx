import { ScreenShell } from "@/components/screens/_ScreenShell";
import { Card } from "@/components/blocks/Card";
import { PLANS } from "@/domain/plans/plans";

export function PricingScreen() {
  return (
    <ScreenShell title="Plans" subtitle="Usage-based. No subscription. No auto-renew.">
      <div style={{ display: "grid", gap: 12, gridTemplateColumns: "repeat(auto-fit, minmax(240px, 1fr))" }}>
        {PLANS.map((p) => (
          <Card key={p.code}>
            <div style={{ display: "grid", gap: 6 }}>
              <div style={{ fontWeight: 900, fontSize: 18 }}>Plan {p.code}</div>
              <div style={{ color: "var(--muted)" }}>{p.days} days • {p.uses} uses</div>
              <div style={{ fontWeight: 900, fontSize: 22 }}>{p.priceMnt.toLocaleString()}₮</div>
              <div style={{ fontSize: 12, color: "var(--muted)" }}>
                1 PDF = 1 use
              </div>
            </div>
          </Card>
        ))}
      </div>
    </ScreenShell>
  );
}
