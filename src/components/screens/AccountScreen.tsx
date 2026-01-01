"use client";

import { ScreenShell } from "@/components/screens/_ScreenShell";
import { Card } from "@/components/blocks/Card";
import { useAccountSummary } from "@/services/hooks/useAccountSummary";

export function AccountScreen() {
  const a = useAccountSummary();

  return (
    <ScreenShell title="Account" subtitle="Header/Login + entitlement UI goes here later.">
      <div style={{ maxWidth: 720, display: "grid", gap: 12 }}>
        <Card>
          <div style={{ display: "grid", gap: 6 }}>
            <div style={{ fontWeight: 900 }}>Usage</div>
            {a.loading ? (
              <div style={{ color: "var(--muted)" }}>Loading…</div>
            ) : (
              <div style={{ color: "var(--muted)" }}>
                {a.remainingUses} uses · {a.daysLeft} days left
              </div>
            )}
          </div>
        </Card>

        <Card>
          <div style={{ fontWeight: 900 }}>Note</div>
          <div style={{ color: "var(--muted)", marginTop: 6 }}>
            Одоогоор mock data. Supabase entitlements-тэй холбоход энэ UI автоматаар бодит болно.
          </div>
        </Card>
      </div>
    </ScreenShell>
  );
}
