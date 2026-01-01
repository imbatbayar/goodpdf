import { ScreenShell } from "@/components/screens/_ScreenShell";
import { Card } from "@/components/blocks/Card";

export function PrivacyScreen() {
  return (
    <ScreenShell title="Privacy" subtitle="MVP placeholder — privacy-first positioning.">
      <Card>
        <div style={{ color: "var(--muted)", lineHeight: 1.6 }}>
          Files are processed temporarily and deleted automatically. <br />
          (Replace this page content later.)
        </div>
      </Card>
    </ScreenShell>
  );
}
