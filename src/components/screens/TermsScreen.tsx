import { ScreenShell } from "@/components/screens/_ScreenShell";
import { Card } from "@/components/blocks/Card";

export function TermsScreen() {
  return (
    <ScreenShell title="Terms" subtitle="MVP placeholder — replace with your terms text.">
      <Card>
        <div style={{ color: "var(--muted)", lineHeight: 1.6 }}>
          By continuing, you agree to our Terms & Privacy. <br />
          (Replace this page content later.)
        </div>
      </Card>
    </ScreenShell>
  );
}
