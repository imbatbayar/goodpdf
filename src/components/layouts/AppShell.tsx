import { Header } from "@/components/layouts/Header";
import { FooterLinks } from "@/components/layouts/FooterLinks";

export function AppShell({ children }: { children: React.ReactNode }) {
  return (
    <div style={{ minHeight: "100vh", display: "flex", flexDirection: "column" }}>
      <Header />
      <main style={{ flex: 1, padding: 24 }}>
        {children}
      </main>
      <footer style={{ padding: 16, borderTop: "1px solid var(--border)" }}>
        <FooterLinks />
      </footer>
    </div>
  );
}
