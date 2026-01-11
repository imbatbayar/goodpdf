import { Header } from "@/ui/layouts/Header";
import { FooterLinks } from "@/components/layouts/FooterLinks";
import styles from "./AppShell.module.css";

export function AppShell({ children }: { children: React.ReactNode }) {
  return (
    <div className={styles.shell}>
      <Header />

      <main className={styles.main}>
        <div className={styles.mainInner}>{children}</div>
      </main>

      <footer className={styles.footer}>
        <FooterLinks />
      </footer>
    </div>
  );
}
