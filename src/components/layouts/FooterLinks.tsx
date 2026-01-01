import Link from "next/link";

export function FooterLinks() {
  return (
    <div style={{ maxWidth: 980, margin: "0 auto", display: "flex", gap: 12, fontSize: 12, color: "var(--muted)" }}>
      <Link href="/terms">Terms</Link>
      <Link href="/privacy">Privacy</Link>
      <a href="mailto:support@goodpdf.org">Contact</a>
    </div>
  );
}
