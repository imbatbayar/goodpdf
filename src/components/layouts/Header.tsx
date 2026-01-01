"use client";

import Link from "next/link";
import { Button } from "@/components/ui/Button";
import { usePathname } from "next/navigation";

/**
 * MVP Header:
 * - Logo (wordmark)
 * - Simple nav
 * - Login button placeholder (чи дараа Supabase auth-тай холбоно)
 */
export function Header() {
  const path = usePathname();

  return (
    <header style={{ padding: 16, borderBottom: "1px solid var(--border)", background: "var(--card)" }}>
      <div style={{ maxWidth: 980, margin: "0 auto", display: "flex", alignItems: "center", justifyContent: "space-between", gap: 12 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 14 }}>
          <Link href="/" aria-label="Home">
            <div style={{ fontWeight: 800, letterSpacing: 0.2 }}>
              good<span style={{ opacity: 0.8 }}>PDF</span><span style={{ opacity: 0.55, fontWeight: 700 }}>.org</span>
            </div>
          </Link>

          <nav style={{ display: "flex", gap: 12, fontSize: 13, opacity: 0.85 }}>
            <Link href="/upload" style={{ fontWeight: path?.includes("/upload") ? 800 : 600 }}>Upload</Link>
            <Link href="/pricing" style={{ fontWeight: path?.includes("/pricing") ? 800 : 600 }}>Pricing</Link>
            <Link href="/account" style={{ fontWeight: path?.includes("/account") ? 800 : 600 }}>Account</Link>
          </nav>
        </div>

        <Button variant="ghost" onClick={() => alert("Login UI-г дараа Supabase Auth-тай холбоно.")}>
          Login
        </Button>
      </div>
    </header>
  );
}
