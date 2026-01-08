"use client";

import Link from "next/link";
import { useState } from "react";
import { Button } from "@/components/ui/Button";
import styles from "./Header.module.css";
import { Tooltip } from "@/ui/primitives/Tooltip";
import { Drawer } from "@/ui/layouts/Drawer";
import { useCountry } from "@/ui/state/country";

export function Header() {
  const [open, setOpen] = useState(false);
  const country = useCountry();

  return (
    <header className={styles.header}>
      <div className={styles.inner}>
        <div className={styles.left}>
          <Link href="/" aria-label="Home" className={styles.logo}>
            good<span className={styles.logoDim}>PDF</span>
            <span className={styles.logoTld}>.org</span>
          </Link>
        </div>

        <div className={styles.right}>
          <Tooltip
            content={
              <span>
                Free upload — max 100MB (3 uses)
                <br />
                After Confirm, files auto-delete in 10 minutes
              </span>
            }
          >
            <span className={styles.pill}>
              Free 3 <span aria-hidden>💰</span>
            </span>
          </Tooltip>

          <Button
            variant="ghost"
            onClick={() => alert("Login UI-г дараа Auth-той холбоно (STAGE 3).")}
          >
            Login
          </Button>

          {/* ☰ Menu */}
          <button
            onClick={() => setOpen(true)}
            aria-label="Menu"
            style={{
              border: "1px solid var(--border)",
              background: "transparent",
              borderRadius: 12,
              padding: "8px 12px",
              fontWeight: 900,
              cursor: "pointer",
            }}
          >
            ☰
          </button>

          <Drawer
            open={open}
            onClose={() => setOpen(false)}
            title="Menu"
            items={[
              { label: "Account", href: "/account" },
              { label: "Billing / Upgrade", href: "/pricing" },
              { label: "Usage", href: "/usage" },
              { label: "Privacy", href: "/privacy" },
              { label: "Terms", href: "/terms" },
              { label: "Logout", disabled: true },
            ]}
            footer={
              <div style={{ fontSize: 12, opacity: 0.85, lineHeight: 1.35 }}>
                Country: <b>{country}</b>
                <br />
                Change country in <b>Account</b>.
              </div>
            }
          />
        </div>
      </div>
    </header>
  );
}
