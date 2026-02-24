"use client";

import Link from "next/link";
import { useRef, useState } from "react";
import { Button } from "@/components/ui/Button";
import styles from "./Header.module.css";
import { Tooltip } from "@/ui/primitives/Tooltip";
import { useCountry } from "@/ui/state/country";
import { AccountDropdown } from "./account/AccountDropdown";

export function Header() {
  const [open, setOpen] = useState(false);
  const country = useCountry();
  const anchorRef = useRef<HTMLButtonElement | null>(null);

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
                BASIC plan — 30 files, 60 CPU minutes, 30 days
                <br />
                After Confirm, files auto-delete in 10 minutes
              </span>
            }
          >
            <span className={styles.pill}>
              BASIC <span aria-hidden>💰</span>
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
            ref={anchorRef}
            onClick={() => setOpen((v) => !v)}
            aria-label="Menu"
            className={styles.menuBtn}
          >
            ☰
          </button>

          <AccountDropdown
            open={open}
            onClose={() => setOpen(false)}
            anchorRef={anchorRef}
            country={country}
            email="you@example.com"
            planLabel="BASIC"
          />
        </div>
      </div>
    </header>
  );
}
