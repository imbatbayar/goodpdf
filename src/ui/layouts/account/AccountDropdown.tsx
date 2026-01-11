"use client";

import Link from "next/link";
import { useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import styles from "./AccountDropdown.module.css";

type Props = {
  open: boolean;
  onClose: () => void;
  anchorRef: React.RefObject<HTMLElement | null>;
  country: string;
  email: string;
  planLabel?: string; // e.g. FREE
};

type Pos = { top: number; right: number };

export function AccountDropdown({
  open,
  onClose,
  anchorRef,
  country,
  email,
  planLabel = "FREE",
}: Props) {
  const panelRef = useRef<HTMLDivElement | null>(null);
  const [pos, setPos] = useState<Pos>({ top: 0, right: 0 });

  const items = useMemo(
    () => [
      { label: "Account", href: "/account" },
      { label: "Usage", href: "/usage" },
      { label: "Privacy", href: "/privacy" },
      { label: "Terms", href: "/terms" },
    ],
    []
  );

  function recomputePos() {
    const a = anchorRef.current;
    if (!a) return;
    const r = a.getBoundingClientRect();

    // Keep a consistent breathing space from the top header (trigger row)
    // and the right edge of the window.
    const GAP = 16;

    // Open below the header row so we never cover the header.
    // (Gap from header == gap from right edge.)
    let top = Math.round(r.bottom + GAP);

    // Keep inside viewport if we can measure the panel height.
    const panelH = panelRef.current?.offsetHeight ?? 0;
    if (panelH > 0) {
      const maxTop = Math.max(GAP, window.innerHeight - panelH - GAP);
      top = Math.min(top, maxTop);
    }

    top = Math.max(GAP, top);
    // Keep a small inset from the right edge of the window.
    // (Even if the trigger is at the far right.)
    const right = GAP;
    setPos({ top, right });
  }

  useLayoutEffect(() => {
    if (!open) return;
    recomputePos();
    // next paints (fonts/layout can shift + panel height becomes measurable)
    const t1 = window.setTimeout(recomputePos, 0);
    const t2 = window.setTimeout(recomputePos, 50);
    return () => {
      window.clearTimeout(t1);
      window.clearTimeout(t2);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  useEffect(() => {
    if (!open) return;

    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") onClose();
    }

    function onPointerDown(e: MouseEvent | PointerEvent) {
      const panel = panelRef.current;
      const anchor = anchorRef.current;
      const t = e.target as Node | null;
      if (!t) return;
      if (panel && panel.contains(t)) return;
      if (anchor && anchor.contains(t)) return;
      onClose();
    }

    function onReflow() {
      recomputePos();
    }

    window.addEventListener("keydown", onKey);
    window.addEventListener("pointerdown", onPointerDown);
    window.addEventListener("resize", onReflow);
    window.addEventListener("scroll", onReflow, true);
    return () => {
      window.removeEventListener("keydown", onKey);
      window.removeEventListener("pointerdown", onPointerDown);
      window.removeEventListener("resize", onReflow);
      window.removeEventListener("scroll", onReflow, true);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  if (!open) return null;

  return (
    <div
      ref={panelRef}
      className={styles.panel}
      style={{ top: pos.top, right: pos.right }}
      role="menu"
      aria-label="Account menu"
    >
      <div className={styles.header}>
        <div className={styles.identity}>
          <span className={styles.avatar} aria-hidden>
            G
          </span>
          <div className={styles.meta}>
            <div className={styles.countryRow}>
              <span className={styles.country}>{country}</span>
              <span className={styles.badge}>{planLabel}</span>
            </div>
            <div className={styles.email}>{email}</div>
          </div>
        </div>
      </div>

      <Link
        href="/pricing"
        className={styles.upgrade}
        onClick={() => onClose()}
      >
        <span className={styles.crown} aria-hidden>
          👑
        </span>
        <span className={styles.upgradeText}>Upgrade</span>
        <span className={styles.proPill}>PRO</span>
      </Link>

      <div className={styles.section}>
        {items.map((it) => (
          <Link
            key={it.href}
            href={it.href}
            className={styles.item}
            onClick={() => onClose()}
            role="menuitem"
          >
            {it.label}
          </Link>
        ))}
      </div>

      <button
        type="button"
        className={styles.logout}
        onClick={() => {
          onClose();
          alert("Sign out will be wired after Auth.");
        }}
      >
        Sign out
      </button>
    </div>
  );
}
