"use client";

import { useEffect } from "react";

type Item = { label: string; href?: string; onClick?: () => void; disabled?: boolean };

export function Drawer({
  open,
  onClose,
  title = "Menu",
  items,
  footer,
}: {
  open: boolean;
  onClose: () => void;
  title?: string;
  items: Item[];
  footer?: React.ReactNode;
}) {
  // ESC to close
  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open, onClose]);

  if (!open) return null;

  return (
    <div
      style={{
        position: "fixed",
        inset: 0,
        zIndex: 100,
        background: "rgba(0,0,0,0.25)",
        display: "flex",
        justifyContent: "flex-end",
      }}
      onMouseDown={onClose}
    >
      <aside
        style={{
          width: "min(360px, 90vw)",
          height: "100%",
          background: "var(--card)",
          borderLeft: "1px solid var(--border)",
          padding: 16,
          boxShadow: "0 20px 60px rgba(15,23,42,.20)",
          display: "flex",
          flexDirection: "column",
          gap: 12,
        }}
        onMouseDown={(e) => e.stopPropagation()}
        aria-modal="true"
        role="dialog"
      >
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 10 }}>
          <div style={{ fontWeight: 900 }}>{title}</div>
          <button
            onClick={onClose}
            style={{
              border: "1px solid var(--border)",
              background: "transparent",
              borderRadius: 10,
              padding: "6px 10px",
              fontWeight: 800,
              cursor: "pointer",
            }}
          >
            ✕
          </button>
        </div>

        <div style={{ display: "grid", gap: 8, paddingTop: 6 }}>
          {items.map((it) => {
            const common = {
              key: it.label,
              style: {
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                padding: "10px 12px",
                borderRadius: 12,
                border: "1px solid var(--border)",
                background: "var(--card)",
                fontWeight: 800,
                cursor: it.disabled ? "not-allowed" : "pointer",
                opacity: it.disabled ? 0.55 : 1,
                textDecoration: "none",
                color: "inherit",
              } as React.CSSProperties,
            };

            if (it.href && !it.disabled) {
              return (
                <a {...common} href={it.href}>
                  <span>{it.label}</span>
                  <span style={{ opacity: 0.5 }}>›</span>
                </a>
              );
            }

            return (
              <div
                {...common}
                onClick={() => {
                  if (it.disabled) return;
                  it.onClick?.();
                  onClose();
                }}
              >
                <span>{it.label}</span>
                <span style={{ opacity: 0.5 }}>{it.disabled ? "Soon" : "›"}</span>
              </div>
            );
          })}
        </div>

        <div style={{ marginTop: "auto" }}>
          {footer ? (
            footer
          ) : (
            <div style={{ fontSize: 12, opacity: 0.75, lineHeight: 1.3 }}>
              Mongolia • QPay (coming soon)<br />
              Others • Card (coming soon)
            </div>
          )}
        </div>
      </aside>
    </div>
  );
}
