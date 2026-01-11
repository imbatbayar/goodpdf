"use client";

import { useEffect, useMemo, useRef } from "react";

type Item = {
  label: string;
  href?: string;
  onClick?: () => void;
  disabled?: boolean;
};

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
  const panelRef = useRef<HTMLElement | null>(null);

  // ESC + Outside click/touch to close (CAPTURE => reliable)
  useEffect(() => {
    if (!open) return;

    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };

    const onOutside = (e: MouseEvent | TouchEvent) => {
      const target = e.target as Node | null;
      const panel = panelRef.current;
      if (!panel || !target) return;

      // If click/touch is outside panel => close
      if (!panel.contains(target)) onClose();
    };

    window.addEventListener("keydown", onKey);
    document.addEventListener("mousedown", onOutside, true); // capture
    document.addEventListener("touchstart", onOutside, true); // capture

    return () => {
      window.removeEventListener("keydown", onKey);
      document.removeEventListener("mousedown", onOutside, true);
      document.removeEventListener("touchstart", onOutside, true);
    };
  }, [open, onClose]);

  // Active highlight (client-only)
  const activePath = useMemo(() => {
    if (typeof window === "undefined") return "";
    return window.location.pathname || "";
  }, []);

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-60" role="dialog" aria-modal="true">
      {/* overlay (visual only; outside-close handled by document listener) */}
      <div className="absolute inset-0 bg-black/30 backdrop-blur-[2px]" />

      {/* panel */}
      <aside
        ref={(el) => {
          panelRef.current = el;
        }}
        className={[
          "absolute right-3 top-3 h-[calc(100vh-24px)] w-[320px] max-w-[calc(100vw-24px)]",
          "rounded-3xl bg-white shadow-[0_18px_55px_rgba(0,0,0,0.18)]",
          "border border-zinc-200/70 overflow-hidden",
          "flex flex-col",
        ].join(" ")}
      >
        {/* header */}
        <div className="flex items-center justify-between px-5 py-4">
          <div className="text-sm font-semibold text-zinc-900">{title}</div>
          <button
            onClick={onClose}
            className="rounded-xl border border-zinc-200 bg-white px-3 py-2 text-xs font-semibold text-zinc-800 hover:bg-zinc-50"
            aria-label="Close"
          >
            ✕
          </button>
        </div>

        <div className="px-3">
          <div className="h-px bg-zinc-200/70" />
        </div>

        {/* items (scroll area) */}
        <nav className="flex-1 overflow-auto px-3 py-3">
          <div className="grid gap-1">
            {items.map((it) => {
              const isActive = !!it.href && it.href === activePath;
              const disabled = !!it.disabled;

              const base =
                "flex items-center justify-between rounded-2xl px-4 py-3 text-sm font-medium transition";
              const state = disabled
                ? "text-zinc-400 cursor-not-allowed"
                : isActive
                  ? "bg-zinc-100 text-zinc-900"
                  : "text-zinc-700 hover:bg-zinc-50 hover:text-zinc-900";

              const rightMark = disabled ? "Soon" : "›";

              if (it.href && !disabled) {
                return (
                  <a key={it.label} href={it.href} className={`${base} ${state}`}>
                    <span>{it.label}</span>
                    <span className="text-xs text-zinc-400">{rightMark}</span>
                  </a>
                );
              }

              return (
                <button
                  key={it.label}
                  type="button"
                  className={`${base} ${state} text-left`}
                  onClick={() => {
                    if (disabled) return;
                    it.onClick?.();
                    onClose();
                  }}
                >
                  <span>{it.label}</span>
                  <span className="text-xs text-zinc-400">{rightMark}</span>
                </button>
              );
            })}
          </div>
        </nav>

        {/* footer (always bottom) */}
        <div className="mt-auto px-5 pb-5">
          {footer ? (
            footer
          ) : (
            <div className="rounded-2xl bg-zinc-50 px-4 py-3 text-xs text-zinc-500">
              Privacy-first. Files auto-delete after 10 minutes.
            </div>
          )}
        </div>
      </aside>
    </div>
  );
}
