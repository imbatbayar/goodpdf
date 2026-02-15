"use client";

import { useEffect } from "react";

export function LongProcessingModal({
  open,
  text,
  onClose,
}: {
  open: boolean;
  text: string;
  onClose: () => void;
}) {
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
    <div className="fixed inset-0 z-50">
      {/* Overlay (click to close) */}
      <button
        aria-label="Close"
        className="absolute inset-0 bg-black/20"
        onClick={onClose}
      />

      {/* Dialog */}
      <div className="relative mx-auto mt-24 w-[92vw] max-w-md rounded-2xl border border-zinc-200 bg-white p-5 shadow-xl">
        <div className="flex items-start justify-between gap-3">
          <div>
            <div className="text-sm font-semibold text-zinc-900">Still working…</div>
            <div className="mt-1 text-xs text-zinc-600">{text}</div>
          </div>

          <button
            className="rounded-lg px-2 py-1 text-xs font-semibold text-zinc-600 hover:bg-zinc-50"
            onClick={onClose}
          >
            Close
          </button>
        </div>
      </div>
    </div>
  );
}
