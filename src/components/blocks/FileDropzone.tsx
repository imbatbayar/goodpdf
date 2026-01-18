"use client";

import { useRef, useState } from "react";

export function FileDropzone({ onPick }: { onPick: (f: File) => void }) {
  const inputRef = useRef<HTMLInputElement | null>(null);
  const [isHover, setIsHover] = useState(false);

  const acceptPdf = (f: File) => {
    // quiet guard: pdf mime OR .pdf extension
    return f.type === "application/pdf" || f.name.toLowerCase().endsWith(".pdf");
  };

  const openPicker = () => {
    // allow re-selecting the same file reliably
    if (inputRef.current) inputRef.current.value = "";
    inputRef.current?.click();
  };

  const pick = (f: File) => {
    if (!acceptPdf(f)) return;

    // keep input clean so selecting the same file again works reliably
    if (inputRef.current) inputRef.current.value = "";

    onPick(f);
  };

  return (
    <div className="rounded-2xl bg-white p-6 shadow-sm ring-1 ring-zinc-200">
      {/* Touch / click dropzone (NO extra button) */}
      <div
        className={[
          "relative grid place-items-center rounded-2xl border-2 border-dashed p-6 text-center transition",
          "cursor-pointer select-none touch-manipulation",
          isHover
            ? "border-(--primary) bg-[rgba(31,122,74,.06)]"
            : "border-zinc-200 bg-zinc-50",
        ].join(" ")}
        onMouseDown={(e) => e.preventDefault()} // prevent caret / text selection
        onClick={openPicker}
        onDragOver={(e) => {
          e.preventDefault();
          e.stopPropagation();
          setIsHover(true);
        }}
        onDragEnter={(e) => {
          e.preventDefault();
          e.stopPropagation();
          setIsHover(true);
        }}
        onDragLeave={(e) => {
          e.preventDefault();
          e.stopPropagation();
          setIsHover(false);
        }}
        onDrop={(e) => {
          e.preventDefault();
          e.stopPropagation();
          setIsHover(false);
          const f = e.dataTransfer.files?.[0];
          if (!f) return;
          pick(f);
        }}
        role="button"
        tabIndex={0}
        aria-label="Upload PDF"
        onKeyDown={(e) => {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault(); // avoid page scroll on Space
            openPicker();
          }
        }}
      >
        {/* icon */}
        <div className="mb-3 grid h-11 w-11 place-items-center rounded-2xl bg-white shadow-sm ring-1 ring-zinc-200">
          <span className="text-xl leading-none">＋</span>
        </div>

        <div className="text-sm font-semibold text-zinc-900">
          Drag &amp; drop a PDF here
        </div>

        <div className="mt-1 text-xs text-zinc-600">
          or{" "}
          <span className="font-semibold text-(--primary)">click to browse</span>
        </div>

        <div className="mt-3 text-[11px] tracking-wide text-zinc-500">
          PDF only • 1 file per job
        </div>

        {/* hidden input */}
        <input
          ref={inputRef}
          type="file"
          accept="application/pdf"
          className="hidden"
          onChange={(e) => {
            const f = e.target.files?.[0];
            if (!f) return;
            pick(f);
          }}
        />
      </div>
    </div>
  );
}
