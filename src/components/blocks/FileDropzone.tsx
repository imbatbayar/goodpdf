"use client";

import { useRef, useState } from "react";
import { Button } from "@/components/ui/Button";

export function FileDropzone({ onPick }: { onPick: (f: File) => void }) {
  const ref = useRef<HTMLInputElement | null>(null);
  const [fileName, setFileName] = useState<string>("No file chosen");

  return (
    <div className="rounded-2xl bg-white p-6 shadow-sm ring-1 ring-zinc-200">
      <div className="flex items-start justify-between gap-4">
        <div className="min-w-0">
          <div className="text-sm font-semibold text-zinc-900">1 PDF</div>
          <div className="mt-1 text-xs text-zinc-600">One file per job.</div>
          <div className="mt-3 truncate text-sm text-zinc-700">{fileName}</div>
        </div>

        {/* Hidden file input */}
        <input
          ref={ref}
          type="file"
          accept="application/pdf"
          className="hidden"
          onChange={(e) => {
            const f = e.target.files?.[0];
            if (!f) return;
            setFileName(f.name);
            onPick(f);
          }}
        />

        {/* The button you want on home */}
        <Button
          onClick={() => ref.current?.click()}
          className="shrink-0"
        >
          Upload PDF
        </Button>
      </div>
    </div>
  );
}
