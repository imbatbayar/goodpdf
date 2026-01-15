"use client";

import type { ReactNode } from "react";

export function ScreenShell({
  title,
  subtitle,
  children,
}: {
  title: string;
  subtitle?: string;
  children: ReactNode;
}) {
  return (
    <div className="mx-auto w-full max-w-245 px-3 sm:px-4">
      {/* header */}
      <div className="grid gap-2 pt-6 text-center">
        <h1 className="text-[28px] font-semibold tracking-tight text-zinc-900">
          {title}
        </h1>
        {subtitle ? (
          <p className="text-[15px] leading-6 text-zinc-500">{subtitle}</p>
        ) : null}
      </div>

      {/* content */}
      <div className="mt-6">{children}</div>
    </div>
  );
}
