
"use client";

import { useId, useState } from "react";

type Props = {
  content: React.ReactNode;
  children: React.ReactNode;
};

// Minimal, dependency-free tooltip (hover/focus)
export function Tooltip({ content, children }: Props) {
  const id = useId();
  const [open, setOpen] = useState(false);

  return (
    <span
      style={{ position: "relative", display: "inline-flex" }}
      onMouseEnter={() => setOpen(true)}
      onMouseLeave={() => setOpen(false)}
      onFocus={() => setOpen(true)}
      onBlur={() => setOpen(false)}
    >
      <span aria-describedby={id}>{children}</span>
      {open ? (
        <span
          id={id}
          role="tooltip"
          style={{
            position: "absolute",
            left: "50%",
            bottom: "calc(100% + 8px)",
            transform: "translateX(-50%)",
            zIndex: 50,
            padding: "8px 10px",
            borderRadius: 10,
            border: "1px solid var(--border)",
            background: "var(--card)",
            color: "var(--fg)",
            fontSize: 12,
            lineHeight: 1.25,
            whiteSpace: "nowrap",
            boxShadow: "0 10px 30px rgba(15,23,42,.10)",
          }}
        >
          {content}
        </span>
      ) : null}
    </span>
  );
}
