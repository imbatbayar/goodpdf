"use client";

import * as React from "react";

type Props = React.ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: "primary" | "ghost" | "secondary";
};

export function Button({ variant = "primary", style, ...props }: Props) {
  const base: React.CSSProperties = {
    padding: "10px 14px",
    borderRadius: 12,
    border: "1px solid var(--border)",
    cursor: props.disabled ? "not-allowed" : "pointer",
    fontWeight: 700,
    transition: "transform .02s ease, opacity .12s ease",
    opacity: props.disabled ? 0.55 : 1,
  };

  const v: Record<string, React.CSSProperties> = {
    primary: { background: "var(--primary)", color: "var(--primary-ink)", borderColor: "var(--primary)" },
    secondary: { background: "var(--card)", color: "var(--ink)" },
    ghost: { background: "transparent", color: "var(--ink)" },
  };

  return (
    <button
      {...props}
      style={{ ...base, ...v[variant], ...style }}
      onMouseDown={(e) => (e.currentTarget.style.transform = "scale(0.99)")}
      onMouseUp={(e) => (e.currentTarget.style.transform = "scale(1)")}
    />
  );
}
