"use client";

import * as React from "react";

type ButtonVariant = "primary" | "secondary" | "ghost";

type Props = React.ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: ButtonVariant;
};

/**
 * Presentational button (Tailwind-first).
 * Business logic must live outside.
 */
export const Button = React.forwardRef<HTMLButtonElement, Props>(function Button(
  { variant = "primary", className = "", type, ...props },
  ref
) {
  const base =
    "inline-flex items-center justify-center gap-2 rounded-2xl px-4 py-2.5 text-sm font-semibold " +
    "transition active:scale-[0.99] disabled:pointer-events-none disabled:opacity-60 " +
    "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--primary)] focus-visible:ring-offset-2";

  const variants: Record<ButtonVariant, string> = {
    primary:
      "bg-[var(--primary)] text-[var(--primary-ink)] border border-[var(--primary)] shadow-sm hover:brightness-[0.98]",
    secondary:
      "bg-[var(--card)] text-[var(--ink)] border border-[var(--border)] shadow-sm hover:brightness-[0.99]",
    ghost:
      "bg-transparent text-[var(--ink)] border border-transparent hover:bg-[var(--card)] hover:brightness-[0.99]",
  };

  return (
    <button
      ref={ref}
      type={type ?? "button"}
      className={`${base} ${variants[variant]} ${className}`}
      {...props}
    />
  );
});
