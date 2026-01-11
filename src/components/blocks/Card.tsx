import * as React from "react";

type Props = {
  children: React.ReactNode;
  className?: string;
};

/**
 * Shared Card primitive.
 * Keep it purely presentational.
 */
export function Card({ children, className = "" }: Props) {
  return (
    <div
      className={`rounded-2xl border border-[var(--border)] bg-[var(--card)] p-6 shadow-sm ${className}`}
    >
      {children}
    </div>
  );
}
