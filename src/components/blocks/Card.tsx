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
      className={`rounded-2xl border border-zinc-200 bg-white p-5 shadow-sm ${className}`}
    >
      {children}
    </div>
  );
}
