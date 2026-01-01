"use client";

import * as React from "react";

type Props = React.InputHTMLAttributes<HTMLInputElement>;

export function Input(props: Props) {
  return (
    <input
      {...props}
      style={{
        padding: "10px 12px",
        borderRadius: 12,
        border: "1px solid var(--border)",
        width: "100%",
        outline: "none",
        background: "var(--card)",
        ...props.style,
      }}
    />
  );
}
