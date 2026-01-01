export function Progress({ value }: { value: number }) {
  return (
    <div style={{ height: 10, borderRadius: 999, background: "rgba(15,23,42,.08)", overflow: "hidden" }}>
      <div style={{ height: "100%", width: `${Math.max(0, Math.min(100, value))}%`, background: "var(--primary)" }} />
    </div>
  );
}
