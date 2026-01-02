"use client";

export function DoneConfirmModal(props: {
  open: boolean;
  onDone: () => void | Promise<void>;
  onClose: () => void;
}) {
  const { open, onDone, onClose } = props;
  if (!open) return null;

  return (
    <div
      style={{
        position: "fixed",
        inset: 0,
        zIndex: 60,
        background: "rgba(2,6,23,.45)",
        display: "grid",
        placeItems: "center",
        padding: 16,
      }}
    >
      <div
        style={{
          width: "min(480px, 92vw)",
          borderRadius: 18,
          background: "white",
          border: "1px solid rgba(15,23,42,.12)",
          boxShadow: "0 20px 60px rgba(0,0,0,.20)",
          padding: 16,
          display: "grid",
          gap: 12,
        }}
      >
        <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: 12 }}>
          <div>
            <div style={{ fontWeight: 900, fontSize: 14 }}>🎉 Download started</div>
            <div style={{ fontSize: 12, color: "rgba(15,23,42,.65)", marginTop: 4 }}>
              Your file is being downloaded.
            </div>
          </div>

          <button
            onClick={onClose}
            aria-label="Close"
            style={{
              border: "1px solid rgba(15,23,42,.12)",
              background: "white",
              borderRadius: 10,
              width: 34,
              height: 34,
              cursor: "pointer",
              fontWeight: 900,
              color: "rgba(15,23,42,.7)",
            }}
          >
            ✕
          </button>
        </div>

        <div
          style={{
            border: "1px solid rgba(15,23,42,.10)",
            background: "rgba(15,23,42,.03)",
            borderRadius: 14,
            padding: 12,
            fontSize: 12,
            color: "rgba(15,23,42,.75)",
            lineHeight: 1.45,
          }}
        >
          Do you want to delete all files from the server now?
          <br />
          <span style={{ color: "rgba(15,23,42,.55)" }}>
            If you leave without confirming, everything will still be deleted automatically after 10 minutes.
          </span>
        </div>

        <div style={{ display: "flex", gap: 10, justifyContent: "flex-end" }}>
          <button
            onClick={onClose}
            style={{
              padding: "10px 12px",
              borderRadius: 12,
              border: "1px solid rgba(15,23,42,.12)",
              background: "white",
              cursor: "pointer",
              fontWeight: 900,
              fontSize: 13,
            }}
          >
            Not now
          </button>

          <button
            onClick={onDone}
            style={{
              padding: "10px 14px",
              borderRadius: 12,
              border: "1px solid rgba(15,23,42,.12)",
              background: "rgba(31,122,74,1)",
              color: "white",
              cursor: "pointer",
              fontWeight: 900,
              fontSize: 13,
            }}
          >
            Done
          </button>
        </div>
      </div>
    </div>
  );
}
