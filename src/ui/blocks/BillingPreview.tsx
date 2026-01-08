"use client";

import type { Country } from "@/ui/blocks/CountrySelect";

export function BillingPreview({ country }: { country: Country }) {
  const isMN = country === "Mongolia";

  return (
    <div
      style={{
        border: "1px solid var(--border)",
        borderRadius: 14,
        padding: 12,
        background: "var(--card)",
        display: "grid",
        gap: 10,
      }}
    >
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 10 }}>
        <div style={{ fontWeight: 900 }}>
          {isMN ? "QPay" : "Card Payment"}
        </div>
        <span
          style={{
            fontSize: 11,
            fontWeight: 900,
            padding: "4px 8px",
            borderRadius: 999,
            border: "1px solid var(--border)",
            opacity: 0.8,
          }}
        >
          TEST MODE
        </span>
      </div>

      {/* Payment method preview */}
      {isMN ? (
        <div style={{ display: "grid", gap: 8 }}>
          <div
            style={{
              border: "1px dashed var(--border)",
              borderRadius: 12,
              padding: 12,
              textAlign: "center",
              fontWeight: 900,
              opacity: 0.9,
            }}
          >
            QR (QPay) — coming soon
          </div>

          <div style={{ fontSize: 12, opacity: 0.9, lineHeight: 1.35 }}>
            <b>Анхааруулга (Privacy):</b>
            <br />
            Та ZIP-ээ татаж авсны дараа <b>Confirm</b> товч дармагц,
            <b> 10 минутын дараа</b> манай cloud серверээс бүрэн устгана.
            <br />
            Бид файлыг хадгалдаггүй. Устсаны дараа <b>сэргээх боломжгүй</b>.
          </div>
        </div>
      ) : (
        <div style={{ display: "grid", gap: 8 }}>
          <div
            style={{
              border: "1px dashed var(--border)",
              borderRadius: 12,
              padding: 12,
              textAlign: "center",
              fontWeight: 900,
              opacity: 0.9,
            }}
          >
            Visa / MasterCard — coming soon
          </div>

          <div style={{ fontSize: 12, opacity: 0.9, lineHeight: 1.35 }}>
            <b>Privacy notice:</b>
            <br />
            After you download, when you press <b>Confirm</b>, your files are
            permanently deleted from our cloud servers <b>in 10 minutes</b>.
            <br />
            We do not store your files. After deletion, <b>recovery is not possible</b>.
          </div>
        </div>
      )}
    </div>
  );
}
