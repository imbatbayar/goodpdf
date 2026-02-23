// worker-local/tools/scan_engine.mjs
// Fast scan-oriented profile selector for GOODPDF.

export function classifyScanJob({ bytes, pages }) {
  const b = Math.max(0, Number(bytes || 0));
  const p = Math.max(1, Number(pages || 1));
  const bpp = b / p;

  const isHuge = b >= 120 * 1024 * 1024;
  const isVeryHuge = b >= 220 * 1024 * 1024;
  const isScanLike = bpp >= 650 * 1024;

  if (isVeryHuge || (isHuge && isScanLike)) {
    return {
      className: "SCAN_HARD",
      pass1: { dpi: 68, jpegQ: 28, pdfSettings: "/screen" },
      pass2: { dpi: 62, jpegQ: 24, pdfSettings: "/screen" },
      allowSecondPass: true,
    };
  }

  if (isHuge || isScanLike) {
    return {
      className: "SCAN_FAST",
      pass1: { dpi: 76, jpegQ: 32, pdfSettings: "/screen" },
      pass2: { dpi: 68, jpegQ: 28, pdfSettings: "/screen" },
      allowSecondPass: true,
    };
  }

  return {
    className: "NORMAL_FAST",
    pass1: { dpi: 90, jpegQ: 40, pdfSettings: "/screen" },
    pass2: { dpi: 80, jpegQ: 34, pdfSettings: "/screen" },
    allowSecondPass: true,
  };
}

export function estimatePartsForTarget({ bytes, targetBytes, maxParts, pages }) {
  const b = Math.max(0, Number(bytes || 0));
  const t = Math.max(256 * 1024, Number(targetBytes || 9 * 1024 * 1024));
  const m = Math.max(1, Number(maxParts || 5));
  const p = Math.max(1, Number(pages || 1));
  const fill = 0.95;
  return Math.max(1, Math.min(m, p, Math.ceil(b / Math.floor(t * fill))));
}
