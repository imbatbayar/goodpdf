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

/**
 * Build page ranges that aim to balance part sizes by estimated bytes-per-page.
 * When parts is provided, builds exactly that many ranges; otherwise derives from bytes/targetBytes.
 */
export function buildRangesByEstimatedSize({ bytes, pages, targetBytes, maxParts, parts: partsOverride }) {
  const b = Math.max(0, Number(bytes || 0));
  const p = Math.max(1, Number(pages || 1));
  const t = Math.max(256 * 1024, Number(targetBytes || 9 * 1024 * 1024));
  const m = Math.max(1, Number(maxParts || 5));
  if (p <= 1) return [{ start: 1, end: 1 }];
  const parts = partsOverride != null
    ? Math.min(partsOverride, p)
    : Math.min(m, p, Math.max(1, Math.ceil(b / (t * 0.95))));
  const avgBytesPerPage = b / p;
  const ranges = [];
  let start = 1;
  for (let i = 0; i < parts && start <= p; i++) {
    const isLast = i === parts - 1;
    const remainingPages = p - start + 1;
    const pagesInPart = isLast
      ? remainingPages
      : Math.max(1, Math.min(
          remainingPages - (parts - i - 1),
          Math.ceil((t * 0.95) / avgBytesPerPage),
        ));
    const end = Math.min(p, start + pagesInPart - 1);
    ranges.push({ start, end });
    start = end + 1;
  }
  return ranges.length ? ranges : [{ start: 1, end: p }];
}
