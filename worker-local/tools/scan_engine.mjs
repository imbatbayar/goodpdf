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
 * Build ranges from per-page byte estimates. Truly size-aware: uses cumulative
 * sum cut points to balance part sizes by estimated output.
 */
export function buildRangesFromPageBytes({ pageBytes, parts }) {
  const arr = Array.isArray(pageBytes) ? pageBytes : [];
  const p = arr.length;
  if (p <= 0) return [{ start: 1, end: 1 }];
  const numParts = Math.max(1, Math.min(Number(parts) || 1, p));
  if (numParts === 1) return [{ start: 1, end: p }];

  const cum = [0];
  for (let i = 0; i < p; i++) cum.push(cum[i] + (arr[i] || 0));
  const total = cum[p];
  const ranges = [];
  let startIdx = 0;

  for (let k = 0; k < numParts; k++) {
    const remainingParts = numParts - k;
    const remainingPages = p - startIdx;
    const endIdx =
      remainingParts <= 1
        ? p - 1
        : (() => {
            const targetByte = ((k + 1) * total) / numParts;
            let e = startIdx;
            while (e < p - 1 && cum[e + 1] < targetByte) e++;
            return e;
          })();
    ranges.push({ start: startIdx + 1, end: endIdx + 1 });
    startIdx = endIdx + 1;
  }
  return ranges;
}

/**
 * Fallback when per-page bytes unavailable. Smooth positional weights +
 * chunk-progress balancing for better part-size uniformity.
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

  const avg = b / p;
  // Smooth sinusoidal variation: spreads weight across doc, no strong center/edge bias
  const estBytes = [];
  let sum = 0;
  for (let i = 0; i < p; i++) {
    const frac = (i + 0.5) / p;
    const w = 1 + 0.06 * Math.sin(2 * Math.PI * frac);
    estBytes.push(avg * w);
    sum += avg * w;
  }
  const scale = b / sum;
  for (let i = 0; i < p; i++) estBytes[i] *= scale;

  const ranges = [];
  let start = 0;
  let running = 0;

  for (let k = 0; k < parts; k++) {
    const remainingParts = parts - k;
    const remainingPages = p - start;
    if (remainingParts <= 1) {
      ranges.push({ start: start + 1, end: p });
      break;
    }
    const remainingBytes = b - running;
    const targetForThis = remainingBytes / remainingParts;

    let end = start;
    let chunk = 0;
    while (end < p - 1) {
      const nextChunk = chunk + estBytes[end];
      if (chunk > 0 && nextChunk >= targetForThis * 0.88) break;
      if (end - start >= remainingPages - (remainingParts - 1)) break;
      chunk = nextChunk;
      end++;
    }
    ranges.push({ start: start + 1, end: end + 1 });
    for (let i = start; i <= end; i++) running += estBytes[i];
    start = end + 1;
  }
  return ranges.length ? ranges : [{ start: 1, end: p }];
}
