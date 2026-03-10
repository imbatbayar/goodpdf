/**
 * splitPolicy.ts — Deterministic PDF split policy for GOODPDF
 *
 * Policy is ALWAYS based on ORIGINAL INPUT PDF size:
 * - Input < 200MB → goal ≤ 5 parts
 * - Input 200–500MB → goal ≤ 10 parts
 * - Target part size ≈ 9MB
 */

export const TARGET_PART_BYTES = 9 * 1024 * 1024; // 9MB
export const TARGET_PART_MB = 9;

export interface SplitPolicy {
  policyMaxParts: number;
  targetPartBytes: number;
  policyLabel: string;
}

export interface PolicyFromInput {
  maxParts: number;
  targetPartBytes: number;
  policyLabel: string;
}

/**
 * Single-source policy from ORIGINAL INPUT bytes.
 * Validates input; if invalid, falls back safely to 5 parts and 9MB target.
 */
export function getSplitPolicy(originalInputBytes: number): SplitPolicy {
  const targetPartBytes = TARGET_PART_BYTES;
  const invalid =
    !Number.isFinite(originalInputBytes) ||
    originalInputBytes < 0 ||
    originalInputBytes !== Math.floor(originalInputBytes);
  if (invalid) {
    return {
      policyMaxParts: 5,
      targetPartBytes,
      policyLabel: "under_200mb",
    };
  }
  const fileSizeMB = originalInputBytes / (1024 * 1024);
  if (fileSizeMB < 200) {
    return { policyMaxParts: 5, targetPartBytes, policyLabel: "under_200mb" };
  }
  return { policyMaxParts: 10, targetPartBytes, policyLabel: "200mb_to_500mb" };
}

/** @deprecated Use getSplitPolicy */
export function getPolicyFromInputBytes(originalInputBytes: number): PolicyFromInput {
  const p = getSplitPolicy(originalInputBytes);
  return {
    maxParts: p.policyMaxParts,
    targetPartBytes: p.targetPartBytes,
    policyLabel: p.policyLabel,
  };
}

/**
 * Part limit based on input file size (MB).
 * Policy: input < 200MB => maxParts = 5; input 200–500MB => maxParts = 10.
 */
export function getPartLimit(fileSizeBytes: number): number {
  return getSplitPolicy(fileSizeBytes).policyMaxParts;
}

/** Estimate pages per part using average bytes per page */
export function estimatePagesPerPart(
  fileSizeBytes: number,
  pageCount: number,
  targetPartBytes: number = TARGET_PART_BYTES
): number {
  if (pageCount <= 0) return 1;
  const safeTarget =
    Number.isFinite(targetPartBytes) && targetPartBytes > 0
      ? targetPartBytes
      : TARGET_PART_BYTES;
  const avgBytesPerPage = fileSizeBytes / pageCount;
  const pagesPerPart = Math.floor(safeTarget / avgBytesPerPage);
  return Math.max(1, pagesPerPart);
}

/** Build page ranges for splitting: each range targets ~9MB */
export function buildSplitRanges(
  pageCount: number,
  pageCountPerPart: number,
  partLimit: number
): Array<{ start: number; end: number }> {
  const ranges: Array<{ start: number; end: number }> = [];
  const totalParts = Math.min(
    partLimit,
    pageCount,
    Math.max(1, Math.ceil(pageCount / pageCountPerPart))
  );

  let start = 1;
  for (let i = 0; i < totalParts; i++) {
    const remainingParts = totalParts - i;
    const remainingPages = pageCount - start + 1;
    const pagesForThisPart = Math.ceil(remainingPages / remainingParts);
    const end = Math.min(pageCount, start + pagesForThisPart - 1);
    ranges.push({ start, end });
    start = end + 1;
    if (start > pageCount) break;
  }
  return ranges.length ? ranges : [{ start: 1, end: pageCount }];
}

/** Get policy summary for logging */
export function getPolicySummary(
  fileSizeBytes: number,
  pageCount: number
): {
  partLimit: number;
  targetPartBytes: number;
  pagesPerPart: number;
  ranges: Array<{ start: number; end: number }>;
} {
  const policy = getSplitPolicy(fileSizeBytes);
  const pagesPerPart = estimatePagesPerPart(fileSizeBytes, pageCount);
  const ranges = buildSplitRanges(pageCount, pagesPerPart, policy.policyMaxParts);
  return {
    partLimit: policy.policyMaxParts,
    targetPartBytes: policy.targetPartBytes,
    pagesPerPart,
    ranges,
  };
}
