/**
 * pdfWorker — Deterministic PDF processing for GOODPDF
 *
 * Exports: processPdf(inputPath, outputDir) → { parts, partCount, avgPartSize, usedFallback }
 */

export {
  processPdf,
  ProcessPdfResult,
  type StrategyUsed,
} from "./processPdf";
export { probePageSizesViaSingles, buildRangesNearTarget } from "./pageProbe";
export {
  getSplitPolicy,
  getPartLimit,
  getPolicyFromInputBytes,
  estimatePagesPerPart,
  buildSplitRanges,
  getPolicySummary,
  TARGET_PART_BYTES,
  TARGET_PART_MB,
} from "./splitPolicy";
export type { SplitPolicy, PolicyFromInput } from "./splitPolicy";
export { compressBalanced, compressRescue, imagesToPdf } from "./ghostscript";
export { pdfToImages, rasterizePdf } from "./rasterFallback";
