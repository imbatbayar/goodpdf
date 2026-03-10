/**
 * pdfWorker — Deterministic PDF processing for GOODPDF
 *
 * Exports: processPdf(inputPath, outputDir) → { parts, partCount, avgPartSize, usedFallback }
 */

export { processPdf, ProcessPdfResult } from "./processPdf";
export {
  getPartLimit,
  getPolicyFromInputBytes,
  estimatePagesPerPart,
  buildSplitRanges,
  getPolicySummary,
  TARGET_PART_BYTES,
  TARGET_PART_MB,
} from "./splitPolicy";
export type { PolicyFromInput } from "./splitPolicy";
export { compressBalanced, compressRescue, imagesToPdf } from "./ghostscript";
export { pdfToImages, rasterizePdf } from "./rasterFallback";
