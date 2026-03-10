/**
 * processPdf.ts — Deterministic PDF processing pipeline for GOODPDF
 *
 * Pipeline: Normalize → Compress → Split → Oversize Rescue → Raster Fallback
 *
 * Dependencies: qpdf, ghostscript, pdftoppm (poppler)
 */

import { spawn } from "child_process";
import * as fs from "fs";
import * as path from "path";
import { compressBalanced, compressRescue, imagesToPdf } from "./ghostscript";
import {
  getSplitPolicy,
  buildSplitRanges,
  estimatePagesPerPart,
} from "./splitPolicy";
import {
  probePageSizesViaSingles,
  buildRangesNearTarget,
} from "./pageProbe";
import { pdfToImages } from "./rasterFallback";

const QPDF_EXE = process.env.QPDF_EXE || "qpdf";
const DEFAULT_TIMEOUT_MS = 120_000;

export type StrategyUsed =
  | "balanced"
  | "rescue"
  | "raster_q60"
  | "raster_q45"
  | "raster_q35";

export interface ProcessPdfResult {
  parts: string[];
  partCount: number;
  avgPartSize: number;
  usedFallback: boolean;
  policyMaxParts: number;
  finalPartCount: number;
  maxPartBytes: number;
  fitStatus: "fit" | "best_effort";
  strategyUsed: StrategyUsed;
}

function runCmd(
  cmd: string,
  args: string[],
  timeoutMs: number,
  okCodes: number[] = [0]
): Promise<{ exitCode: number; stdout: string; stderr: string }> {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, {
      stdio: ["ignore", "pipe", "pipe"],
    });

    let out = "";
    let err = "";
    child.stdout?.on("data", (d) => (out += d.toString()));
    child.stderr?.on("data", (d) => (err += d.toString()));

    const to = setTimeout(() => {
      try {
        child.kill("SIGKILL");
      } catch {}
      reject(new Error(`${cmd} timeout after ${timeoutMs}ms`));
    }, timeoutMs);

    child.on("close", (code) => {
      clearTimeout(to);
      if (code !== null && !okCodes.includes(code)) {
        reject(new Error(`${cmd} failed code=${code}\n${err || out}`));
        return;
      }
      resolve({ exitCode: code ?? -1, stdout: out, stderr: err });
    });
    child.on("error", reject);
  });
}

function safeStatSize(p: string): number | null {
  try {
    if (!fs.existsSync(p)) return null;
    const st = fs.statSync(p);
    return Number.isFinite(st?.size) ? st.size : null;
  } catch {
    return null;
  }
}

function safeUnlink(p: string): void {
  try {
    if (fs.existsSync(p)) fs.unlinkSync(p);
  } catch {}
}

function safeRm(p: string): void {
  try {
    fs.rmSync(p, { recursive: true, force: true });
  } catch {}
}

/** Get page count via qpdf --show-npages */
async function qpdfPages(inPdf: string, timeoutMs: number): Promise<number> {
  const { stdout } = await runCmd(
    QPDF_EXE,
    ["--show-npages", inPdf],
    Math.min(timeoutMs, 25_000),
    [0, 3]
  );
  const pages = Number(String(stdout).trim());
  if (!Number.isFinite(pages) || pages <= 0) {
    throw new Error("Failed to get page count");
  }
  return pages;
}

/** Normalize PDF: qpdf --linearize */
async function normalizePdf(
  inputPath: string,
  outputPath: string,
  timeoutMs: number = DEFAULT_TIMEOUT_MS
): Promise<void> {
  await runCmd(QPDF_EXE, ["--linearize", inputPath, outputPath], timeoutMs);
}

/** Split PDF by page ranges using qpdf */
async function splitByRanges(
  inPdf: string,
  partsDir: string,
  ranges: Array<{ start: number; end: number }>,
  timeoutMs: number = DEFAULT_TIMEOUT_MS
): Promise<string[]> {
  fs.mkdirSync(partsDir, { recursive: true });
  const partPaths: string[] = [];
  const tEach = Math.min(timeoutMs / Math.max(1, ranges.length), 60_000);

  for (let i = 0; i < ranges.length; i++) {
    const r = ranges[i];
    const outPath = path.join(partsDir, `part_${i + 1}.pdf`);
    await runCmd(
      QPDF_EXE,
      ["--empty", "--pages", inPdf, `${r.start}-${r.end}`, "--", outPath],
      tEach
    );
    partPaths.push(outPath);
  }
  return partPaths;
}

/**
 * Main pipeline: processPdf(inputPath, outputDir)
 */
export async function processPdf(
  inputPath: string,
  outputDir: string,
  options?: { timeoutMs?: number; targetBytes?: number }
): Promise<ProcessPdfResult> {
  const timeoutMs = options?.timeoutMs ?? DEFAULT_TIMEOUT_MS;

  // Policy from ORIGINAL INPUT only — never from compressed size
  const originalInputBytes = safeStatSize(inputPath) ?? 0;
  const policy = getSplitPolicy(originalInputBytes);
  const targetPartBytes = policy.targetPartBytes;
  const policyMaxParts = policy.policyMaxParts;

  const workDir = path.join(outputDir, ".__process_work");
  const partsDir = outputDir;
  const tempFiles: string[] = [];

  try {
    fs.mkdirSync(workDir, { recursive: true });
    fs.mkdirSync(outputDir, { recursive: true });

    const normalizedPath = path.join(workDir, "normalized.pdf");
    const compressedPath = path.join(workDir, "compressed.pdf");
    tempFiles.push(normalizedPath, compressedPath);

    // 1. Normalize
    await normalizePdf(inputPath, normalizedPath, timeoutMs);

    // 2. Balanced compress
    await compressBalanced({
      inPdf: normalizedPath,
      outPdf: compressedPath,
      pdfSettings: "/ebook",
      colorImageResolution: 150,
      grayImageResolution: 150,
      monoImageResolution: 300,
      timeoutMs,
    });

    const compressedBytes = safeStatSize(compressedPath) ?? 0;
    const pageCount = await qpdfPages(compressedPath, timeoutMs);

    // 3. Byte-aware split planning: probe page sizes, then build ranges
    let ranges: Array<{ start: number; end: number }>;
    const probeDir = path.join(workDir, ".__probe");
    let pageBytes: number[] | null = null;
    try {
      pageBytes = await probePageSizesViaSingles(
        compressedPath,
        pageCount,
        probeDir,
        Math.min(120_000, timeoutMs)
      );
    } catch {
      pageBytes = null;
    }

    if (pageBytes && pageBytes.length === pageCount) {
      ranges = buildRangesNearTarget(
        pageBytes,
        targetPartBytes,
        policyMaxParts
      );
    } else {
      const pagesPerPart = estimatePagesPerPart(
        compressedBytes,
        pageCount,
        targetPartBytes
      );
      ranges = buildSplitRanges(pageCount, pagesPerPart, policyMaxParts);
    }

    // 4. Split
    let partPaths = await splitByRanges(
      compressedPath,
      partsDir,
      ranges,
      timeoutMs
    );

    // 5. Oversize rescue: recompress parts > targetPartBytes
    let strategyUsed: StrategyUsed = "balanced";
    for (let i = 0; i < partPaths.length; i++) {
      const p = partPaths[i];
      const sz = safeStatSize(p) ?? 0;
      if (sz > targetPartBytes) {
        strategyUsed = "rescue";
        const rescuedPath = path.join(workDir, `rescued_${i + 1}.pdf`);
        tempFiles.push(rescuedPath);
        await compressRescue({
          inPdf: p,
          outPdf: rescuedPath,
          pdfSettings: "/screen",
          colorImageResolution: 110,
          grayImageResolution: 110,
          timeoutMs,
        });
        fs.copyFileSync(rescuedPath, p);
      }
    }

    // Check if we're within policy
    let partSizes = partPaths.map((p) => safeStatSize(p) ?? 0);
    let maxPartSize = Math.max(...partSizes, 0);
    let partCount = partPaths.length;
    let usedFallback = false;

    // 6. Raster fallback ladder: q60 -> q45 -> q35
    const rasterQualities: Array<{ q: number; label: StrategyUsed }> = [
      { q: 60, label: "raster_q60" },
      { q: 45, label: "raster_q45" },
      { q: 35, label: "raster_q35" },
    ];

    for (const { q, label } of rasterQualities) {
      if (partCount <= policyMaxParts && maxPartSize <= targetPartBytes) break;

      usedFallback = true;
      strategyUsed = label;
      const rasterPath = path.join(workDir, `rasterized_q${q}.pdf`);
      tempFiles.push(rasterPath);
      const imgDir = path.join(workDir, `.__raster_imgs_q${q}`);

      try {
        safeRm(imgDir);
        fs.mkdirSync(imgDir, { recursive: true });
        const imagePaths = await pdfToImages(
          compressedPath,
          imgDir,
          "page",
          q,
          timeoutMs
        );
        await imagesToPdf(imagePaths, rasterPath, timeoutMs);
      } finally {
        safeRm(imgDir);
      }

      const rasterBytes = safeStatSize(rasterPath) ?? 0;
      const rasterPages = await qpdfPages(rasterPath, timeoutMs);

      let rasterRanges: Array<{ start: number; end: number }>;
      const rasterProbeDir = path.join(workDir, `.__probe_raster_q${q}`);
      let rasterPageBytes: number[] | null = null;
      try {
        rasterPageBytes = await probePageSizesViaSingles(
          rasterPath,
          rasterPages,
          rasterProbeDir,
          Math.min(90_000, timeoutMs)
        );
      } catch {
        rasterPageBytes = null;
      }
      safeRm(rasterProbeDir);

      if (rasterPageBytes && rasterPageBytes.length === rasterPages) {
        rasterRanges = buildRangesNearTarget(
          rasterPageBytes,
          targetPartBytes,
          policyMaxParts
        );
      } else {
        const newPagesPerPart = estimatePagesPerPart(
          rasterBytes,
          rasterPages,
          targetPartBytes
        );
        rasterRanges = buildSplitRanges(
          rasterPages,
          newPagesPerPart,
          policyMaxParts
        );
      }

      for (const p of partPaths) safeUnlink(p);
      partPaths = await splitByRanges(
        rasterPath,
        partsDir,
        rasterRanges,
        timeoutMs
      );

      for (let i = 0; i < partPaths.length; i++) {
        const p = partPaths[i];
        const sz = safeStatSize(p) ?? 0;
        if (sz > targetPartBytes) {
          const rescuedPath = path.join(
            workDir,
            `raster_rescued_q${q}_${i + 1}.pdf`
          );
          tempFiles.push(rescuedPath);
          await compressRescue({
            inPdf: p,
            outPdf: rescuedPath,
            pdfSettings: "/screen",
            colorImageResolution: 110,
            grayImageResolution: 110,
            timeoutMs,
          });
          fs.copyFileSync(rescuedPath, p);
        }
      }

      partSizes = partPaths.map((p) => safeStatSize(p) ?? 0);
      maxPartSize = Math.max(...partSizes, 0);
      partCount = partPaths.length;
    }

    // Rename to final names: goodPDF-N(1).pdf, goodPDF-N(2).pdf, ...
    const finalParts: string[] = [];
    const totalParts = partPaths.length;
    for (let i = 0; i < partPaths.length; i++) {
      const src = partPaths[i];
      const finalName = `goodPDF-${totalParts}(${i + 1}).pdf`;
      const dest = path.join(outputDir, finalName);
      if (src !== dest) {
        fs.renameSync(src, dest);
      }
      finalParts.push(dest);
    }

    const sizes = finalParts.map((p) => safeStatSize(p) ?? 0);
    const totalBytes = sizes.reduce((a, b) => a + b, 0);
    const avgPartSize = sizes.length > 0 ? totalBytes / sizes.length : 0;
    const maxPartBytes = sizes.length > 0 ? Math.max(...sizes) : 0;
    const finalPartCount = finalParts.length;

    const partCountOk = finalPartCount <= policyMaxParts;
    const maxSizeOk = maxPartBytes <= targetPartBytes;
    const fitsReasonably =
      maxPartBytes <= targetPartBytes * 1.1;
    const fitStatus =
      partCountOk && (maxSizeOk || fitsReasonably) ? "fit" : "best_effort";

    return {
      parts: finalParts,
      partCount: finalPartCount,
      avgPartSize,
      usedFallback,
      policyMaxParts,
      finalPartCount,
      maxPartBytes,
      fitStatus,
      strategyUsed,
    };
  } finally {
    for (const f of tempFiles) safeUnlink(f);
    safeRm(workDir);
  }
}
