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
  TARGET_PART_BYTES,
  getPartLimit,
  buildSplitRanges,
  estimatePagesPerPart,
} from "./splitPolicy";
import { pdfToImages } from "./rasterFallback";

const QPDF_EXE = process.env.QPDF_EXE || "qpdf";
const DEFAULT_TIMEOUT_MS = 120_000;

/** Single source for part-size budget. Never allow missing/invalid targetBytes. */
const SYSTEM_PART_BYTES = TARGET_PART_BYTES;

export interface ProcessPdfResult {
  parts: string[];
  partCount: number;
  avgPartSize: number;
  usedFallback: boolean;
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
  const rawTarget = options?.targetBytes ?? SYSTEM_PART_BYTES;
  const partTargetBytes =
    Number.isFinite(rawTarget) && rawTarget > 0
      ? Math.floor(rawTarget)
      : SYSTEM_PART_BYTES;

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

    const fileSizeBytes = safeStatSize(compressedPath) ?? 0;
    const pageCount = await qpdfPages(compressedPath, timeoutMs);

    // 3. Determine policy: <200MB => 5 parts, 200–500MB => 10 parts
    const partLimit = getPartLimit(fileSizeBytes);
    const pagesPerPart = estimatePagesPerPart(
      fileSizeBytes,
      pageCount,
      partTargetBytes
    );
    const ranges = buildSplitRanges(pageCount, pagesPerPart, partLimit);

    // 4. Split
    let partPaths = await splitByRanges(
      compressedPath,
      partsDir,
      ranges,
      timeoutMs
    );

    // 5. Oversize rescue: recompress parts > partTargetBytes
    for (let i = 0; i < partPaths.length; i++) {
      const p = partPaths[i];
      const sz = safeStatSize(p) ?? 0;
      if (sz > partTargetBytes) {
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
    const partSizes = partPaths.map((p) => safeStatSize(p) ?? 0);
    const maxPartSize = Math.max(...partSizes, 0);
    const partCount = partPaths.length;
    let usedFallback = false;

    // 6. Raster fallback: if part count > limit or any part still > partTargetBytes
    if (partCount > partLimit || maxPartSize > partTargetBytes) {
      usedFallback = true;
      const rasterPath = path.join(workDir, "rasterized.pdf");
      tempFiles.push(rasterPath);

      const imagePaths = await pdfToImages(
        compressedPath,
        path.join(workDir, ".__raster_imgs"),
        "page",
        60,
        timeoutMs
      );
      await imagesToPdf(imagePaths, rasterPath, timeoutMs);
      safeRm(path.join(workDir, ".__raster_imgs"));

      const rasterBytes = safeStatSize(rasterPath) ?? 0;
      const rasterPages = await qpdfPages(rasterPath, timeoutMs);
      const newPagesPerPart = estimatePagesPerPart(
        rasterBytes,
        rasterPages,
        partTargetBytes
      );
      const newRanges = buildSplitRanges(
        rasterPages,
        newPagesPerPart,
        partLimit
      );

      // Clear old parts and split rasterized PDF
      for (const p of partPaths) safeUnlink(p);
      partPaths = await splitByRanges(rasterPath, partsDir, newRanges, timeoutMs);
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

    return {
      parts: finalParts,
      partCount: finalParts.length,
      avgPartSize,
      usedFallback,
    };
  } finally {
    for (const f of tempFiles) safeUnlink(f);
    safeRm(workDir);
  }
}
