/**
 * rasterFallback.ts — Raster fallback for GOODPDF
 *
 * When PDF splitting + rescue still exceeds policy limits, render pages to
 * images and rebuild a lightweight PDF, then split again.
 *
 * Uses: pdftoppm (poppler) for PDF → images, Ghostscript for images → PDF.
 */

import { spawn } from "child_process";
import * as fs from "fs";
import * as path from "path";
import { imagesToPdf } from "./ghostscript";

const PDFTOPPM_EXE = process.env.PDFTOPPM_EXE || "pdftoppm";
const DEFAULT_TIMEOUT_MS = 180_000;

function runCmd(
  cmd: string,
  args: string[],
  timeoutMs: number
): Promise<{ exitCode: number; stderr: string }> {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, {
      stdio: ["ignore", "pipe", "pipe"],
    });

    let err = "";
    child.stderr.on("data", (d) => (err += d.toString()));

    const to = setTimeout(() => {
      try {
        child.kill("SIGKILL");
      } catch {}
      reject(new Error(`${cmd} timeout after ${timeoutMs}ms`));
    }, timeoutMs);

    child.on("close", (code) => {
      clearTimeout(to);
      resolve({ exitCode: code ?? -1, stderr: err });
    });
    child.on("error", reject);
  });
}

/**
 * Render PDF pages to JPEG images using pdftoppm.
 * Output: {outputDir}/{prefix}-1.jpg, {prefix}-2.jpg, ...
 */
export async function pdfToImages(
  pdfPath: string,
  outputDir: string,
  prefix: string = "page",
  jpegQuality: number = 60,
  timeoutMs: number = DEFAULT_TIMEOUT_MS
): Promise<string[]> {
  fs.mkdirSync(outputDir, { recursive: true });
  const outPrefix = path.join(outputDir, prefix);

  const args = [
    "-jpeg",
    "-jpegopt",
    `quality=${jpegQuality}`,
    pdfPath,
    outPrefix,
  ];

  const { exitCode, stderr } = await runCmd(PDFTOPPM_EXE, args, timeoutMs);
  if (exitCode !== 0) {
    throw new Error(`pdftoppm failed code=${exitCode}\n${stderr}`);
  }

  const files = fs.readdirSync(outputDir);
  const jpegs = files
    .filter((f) => f.startsWith(prefix) && /\.(jpg|jpeg)$/i.test(f))
    .sort((a, b) => {
      const na = parseInt(a.replace(/\D/g, ""), 10) || 0;
      const nb = parseInt(b.replace(/\D/g, ""), 10) || 0;
      return na - nb;
    })
    .map((f) => path.join(outputDir, f));

  return jpegs;
}

/**
 * Raster fallback: PDF → images → PDF (compressed), then return path.
 */
export async function rasterizePdf(
  pdfPath: string,
  outputPdfPath: string,
  workDir: string,
  jpegQuality: number = 60,
  timeoutMs: number = DEFAULT_TIMEOUT_MS
): Promise<string> {
  const imgDir = path.join(workDir, ".__raster_images");
  try {
    fs.rmSync(imgDir, { recursive: true, force: true });
  } catch {}
  fs.mkdirSync(imgDir, { recursive: true });

  const imagePaths = await pdfToImages(
    pdfPath,
    imgDir,
    "page",
    jpegQuality,
    timeoutMs
  );

  if (imagePaths.length === 0) {
    throw new Error("pdftoppm produced no images");
  }

  await imagesToPdf(imagePaths, outputPdfPath, timeoutMs);

  try {
    fs.rmSync(imgDir, { recursive: true, force: true });
  } catch {}

  return outputPdfPath;
}
