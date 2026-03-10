/**
 * ghostscript.ts — Ghostscript compression for GOODPDF
 *
 * Uses child_process to invoke gs (Ghostscript).
 * Dependencies: ghostscript must be available in PATH.
 */

import { spawn } from "child_process";
import * as path from "path";

const GS_EXE = process.env.GS_EXE || "gs";
const DEFAULT_TIMEOUT_MS = 120_000;

export interface GsCompressOptions {
  inPdf: string;
  outPdf: string;
  /** /ebook (balanced) or /screen (stronger) */
  pdfSettings?: "/ebook" | "/screen";
  colorImageResolution?: number;
  grayImageResolution?: number;
  monoImageResolution?: number;
  timeoutMs?: number;
}

function runGs(args: string[], timeoutMs: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const child = spawn(GS_EXE, args, {
      stdio: ["ignore", "pipe", "pipe"],
    });

    let err = "";
    child.stderr.on("data", (d) => (err += d.toString()));

    const to = setTimeout(() => {
      try {
        child.kill("SIGKILL");
      } catch {}
      reject(new Error(`gs timeout after ${timeoutMs}ms`));
    }, timeoutMs);

    child.on("close", (code) => {
      clearTimeout(to);
      if (code !== 0) {
        reject(new Error(`gs failed code=${code}\n${err}`));
        return;
      }
      resolve();
    });
    child.on("error", reject);
  });
}

/** Balanced compression: /ebook, 150 DPI color/gray, 300 mono */
export async function compressBalanced(options: GsCompressOptions): Promise<void> {
  const {
    inPdf,
    outPdf,
    pdfSettings = "/ebook",
    colorImageResolution = 150,
    grayImageResolution = 150,
    monoImageResolution = 300,
    timeoutMs = DEFAULT_TIMEOUT_MS,
  } = options;

  const args = [
    "-q",
    "-dSAFER",
    "-dBATCH",
    "-dNOPAUSE",
    "-sDEVICE=pdfwrite",
    "-dCompatibilityLevel=1.4",
    `-dPDFSETTINGS=${pdfSettings}`,
    `-dColorImageResolution=${colorImageResolution}`,
    `-dGrayImageResolution=${grayImageResolution}`,
    `-dMonoImageResolution=${monoImageResolution}`,
    "-sOutputFile=" + outPdf,
    inPdf,
  ];

  await runGs(args, timeoutMs);
}

/** Stronger rescue compression: /screen, 110 DPI for oversize parts */
export async function compressRescue(options: GsCompressOptions): Promise<void> {
  const {
    inPdf,
    outPdf,
    pdfSettings = "/screen",
    colorImageResolution = 110,
    grayImageResolution = 110,
    monoImageResolution = 300,
    timeoutMs = DEFAULT_TIMEOUT_MS,
  } = options;

  const args = [
    "-q",
    "-dSAFER",
    "-dBATCH",
    "-dNOPAUSE",
    "-sDEVICE=pdfwrite",
    "-dCompatibilityLevel=1.4",
    `-dPDFSETTINGS=${pdfSettings}`,
    `-dColorImageResolution=${colorImageResolution}`,
    `-dGrayImageResolution=${grayImageResolution}`,
    `-dMonoImageResolution=${monoImageResolution}`,
    "-sOutputFile=" + outPdf,
    inPdf,
  ];

  await runGs(args, timeoutMs);
}

/**
 * Rebuild PDF from JPEG image files (for raster fallback).
 * Uses Ghostscript viewJPEG.ps to embed images without re-encoding.
 */
export async function imagesToPdf(
  imagePaths: string[],
  outPdf: string,
  timeoutMs: number = DEFAULT_TIMEOUT_MS
): Promise<void> {
  if (imagePaths.length === 0) {
    throw new Error("No images to convert");
  }

  // Build PostScript: (path) viewJPEG showpage for each image.
  // viewjpeg.ps is in Ghostscript's lib path; gs finds it automatically.
  const absPaths = imagePaths.map((p) => path.resolve(p));
  const psFragments = absPaths
    .map((p) => `(${p.replace(/\\/g, "/")}) viewJPEG showpage`)
    .join(" ");

  const args = [
    "-q",
    "-dNOPAUSE",
    "-dBATCH",
    "-sDEVICE=pdfwrite",
    "-dCompatibilityLevel=1.4",
    "-sOutputFile=" + outPdf,
    "viewjpeg.ps",
    "-c",
    psFragments,
  ];

  await runGs(args, timeoutMs);
}
