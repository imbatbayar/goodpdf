/**
 * pageProbe.ts — Byte-aware page planning for GOODPDF
 *
 * Probes per-page byte sizes and builds ranges that target part size.
 * Essential for image-heavy PDFs where page sizes vary heavily.
 */

import * as fs from "fs";
import * as path from "path";
import * as os from "os";
import { spawn } from "child_process";

const QPDF_EXE = process.env.QPDF_EXE || "qpdf";
const SPLIT_PAR = Math.max(1, Math.min(4, Math.floor((os.cpus?.()?.length ?? 2) / 2)));

function safeStatSize(p: string): number | null {
  try {
    if (!fs.existsSync(p)) return null;
    const st = fs.statSync(p);
    return Number.isFinite(st?.size) ? st.size : null;
  } catch {
    return null;
  }
}

function safeRm(p: string): void {
  try {
    fs.rmSync(p, { recursive: true, force: true });
  } catch {}
}

function runCmd(
  cmd: string,
  args: string[],
  timeoutMs: number,
  okCodes: number[] = [0]
): Promise<{ exitCode: number }> {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, { stdio: ["ignore", "pipe", "pipe"] });
    const to = setTimeout(() => {
      try {
        child.kill("SIGKILL");
      } catch {}
      reject(new Error(`${cmd} timeout`));
    }, timeoutMs);
    child.on("close", (code) => {
      clearTimeout(to);
      if (code !== null && !okCodes.includes(code)) {
        reject(new Error(`${cmd} failed code=${code}`));
        return;
      }
      resolve({ exitCode: code ?? -1 });
    });
    child.on("error", reject);
  });
}

async function qpdfExtractPage(
  inPdf: string,
  pageNum: number,
  outPdf: string,
  timeoutMs: number
): Promise<number> {
  await runCmd(
    QPDF_EXE,
    ["--empty", "--pages", inPdf, `${pageNum}-${pageNum}`, "--", outPdf],
    timeoutMs,
    [0, 3]
  );
  return safeStatSize(outPdf) ?? 0;
}

async function asyncPool<T, R>(
  limit: number,
  items: T[],
  fn: (item: T, i: number) => Promise<R>
): Promise<R[]> {
  const ret: R[] = [];
  let i = 0;
  const workers = Array(Math.min(limit, items.length))
    .fill(0)
    .map(async () => {
      while (i < items.length) {
        const idx = i++;
        ret[idx] = await fn(items[idx], idx);
      }
    });
  await Promise.all(workers);
  return ret;
}

/**
 * Probe per-page byte sizes by extracting each page with qpdf.
 * Returns array of bytes per page (1-indexed: pageBytes[0] = page 1).
 */
export async function probePageSizesViaSingles(
  inPdf: string,
  pageCount: number,
  probeDir: string,
  timeoutMs: number
): Promise<number[]> {
  safeRm(probeDir);
  fs.mkdirSync(probeDir, { recursive: true });
  const tEach = Math.min(30_000, Math.floor(timeoutMs / Math.max(1, pageCount)));
  const pageNums = Array.from({ length: pageCount }, (_, i) => i + 1);

  const rows = await asyncPool(SPLIT_PAR, pageNums, async (p) => {
    const onePath = path.join(probeDir, `p${String(p).padStart(5, "0")}.pdf`);
    const b = await qpdfExtractPage(inPdf, p, onePath, tEach);
    return { p, bytes: b };
  });

  safeRm(probeDir);
  rows.sort((a, b) => a.p - b.p);
  return rows.map((r) => Math.max(0, Number(r.bytes || 0)));
}

/**
 * Build page ranges from per-page byte estimates.
 * Uses cumulative suffix to balance part sizes near targetBytes.
 */
export function buildRangesNearTarget(
  pageBytes: number[],
  targetBytes: number,
  maxParts: number
): Array<{ start: number; end: number }> {
  const pages = pageBytes.length;
  if (pages <= 0) return [{ start: 1, end: 1 }];

  const totalBytes = pageBytes.reduce((a, b) => a + b, 0);
  const parts = Math.max(
    1,
    Math.min(
      maxParts,
      pages,
      Math.ceil(
        totalBytes / Math.max(256 * 1024, Math.floor(targetBytes * 0.96))
      )
    )
  );

  const suffix = new Array(pages + 2).fill(0);
  for (let i = pages; i >= 1; i--) {
    suffix[i] = suffix[i + 1] + pageBytes[i - 1];
  }

  const ranges: Array<{ start: number; end: number }> = [];
  let start = 1;
  let running = 0;
  let remainingParts = parts;

  for (let p = 1; p <= pages; p++) {
    const remainingPages = pages - p + 1;
    if (remainingPages === remainingParts) {
      if (start <= p - 1) ranges.push({ start, end: p - 1 });
      for (let q = p; q <= pages; q++) ranges.push({ start: q, end: q });
      return ranges;
    }

    const remainingBytes = suffix[p];
    const dynamicTarget = Math.min(
      Math.floor(targetBytes * 0.98),
      Math.max(
        256 * 1024,
        Math.floor(remainingBytes / Math.max(1, remainingParts))
      )
    );

    const b = pageBytes[p - 1];
    if (running > 0 && running + b > dynamicTarget) {
      ranges.push({ start, end: p - 1 });
      remainingParts--;
      start = p;
      running = 0;
    }
    running += b;
  }

  if (start <= pages) ranges.push({ start, end: pages });
  return ranges;
}
