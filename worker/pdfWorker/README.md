# pdfWorker — Deterministic PDF Processing for GOODPDF

Production PDF processing pipeline: **Normalize → Compress → Split → Oversize Rescue → Raster Fallback**.

## Policies

| Input Size | Part Limit |
|------------|------------|
| < 200MB    | ≤ 5 parts  |
| 200–500MB  | ≤ 10 parts |
| Target part size | ≈ 9MB (8–9MB preferred) |

## Pipeline

1. **Normalize** — `qpdf --linearize` to linearize and normalize structure
2. **Balanced Compress** — Ghostscript `/ebook`, 150 DPI color/gray, 300 mono
3. **Split** — Page ranges by estimated bytes per page
4. **Oversize Rescue** — Parts > 9MB recompressed with `/screen`, 110 DPI
5. **Raster Fallback** — If still over limits: `pdftoppm` → images → Ghostscript → PDF → split again

## Dependencies

- **qpdf** — normalize, split
- **ghostscript** — compress, images→PDF (raster fallback)
- **pdftoppm** (poppler) — PDF→images for raster fallback

## Build

```bash
npm run build:worker
```

## Usage

```ts
import { processPdf } from "./worker/pdfWorker";

const result = await processPdf("/path/to/input.pdf", "/path/to/output");
// result: { parts, partCount, avgPartSize, usedFallback }
```

## Integration with worker-local

Set `USE_PDF_WORKER=true` to use this deterministic pipeline instead of the default adaptive logic. Requires `npm run build:worker` first.
