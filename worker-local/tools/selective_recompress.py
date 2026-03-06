import argparse
import os
import sys
import json
import hashlib
from io import BytesIO
from concurrent.futures import ProcessPoolExecutor, as_completed

import pikepdf
from PIL import Image

WORKERS = max(2, (os.cpu_count() or 4) - 1)  # CPU-1

# QUALITY floors
MIN_SIDE_FLOOR = 1200
MIN_Q_FLOOR = 35

# Heuristic: already-small JPEG бол алгасна (can be disabled via --force)
SKIP_JPEG_IF_PIXELS_LT = 1_000_000
SKIP_JPEG_IF_BYTES_LT = 2_000_000


def _resize(img: Image.Image, max_side: int) -> Image.Image:
    w, h = img.size
    m = max(w, h)
    if m <= max_side:
        return img
    r = max_side / float(m)
    nw, nh = max(1, int(w * r)), max(1, int(h * r))
    return img.resize((nw, nh), Image.LANCZOS)


def _recompress_one(payload):
    """
    payload = (sha1, data, w, h, filt, max_side, q, force, gentle)
    return (sha1, new_bytes or None)
    """
    sha1, data, w, h, filt, max_side, q, force, gentle = payload

    try:
        # Gentle: higher skip thresholds to preserve quality
        min_side = 1600 if gentle else MIN_SIDE_FLOOR
        min_q = 45 if gentle else MIN_Q_FLOOR
        replace_threshold = 0.95 if gentle else 0.92  # don't replace if save <5% (gentle) or <8%

        # JPEG small enough => skip (unless force=true); gentle uses higher thresholds
        if not force and "DCTDecode" in filt and w and h:
            skip_px = 1_500_000 if gentle else SKIP_JPEG_IF_PIXELS_LT
            skip_bytes = 3_000_000 if gentle else SKIP_JPEG_IF_BYTES_LT
            if (w * h) < skip_px and len(data) < skip_bytes:
                return (sha1, None)

        img = Image.open(BytesIO(data)).convert("RGB")
        cap = max(max_side, min_side)
        img = _resize(img, cap)

        buf = BytesIO()
        qq = max(q, min_q)
        img.save(buf, format="JPEG", quality=qq, optimize=True)
        new_data = buf.getvalue()

        if len(new_data) >= len(data) * replace_threshold:
            return (sha1, None)

        return (sha1, new_data)

    except Exception:
        return (sha1, None)


def _collect_images(pdf: pikepdf.Pdf, min_stream_bytes: int):
    """
    Return list of unique images by sha1(stream_bytes) with metadata:
    {sha1: {"xobjs":[xobj,...], "len":N, "w":w, "h":h, "filt":str}}
    """
    mp = {}

    for page in pdf.pages:
        res = page.get("/Resources")
        if not res:
            continue
        xobjs = res.get("/XObject")
        if not xobjs:
            continue

        for _, xobj in list(xobjs.items()):
            try:
                if xobj.get("/Subtype") != "/Image":
                    continue
                data = xobj.read_bytes()
                if len(data) < min_stream_bytes:
                    continue

                w = int(xobj.get("/Width", 0) or 0)
                h = int(xobj.get("/Height", 0) or 0)
                filt = str(xobj.get("/Filter", ""))

                sha1 = hashlib.sha1(data).hexdigest()
                if sha1 not in mp:
                    mp[sha1] = {"xobjs": [xobj], "len": len(
                        data), "w": w, "h": h, "filt": filt, "data": data}
                else:
                    mp[sha1]["xobjs"].append(xobj)
            except Exception:
                continue

    return mp


def _apply_results(imgmap, results):
    """
    Replace streams for all xobjs with returned new bytes.
    returns changed_count
    """
    changed = 0
    for sha1, new_data in results.items():
        if not new_data:
            continue
        meta = imgmap.get(sha1)
        if not meta:
            continue
        for xobj in meta["xobjs"]:
            try:
                xobj.stream = new_data
                xobj["/Filter"] = pikepdf.Name("/DCTDecode")
                xobj["/ColorSpace"] = pikepdf.Name("/DeviceRGB")
                changed += 1
            except Exception:
                pass
    return changed


def recompress_topk_parallel(pdf: pikepdf.Pdf, max_side: int, q: int, top_k: int, min_stream_bytes: int, force: bool, gentle: bool = False) -> int:
    imgmap = _collect_images(pdf, min_stream_bytes=min_stream_bytes)

    # sort by stream bytes desc, take top_k
    items = sorted(imgmap.items(), key=lambda kv: kv[1]["len"], reverse=True)[:top_k]

    payloads = []
    for sha1, meta in items:
        payloads.append(
            (sha1, meta["data"], meta["w"], meta["h"], meta["filt"], max_side, q, force, gentle)
        )

    results = {}
    # PARALLEL
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(_recompress_one, p) for p in payloads]
        for f in as_completed(futs):
            sha1, new_data = f.result()
            if new_data:
                results[sha1] = new_data
            else:
                results[sha1] = None

    changed = _apply_results(imgmap, results)

    # free big bytes
    for _, meta in imgmap.items():
        meta.pop("data", None)

    return changed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input")
    parser.add_argument("output")
    parser.add_argument("--target_mb", type=float, default=45.0)
    parser.add_argument("--top_k", type=int, default=220)
    parser.add_argument("--min_stream_kb", type=int, default=200)
    parser.add_argument("--max_side", type=int, default=1100)
    parser.add_argument("--jpeg_q", type=int, default=28)
    parser.add_argument("--passes", type=int, default=2)
    parser.add_argument("--force", action="store_true", default=False)
    parser.add_argument("--gentle", action="store_true", default=False,
                        help="Smart Quality mode: preserve readability, skip marginal gains")

    args = parser.parse_args()

    inp = args.input
    out = args.output

    target_bytes = int(args.target_mb * 1024 * 1024)
    top_k = max(1, int(args.top_k))
    min_stream_bytes = max(0, int(args.min_stream_kb * 1024))
    max_side = int(args.max_side)
    jpeg_q = int(args.jpeg_q)
    passes = max(1, int(args.passes))
    force = bool(args.force)
    gentle = bool(args.gentle)

    before = os.path.getsize(inp)
    images_touched = 0
    passes_run = 0

    cur_in = inp
    tmp_out = out

    for _ in range(passes):
        passes_run += 1
        with pikepdf.open(cur_in) as pdf:
            images_touched += recompress_topk_parallel(
                pdf,
                max_side=max_side,
                q=jpeg_q,
                top_k=top_k,
                min_stream_bytes=min_stream_bytes,
                force=force,
                gentle=gentle,
            )
            pdf.save(tmp_out)

        after = os.path.getsize(tmp_out)
        if after <= target_bytes:
            break

        # next pass uses previous output as input
        cur_in = tmp_out

    summary = {
        "before_bytes": before,
        "after_bytes": os.path.getsize(tmp_out),
        "images_touched": images_touched,
        "passes_run": passes_run,
    }
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
