import argparse
import os
import shutil
import sys
import json
import traceback
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


def _gentle_params(level):
    """Stepped gentle heuristics: 1=most conservative, 3=slightly stronger."""
    tbl = {
        1: {"min_side": 1800, "min_q": 48, "replace": 0.97, "skip_bytes": 150_000, "skip_px": 2_000_000, "skip_bytes_jpeg": 4_000_000},
        2: {"min_side": 1600, "min_q": 45, "replace": 0.95, "skip_bytes": 100_000, "skip_px": 1_500_000, "skip_bytes_jpeg": 3_000_000},
        3: {"min_side": 1400, "min_q": 42, "replace": 0.93, "skip_bytes": 80_000, "skip_px": 1_200_000, "skip_bytes_jpeg": 2_500_000},
    }
    return tbl.get(max(1, min(3, level)), tbl[1])


def _recompress_one(payload):
    """
    payload = (sha1, data, w, h, filt, max_side, q, force, gentle, gentle_level)
    return (sha1, new_bytes or None)
    """
    sha1, data, w, h, filt, max_side, q, force, gentle, gentle_level = payload

    try:
        if gentle:
            gp = _gentle_params(gentle_level)
            min_side = gp["min_side"]
            min_q = gp["min_q"]
            replace_threshold = gp["replace"]
            skip_stream_bytes = gp["skip_bytes"]
            skip_px = gp["skip_px"]
            skip_bytes_jpeg = gp["skip_bytes_jpeg"]
        else:
            min_side = MIN_SIDE_FLOOR
            min_q = MIN_Q_FLOOR
            replace_threshold = 0.92
            skip_stream_bytes = 0
            skip_px = SKIP_JPEG_IF_PIXELS_LT
            skip_bytes_jpeg = SKIP_JPEG_IF_BYTES_LT

        # Gentle: skip already-small streams (preserve acceptable assets)
        if gentle and not force and len(data) < skip_stream_bytes:
            return (sha1, None)

        # JPEG small enough => skip (unless force); gentle uses higher thresholds
        if not force and "DCTDecode" in filt and w and h:
            if (w * h) < skip_px and len(data) < skip_bytes_jpeg:
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
    {sha1: {"xobjs":[xobj,...], "len":N, "w":w, "h":h, "filt":str, "pages":[1,2,...]}}
    """
    mp = {}

    for page_no, page in enumerate(pdf.pages, start=1):
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
                    mp[sha1] = {"xobjs": [xobj], "len": len(data), "w": w, "h": h, "filt": filt, "data": data, "pages": [page_no]}
                else:
                    mp[sha1]["xobjs"].append(xobj)
                    mp[sha1]["pages"].append(page_no)
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


def recompress_topk_parallel(pdf: pikepdf.Pdf, max_side: int, q: int, top_k: int, min_stream_bytes: int, force: bool, gentle: bool = False, gentle_level: int = 1):
    """
    Returns (changed_count, n_images_found, n_images_selected).
    """
    imgmap = _collect_images(pdf, min_stream_bytes=min_stream_bytes)

    n_pages = len(pdf.pages)
    n_found = len(imgmap)
    # sort by stream bytes desc, take top_k
    items = sorted(imgmap.items(), key=lambda kv: kv[1]["len"], reverse=True)[:top_k]
    n_selected = len(items)

    print(f"[SELECTIVE_RECOMPRESS] total_pages={n_pages} images_found={n_found} images_selected={n_selected} (top_k={top_k} min_stream_kb={min_stream_bytes // 1024} gentle={gentle})", file=sys.stderr)
    if n_found == 0:
        print(
            f"[SELECTIVE_RECOMPRESS] no images qualify: min_stream_kb={min_stream_bytes // 1024} top_k={top_k} gentle={gentle} (lower --min_stream_kb or check PDF has embedded images above threshold)",
            file=sys.stderr,
        )
    elif n_selected == 0:
        print(f"[SELECTIVE_RECOMPRESS] no images selected: top_k={top_k} gentle={gentle}", file=sys.stderr)
    else:
        for idx, (sha1, meta) in enumerate(items):
            pages = meta.get("pages", [])
            page_str = str(pages[0]) if pages else "?"
            print(
                f"[SELECTIVE_RECOMPRESS] selected[{idx + 1}] page={page_str} w={meta['w']} h={meta['h']} stream_bytes={meta['len']} max_side={max_side} jpeg_q={q}",
                file=sys.stderr,
            )

    payloads = []
    for sha1, meta in items:
        payloads.append(
            (sha1, meta["data"], meta["w"], meta["h"], meta["filt"], max_side, q, force, gentle, gentle_level)
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
    print(f"[SELECTIVE_RECOMPRESS] images_replaced_this_pass={changed}", file=sys.stderr)

    # free big bytes
    for _, meta in imgmap.items():
        meta.pop("data", None)

    return (changed, n_found, n_selected)


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
    parser.add_argument("--gentle_level", type=int, default=1,
                        help="1=most conservative, 2=moderate, 3=slightly stronger (Zone A stepped)")

    args = parser.parse_args()

    inp = os.path.abspath(args.input)
    out = args.output
    tmp_out = os.path.abspath(out)
    parent_dir = os.path.dirname(tmp_out)
    if parent_dir:
        os.makedirs(parent_dir, exist_ok=True)

    target_bytes = int(args.target_mb * 1024 * 1024)
    top_k = max(1, int(args.top_k))
    min_stream_bytes = max(0, int(args.min_stream_kb * 1024))
    max_side = int(args.max_side)
    jpeg_q = int(args.jpeg_q)
    passes = max(1, int(args.passes))
    force = bool(args.force)
    gentle = bool(args.gentle)
    gentle_level = max(1, min(3, int(args.gentle_level)))

    before = os.path.getsize(inp)
    print(f"[SELECTIVE_RECOMPRESS] total_bytes_before={before}", file=sys.stderr)
    images_touched = 0
    passes_run = 0
    last_n_found = 0
    last_n_selected = 0
    base_out, ext_out = os.path.splitext(tmp_out)

    cur_in = inp

    for _ in range(passes):
        passes_run += 1
        pass_out = f"{base_out}.pass{passes_run}{ext_out}"
        print(
            f"[SELECTIVE_RECOMPRESS] pass={passes_run} input={cur_in!r} output={pass_out!r}",
            file=sys.stderr,
        )

        with pikepdf.open(cur_in) as pdf:
            changed, last_n_found, last_n_selected = recompress_topk_parallel(
                pdf,
                max_side=max_side,
                q=jpeg_q,
                top_k=top_k,
                min_stream_bytes=min_stream_bytes,
                force=force,
                gentle=gentle,
                gentle_level=gentle_level,
            )
            images_touched += changed

            # Robust output-path handling (Windows-safe): never read and write same path
            if os.path.exists(pass_out):
                os.remove(pass_out)
            out_dir = os.path.dirname(pass_out)
            out_dir_exists = os.path.isdir(out_dir) if out_dir else True
            out_exists_before = os.path.exists(pass_out)
            input_exists = os.path.isfile(cur_in)
            input_size = os.path.getsize(cur_in) if input_exists else None
            print(
                f"[SELECTIVE_RECOMPRESS] before save: input={cur_in!r} output={pass_out!r} input_exists={input_exists} input_size_bytes={input_size} output_dir_exists={out_dir_exists} output_exists_before_save={out_exists_before}",
                file=sys.stderr,
            )

            # Safe save with retry
            try:
                pdf.save(pass_out, garbage=3, deflate=True)
            except Exception as e:
                print(f"[SELECTIVE_RECOMPRESS] save failed (full): {e!r}", file=sys.stderr)
                print(traceback.format_exc(), file=sys.stderr)
                try:
                    pdf.save(pass_out)
                except Exception as e2:
                    print(f"[SELECTIVE_RECOMPRESS] retry save failed (full): {e2!r}", file=sys.stderr)
                    print(traceback.format_exc(), file=sys.stderr)
                    raise RuntimeError(
                        f"selective_recompress save failed (original: {e!r}, retry: {e2!r})"
                    ) from e2

            saved_bytes = os.path.getsize(pass_out)
            print(
                f"[SELECTIVE_RECOMPRESS] after save: saved_bytes={saved_bytes} path={pass_out!r} total_bytes_after={saved_bytes}",
                file=sys.stderr,
            )

        after = saved_bytes
        cur_in = pass_out
        if after <= target_bytes:
            break

    # Place final result at requested output path; clean up intermediates
    final_result = cur_in
    if final_result != tmp_out:
        if os.path.exists(tmp_out):
            os.remove(tmp_out)
        shutil.copy2(final_result, tmp_out)
        for n in range(1, passes_run + 1):
            p = f"{base_out}.pass{n}{ext_out}"
            if os.path.exists(p):
                try:
                    os.remove(p)
                except OSError:
                    pass

    after_bytes = os.path.getsize(tmp_out)
    before_mb = round(before / (1024 * 1024), 2)
    after_mb = round(after_bytes / (1024 * 1024), 2)
    print(
        f"[SELECTIVE_RECOMPRESS] final_summary images_found={last_n_found} images_selected={last_n_selected} images_replaced={images_touched} before_mb={before_mb} after_mb={after_mb}",
        file=sys.stderr,
    )

    summary = {
        "before_bytes": before,
        "after_bytes": after_bytes,
        "images_touched": images_touched,
        "passes_run": passes_run,
    }
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
