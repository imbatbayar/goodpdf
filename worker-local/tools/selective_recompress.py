import os
import sys
import json
import hashlib
from io import BytesIO
from concurrent.futures import ProcessPoolExecutor, as_completed

import pikepdf
from PIL import Image

TARGET_BYTES = 45 * 1024 * 1024

# SPEED knobs
TOP_K = 60                     # зөвхөн хамгийн том 60 image stream
MIN_STREAM_BYTES = 350 * 1024  # 350KB-с жижиг stream дээр оролдохгүй
WORKERS = max(2, (os.cpu_count() or 4) - 1)  # CPU-1

# QUALITY floors
MAX_SIDE_DEFAULT = 1500
MAX_SIDE_RESCUE = 1300
JPEG_Q_DEFAULT = 40
JPEG_Q_RESCUE = 35
MIN_SIDE_FLOOR = 1200
MIN_Q_FLOOR = 35

# Heuristic: already-small JPEG бол алгасна
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
    payload = (sha1, data, w, h, filt, max_side, q)
    return (sha1, new_bytes or None)
    """
    sha1, data, w, h, filt, max_side, q = payload

    try:
        # JPEG small enough => skip
        if "DCTDecode" in filt and w and h:
            if (w * h) < SKIP_JPEG_IF_PIXELS_LT and len(data) < SKIP_JPEG_IF_BYTES_LT:
                return (sha1, None)

        img = Image.open(BytesIO(data)).convert("RGB")
        cap = max(max_side, MIN_SIDE_FLOOR)
        img = _resize(img, cap)

        buf = BytesIO()
        qq = max(q, MIN_Q_FLOOR)
        img.save(buf, format="JPEG", quality=qq, optimize=True)
        new_data = buf.getvalue()

        # зөвхөн бодитоор багассан бол replace
        if len(new_data) >= len(data) * 0.92:
            return (sha1, None)

        return (sha1, new_data)

    except Exception:
        return (sha1, None)


def _collect_images(pdf: pikepdf.Pdf):
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
                if len(data) < MIN_STREAM_BYTES:
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


def recompress_topk_parallel(pdf: pikepdf.Pdf, max_side: int, q: int) -> int:
    imgmap = _collect_images(pdf)

    # sort by stream bytes desc, take TOP_K
    items = sorted(imgmap.items(), key=lambda kv: kv[1]["len"], reverse=True)[
        :TOP_K]

    payloads = []
    for sha1, meta in items:
        payloads.append(
            (sha1, meta["data"], meta["w"], meta["h"], meta["filt"], max_side, q))

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
    if len(sys.argv) < 3:
        print("Usage: python selective_recompress.py input.pdf output.pdf")
        sys.exit(1)

    inp = sys.argv[1]
    out = sys.argv[2]

    before = os.path.getsize(inp)
    images_touched = 0
    rescue_used = False

    # PASS 1
    with pikepdf.open(inp) as pdf:
        images_touched += recompress_topk_parallel(
            pdf, MAX_SIDE_DEFAULT, JPEG_Q_DEFAULT)
        pdf.save(out)

    after = os.path.getsize(out)
    if after <= TARGET_BYTES:
        print(json.dumps({"before_bytes": before, "after_bytes": after,
              "images_touched": images_touched, "rescue_used": False}))
        return

    # PASS 2 (rescue once)
    rescue_used = True
    with pikepdf.open(out) as pdf:
        images_touched += recompress_topk_parallel(
            pdf, MAX_SIDE_RESCUE, JPEG_Q_RESCUE)
        pdf.save(out)

    after2 = os.path.getsize(out)
    print(json.dumps({"before_bytes": before, "after_bytes": after2,
          "images_touched": images_touched, "rescue_used": rescue_used}))


if __name__ == "__main__":
    main()
