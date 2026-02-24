import os
import sys
from PIL import Image


def main():
    if len(sys.argv) < 3:
        print("Usage: python images_to_pdf.py <images_dir> <output_pdf>")
        sys.exit(1)

    images_dir = sys.argv[1]
    out_pdf = sys.argv[2]

    if not os.path.isdir(images_dir):
        print("images_dir not found")
        sys.exit(2)

    files = sorted(
        f for f in os.listdir(images_dir)
        if f.lower().endswith((".jpg", ".jpeg"))
    )
    if not files:
        print("no images")
        sys.exit(3)

    imgs = []
    for f in files:
        p = os.path.join(images_dir, f)
        im = Image.open(p).convert("RGB")
        imgs.append(im)

    try:
        first = imgs[0]
        rest = imgs[1:]
        first.save(out_pdf, "PDF", save_all=True, append_images=rest, resolution=96.0)
    finally:
        for im in imgs:
            try:
                im.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
