#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path


def file_size_mb(path: str) -> float:
    return os.path.getsize(path) / (1024 * 1024)


def find_gs() -> str:
    candidates = [
        os.environ.get("GS_CMD"),
        r"C:\Program Files\gs\gs10.05.1\bin\gswin64c.exe",
        r"C:\Program Files\gs\gs10.04.0\bin\gswin64c.exe",
        r"C:\Program Files\gs\gs10.03.1\bin\gswin64c.exe",
        r"C:\Program Files\gs\gs10.03.0\bin\gswin64c.exe",
        shutil.which("gswin64c"),
        shutil.which("gswin32c"),
        shutil.which("gs"),
    ]
    for c in candidates:
        if c and os.path.exists(c):
            return c
    raise FileNotFoundError(
        "Ghostscript not found. Set GS_CMD env var or install Ghostscript."
    )


def run_gs(gs_exe: str, input_pdf: str, output_pdf: str, preset: str, dpi: int) -> tuple[int, str, str]:
    cmd = [
        gs_exe,
        "-sDEVICE=pdfwrite",
        "-dCompatibilityLevel=1.4",
        "-dNOPAUSE",
        "-dQUIET",
        "-dBATCH",
        f"-dPDFSETTINGS=/{preset}",
        "-dDetectDuplicateImages=true",
        "-dCompressFonts=true",
        "-dSubsetFonts=true",
        "-dDownsampleColorImages=true",
        "-dDownsampleGrayImages=true",
        "-dDownsampleMonoImages=true",
        "-dColorImageDownsampleType=/Bicubic",
        "-dGrayImageDownsampleType=/Bicubic",
        "-dMonoImageDownsampleType=/Subsample",
        f"-dColorImageResolution={dpi}",
        f"-dGrayImageResolution={dpi}",
        f"-dMonoImageResolution={max(150, dpi)}",
        f"-sOutputFile={output_pdf}",
        input_pdf,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    return proc.returncode, proc.stdout, proc.stderr


def build_ladder(input_mb: float) -> list[dict]:
    # 0–200MB = quality-first
    # 200MB+ = aggressive
    if input_mb <= 200:
        return [
            {"preset": "ebook", "dpi": 150, "label": "q1_ebook_150"},
            {"preset": "ebook", "dpi": 130, "label": "q2_ebook_130"},
            {"preset": "ebook", "dpi": 110, "label": "q3_ebook_110"},
            {"preset": "screen", "dpi": 110, "label": "q4_screen_110"},
            {"preset": "screen", "dpi": 96,  "label": "q5_screen_96"},
            {"preset": "screen", "dpi": 85,  "label": "q6_screen_85"},
            {"preset": "screen", "dpi": 72,  "label": "q7_screen_72"},
        ]
    return [
        {"preset": "screen", "dpi": 110, "label": "h1_screen_110"},
        {"preset": "screen", "dpi": 96,  "label": "h2_screen_96"},
        {"preset": "screen", "dpi": 85,  "label": "h3_screen_85"},
        {"preset": "screen", "dpi": 72,  "label": "h4_screen_72"},
        {"preset": "screen", "dpi": 60,  "label": "h5_screen_60"},
    ]


def choose_best_result(results: list[dict], target_mb: float) -> dict | None:
    if not results:
        return None

    under = [r for r in results if r["size_mb"] <= target_mb]
    if under:
        # target-аас доош орсон хамгийн чанартай буюу хамгийн томыг авна
        return sorted(under, key=lambda x: x["size_mb"], reverse=True)[0]

    # хүрээгүй бол хамгийн их shrink өгсөнг авна
    return sorted(results, key=lambda x: x["size_mb"])[0]


def copy_file(src: str, dst: str) -> None:
    Path(dst).parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(src, dst)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("input_pdf")
    ap.add_argument("output_pdf")
    ap.add_argument("--target_mb", type=float, required=True)
    ap.add_argument(
        "--mode", choices=["auto", "quality", "aggressive"], default="auto")
    args = ap.parse_args()

    input_pdf = os.path.abspath(args.input_pdf)
    output_pdf = os.path.abspath(args.output_pdf)
    workdir = os.path.dirname(output_pdf)

    if not os.path.exists(input_pdf):
        print(json.dumps({"ok": False, "error": "input_not_found"}))
        sys.exit(2)

    Path(workdir).mkdir(parents=True, exist_ok=True)

    input_mb = file_size_mb(input_pdf)
    gs_exe = find_gs()

    if args.mode == "quality":
        ladder = [
            {"preset": "ebook", "dpi": 150, "label": "q1_ebook_150"},
            {"preset": "ebook", "dpi": 130, "label": "q2_ebook_130"},
            {"preset": "ebook", "dpi": 110, "label": "q3_ebook_110"},
            {"preset": "screen", "dpi": 96,  "label": "q4_screen_96"},
            {"preset": "screen", "dpi": 72,  "label": "q5_screen_72"},
        ]
    elif args.mode == "aggressive":
        ladder = [
            {"preset": "screen", "dpi": 110, "label": "h1_screen_110"},
            {"preset": "screen", "dpi": 96,  "label": "h2_screen_96"},
            {"preset": "screen", "dpi": 85,  "label": "h3_screen_85"},
            {"preset": "screen", "dpi": 72,  "label": "h4_screen_72"},
            {"preset": "screen", "dpi": 60,  "label": "h5_screen_60"},
        ]
    else:
        ladder = build_ladder(input_mb)

    results: list[dict] = []

    print(
        f"[TARGET_COMPRESS] start input_mb={input_mb:.2f} target_mb={args.target_mb:.2f} mode={args.mode}",
        file=sys.stderr,
    )

    # эхний оролдлого: хэрэв аль хэдийн target-аас доош бол copy
    if input_mb <= args.target_mb:
        copy_file(input_pdf, output_pdf)
        print(json.dumps({
            "ok": True,
            "input_mb": round(input_mb, 2),
            "output_mb": round(input_mb, 2),
            "target_mb": round(args.target_mb, 2),
            "used_original": True,
            "met_target": True,
            "steps": [],
        }))
        return

    for i, step in enumerate(ladder, start=1):
        out_i = os.path.join(
            workdir, f".__target_compress_{i}_{step['label']}.pdf")
        code, stdout, stderr = run_gs(
            gs_exe=gs_exe,
            input_pdf=input_pdf,
            output_pdf=out_i,
            preset=step["preset"],
            dpi=step["dpi"],
        )

        if code != 0 or not os.path.exists(out_i):
            print(
                f"[TARGET_COMPRESS] step_failed step={i} label={step['label']} code={code}",
                file=sys.stderr,
            )
            if stderr:
                print(stderr[:4000], file=sys.stderr)
            continue

        out_mb = file_size_mb(out_i)
        result = {
            "step": i,
            "label": step["label"],
            "preset": step["preset"],
            "dpi": step["dpi"],
            "size_mb": out_mb,
            "path": out_i,
        }
        results.append(result)

        print(
            f"[TARGET_COMPRESS] step={i} label={step['label']} preset={step['preset']} dpi={step['dpi']} out_mb={out_mb:.2f}",
            file=sys.stderr,
        )

        if out_mb <= args.target_mb:
            print(
                f"[TARGET_COMPRESS] target_met step={i} out_mb={out_mb:.2f}",
                file=sys.stderr,
            )
            break

    best = choose_best_result(results, args.target_mb)
    if not best:
        # fallback: original copy
        copy_file(input_pdf, output_pdf)
        print(json.dumps({
            "ok": False,
            "input_mb": round(input_mb, 2),
            "output_mb": round(input_mb, 2),
            "target_mb": round(args.target_mb, 2),
            "used_original": True,
            "met_target": False,
            "steps": [],
            "error": "no_successful_compression_step",
        }))
        return

    copy_file(best["path"], output_pdf)
    final_mb = file_size_mb(output_pdf)

    print(json.dumps({
        "ok": True,
        "input_mb": round(input_mb, 2),
        "output_mb": round(final_mb, 2),
        "target_mb": round(args.target_mb, 2),
        "used_original": False,
        "met_target": final_mb <= args.target_mb,
        "best_step": {
            "step": best["step"],
            "label": best["label"],
            "preset": best["preset"],
            "dpi": best["dpi"],
        },
        "steps": [
            {
                "step": r["step"],
                "label": r["label"],
                "preset": r["preset"],
                "dpi": r["dpi"],
                "size_mb": round(r["size_mb"], 2),
            }
            for r in results
        ],
    }))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(json.dumps({"ok": False, "error": repr(e)}))
        sys.exit(1)
