#!/usr/bin/env python3
"""
Attach full note text to chunked note parquet files by note_id.

This script upgrades truncated note chunks by joining an external source
containing full note text and writing new chunk files with `note_text_full`.

Input requirements for full text source:
- must contain a note identifier column (default: note_id)
- must contain a full text column (default: note_text)

Example:
  python resources/script/run_attach_full_note_text.py \
    --notes-dir episode_notes \
    --glob 'episode_notes_chunk*.parquet' \
    --full-text-source '/path/to/full_notes.parquet' \
    --full-source-note-id-col note_id \
    --full-source-text-col note_text \
    --output-dir episode_notes_fulltext
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Attach full note text to chunked parquet files")
    ap.add_argument("--notes-dir", type=Path, default=Path("episode_notes"))
    ap.add_argument("--glob", default="episode_notes_chunk*.parquet")
    ap.add_argument("--full-text-source", type=Path, required=True, help="CSV or parquet with full note text")
    ap.add_argument("--full-source-note-id-col", default="note_id")
    ap.add_argument("--full-source-text-col", default="note_text")
    ap.add_argument("--chunk-note-id-col", default="note_id")
    ap.add_argument("--output-dir", type=Path, default=Path("episode_notes_fulltext"))
    ap.add_argument(
        "--full-text-target-col",
        default="note_text_full",
        help="Column name to store full text in upgraded chunks",
    )
    return ap.parse_args()


def load_full_source(path: Path, note_id_col: str, text_col: str) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"full-text-source not found: {path}")

    if path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path)
    else:
        df = pd.read_csv(path)

    missing = [c for c in [note_id_col, text_col] if c not in df.columns]
    if missing:
        raise SystemExit(f"Missing required columns in full source: {missing}")

    out = df[[note_id_col, text_col]].copy()
    out = out.dropna(subset=[note_id_col, text_col])
    out[note_id_col] = out[note_id_col].astype(str)
    out[text_col] = out[text_col].astype(str)
    out = out.sort_values(note_id_col).drop_duplicates(subset=[note_id_col], keep="last")
    return out


def main() -> None:
    args = parse_args()

    files = sorted(args.notes_dir.glob(args.glob))
    if not files:
        raise SystemExit(f"No note chunk files matched: {args.notes_dir / args.glob}")

    full_df = load_full_source(
        path=args.full_text_source,
        note_id_col=args.full_source_note_id_col,
        text_col=args.full_source_text_col,
    )
    full_df = full_df.rename(
        columns={
            args.full_source_note_id_col: "_full_note_id",
            args.full_source_text_col: args.full_text_target_col,
        }
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    total_matched = 0
    for p in files:
        chunk = pd.read_parquet(p)
        if args.chunk_note_id_col not in chunk.columns:
            raise SystemExit(f"Chunk missing note id column `{args.chunk_note_id_col}`: {p}")

        chunk = chunk.copy()
        chunk["_full_note_id"] = chunk[args.chunk_note_id_col].astype(str)
        merged = chunk.merge(full_df, on="_full_note_id", how="left")

        matched = int(merged[args.full_text_target_col].notna().sum())
        total_rows += len(merged)
        total_matched += matched

        merged = merged.drop(columns=["_full_note_id"])
        out_path = args.output_dir / p.name
        merged.to_parquet(out_path, index=False)

        pct = 100.0 * matched / max(len(merged), 1)
        print(f"{p.name}: matched_full_text={matched}/{len(merged)} ({pct:.2f}%) -> {out_path}")

    overall = 100.0 * total_matched / max(total_rows, 1)
    print("=" * 80)
    print(f"Total matched_full_text={total_matched}/{total_rows} ({overall:.2f}%)")
    print(f"Output dir: {args.output_dir}")
    print("Next: run selectors/candidate extraction with --note-text-col-candidates note_text_full,note_text")


if __name__ == "__main__":
    main()
