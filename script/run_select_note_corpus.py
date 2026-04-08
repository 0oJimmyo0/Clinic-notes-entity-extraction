#!/usr/bin/env python3
"""
Build a capped note corpus manifest (and optional parquet) from episode note chunks.

Default behavior:
- Reads episode_notes_chunk000-019 parquet files.
- Keeps non-empty notes.
- De-duplicates by note_id.
- Applies patient-aware cap (max notes per patient), then fills to max total.
- Writes manifest CSV with person_id, visit_occurrence_id, note_id.

Example:
  python resources/script/run_select_note_corpus.py \
    --notes-dir episode_notes \
    --glob 'episode_notes_chunk*.parquet' \
    --max-notes 50000 \
    --max-per-patient 50 \
    --seed 42 \
    --output-manifest episode_notes/selected_corpus_50k_manifest.csv \
    --output-parquet episode_notes/selected_corpus_50k.parquet
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Select capped note corpus from chunked parquet files")
    ap.add_argument("--notes-dir", type=Path, default=Path("episode_notes"))
    ap.add_argument("--glob", default="episode_notes_chunk*.parquet")
    ap.add_argument("--max-notes", type=int, default=50000)
    ap.add_argument("--max-per-patient", type=int, default=50)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--output-manifest", type=Path, default=Path("episode_notes/selected_corpus_50k_manifest.csv"))
    ap.add_argument("--output-parquet", type=Path, default=Path("episode_notes/selected_corpus_50k.parquet"))
    ap.add_argument("--no-output-parquet", action="store_true", help="Only write manifest CSV")
    return ap.parse_args()


def main() -> None:
    args = parse_args()

    files = sorted(args.notes_dir.glob(args.glob))
    if not files:
        raise SystemExit(f"No files matched: {args.notes_dir / args.glob}")

    needed = ["person_id", "visit_occurrence_id", "note_id", "note_text"]
    frames = []
    for p in files:
        df = pd.read_parquet(p)
        miss = [c for c in needed if c not in df.columns]
        if miss:
            raise SystemExit(f"Missing columns {miss} in {p}")
        frames.append(df[needed])

    all_notes = pd.concat(frames, ignore_index=True)
    non_empty = all_notes["note_text"].fillna("").astype(str).str.strip() != ""

    work = all_notes[non_empty].dropna(subset=["person_id", "visit_occurrence_id", "note_id"]).copy()
    work = work.drop_duplicates(subset=["note_id"], keep="first").copy()

    rng = np.random.default_rng(args.seed)
    work["_rand"] = rng.random(len(work))
    work = work.sort_values(["person_id", "_rand"])
    work["_rank"] = work.groupby("person_id").cumcount() + 1

    selected = work[work["_rank"] <= args.max_per_patient].copy()

    if len(selected) > args.max_notes:
        selected = selected.sample(n=args.max_notes, random_state=args.seed)
    elif len(selected) < args.max_notes:
        remaining = work[~work["note_id"].isin(selected["note_id"])]
        need = args.max_notes - len(selected)
        if need > 0 and len(remaining) > 0:
            selected = pd.concat(
                [selected, remaining.sample(n=min(need, len(remaining)), random_state=args.seed)],
                ignore_index=True,
            )

    selected = selected[["person_id", "visit_occurrence_id", "note_id"]].drop_duplicates(subset=["note_id"])
    if len(selected) > args.max_notes:
        selected = selected.sample(n=args.max_notes, random_state=args.seed)

    args.output_manifest.parent.mkdir(parents=True, exist_ok=True)
    selected.to_csv(args.output_manifest, index=False)

    print("selected_manifest_rows", len(selected))
    print("selected_unique_patients", int(selected["person_id"].nunique()))
    print("selected_unique_visits", int(selected["visit_occurrence_id"].nunique()))
    print("manifest", args.output_manifest)

    if not args.no_output_parquet:
        keep_ids = set(selected["note_id"].tolist())
        full_frames = []
        for p in files:
            df = pd.read_parquet(p)
            full_frames.append(df[df["note_id"].isin(keep_ids)])
        out = pd.concat(full_frames, ignore_index=True).drop_duplicates(subset=["note_id"])
        if "note_date" in out.columns:
            out = out.sort_values(["person_id", "visit_occurrence_id", "note_date", "note_id"])
        else:
            out = out.sort_values(["person_id", "visit_occurrence_id", "note_id"])
        args.output_parquet.parent.mkdir(parents=True, exist_ok=True)
        out.to_parquet(args.output_parquet, index=False)
        print("parquet_rows", len(out))
        print("parquet", args.output_parquet)


if __name__ == "__main__":
    main()
