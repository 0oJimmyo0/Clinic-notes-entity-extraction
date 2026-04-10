#!/usr/bin/env python3
"""
Step 6 downstream concordance: compare adjudicated comparable note drugs to structured EHR.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from rq1_adjudication_utils import write_run_summary
from rq1_concordance_utils import (
    build_windowed_ehr,
    compute_domain_similarity,
    infer_timeline_from_visit_id,
    normalize_id,
    parse_list_cell,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run downstream concordance on adjudicated comparable note drugs.")
    p.add_argument(
        "--downstream-comparable-mentions-csv",
        default="episode_extraction_results/rq1/adjudicated/rq1_downstream_comparable_mentions.csv",
        help="Comparable adjudicated mention rows from run_join_adjudication_labels.py",
    )
    p.add_argument(
        "--ehr-csv",
        default="episode_extraction_results/rq1/rq1_ehr_entities_by_visit.csv",
        help="Visit-level structured EHR entities CSV",
    )
    p.add_argument(
        "--timeline-csv",
        default="episode_extraction_results/rq1/rq1_visit_timeline.csv",
        help="Optional visit timeline CSV (required when window size > 0 unless using fallback).",
    )
    p.add_argument(
        "--timeline-fallback",
        choices=["error", "infer_visit_id"],
        default="error",
        help="When window size > 0 and timeline is missing: stop or infer order by visit_id.",
    )
    p.add_argument(
        "--windows",
        default="0",
        help='Comma-separated window sizes, e.g. "0" or "0,1,2".',
    )
    p.add_argument(
        "--method-label",
        default="downstream_adjudicated_comparable",
        help="Method label saved to concordance outputs.",
    )
    p.add_argument(
        "--output-dir",
        default="episode_extraction_results/rq1/downstream_concordance",
        help="Output directory.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    mentions_path = (root / args.downstream_comparable_mentions_csv).resolve()
    ehr_path = (root / args.ehr_csv).resolve()
    timeline_path = (root / args.timeline_csv).resolve()
    out_dir = (root / args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    if not mentions_path.exists():
        raise FileNotFoundError(f"Missing downstream comparable mentions CSV: {mentions_path}")
    if not ehr_path.exists():
        raise FileNotFoundError(f"Missing EHR CSV: {ehr_path}")

    mentions = pd.read_csv(mentions_path).fillna("")
    windows = sorted({int(x.strip()) for x in args.windows.split(",") if x.strip() != ""})
    if any(k < 0 for k in windows):
        raise ValueError("window sizes must be >= 0")

    required = {"person_id", "visit_id", "adjudicated_canonical_label"}
    missing = required - set(mentions.columns)
    if missing:
        raise ValueError(f"Comparable mentions CSV missing columns: {sorted(missing)}")

    note_visit = (
        mentions[mentions["adjudicated_canonical_label"].astype(str).str.strip() != ""]
        .groupby(["person_id", "visit_id"], as_index=False)["adjudicated_canonical_label"]
        .agg(lambda x: sorted(set(str(v).strip().lower() for v in x if str(v).strip())))
        .rename(columns={"adjudicated_canonical_label": "drugs"})
    )
    note_visit["person_id"] = normalize_id(note_visit["person_id"])
    note_visit["visit_id"] = normalize_id(note_visit["visit_id"])

    ehr = pd.read_csv(ehr_path).fillna("")
    if "person_id" not in ehr.columns or "visit_id" not in ehr.columns:
        raise ValueError("EHR CSV must include person_id and visit_id columns")
    ehr["person_id"] = normalize_id(ehr["person_id"])
    ehr["visit_id"] = normalize_id(ehr["visit_id"])
    if "drugs" not in ehr.columns:
        ehr["drugs"] = "[]"
    ehr["drugs"] = ehr["drugs"].apply(parse_list_cell)

    note_path = out_dir / "rq1_downstream_note_entities_by_visit.csv"
    summary_path = out_dir / "rq1_step6_downstream_summary.json"
    summary_csv = out_dir / "rq1_similarity_summary.csv"
    pairs_csv = out_dir / "rq1_similarity_pairs.csv"

    note_write = note_visit.copy()
    note_write["drugs"] = note_write["drugs"].map(lambda xs: json.dumps(xs, ensure_ascii=False))
    note_write.to_csv(note_path, index=False)

    timeline_df = None
    if max(windows) > 0:
        if timeline_path.exists():
            timeline_df = pd.read_csv(timeline_path)
            need_cols = {"person_id", "visit_id", "visit_start_date"}
            if not need_cols.issubset(set(timeline_df.columns)):
                raise ValueError(f"timeline-csv must include {sorted(need_cols)}")
        elif args.timeline_fallback == "infer_visit_id":
            timeline_df = infer_timeline_from_visit_id(note_df=note_visit, ehr_df=ehr)
        else:
            raise FileNotFoundError(
                "k>0 requested but timeline file missing. Provide --timeline-csv with "
                "person_id, visit_id, visit_start_date or set --timeline-fallback infer_visit_id."
            )

    all_summary = []
    all_pairs = []
    for k in windows:
        if k == 0:
            ehr_k = ehr.copy()
        else:
            ehr_k = build_windowed_ehr(ehr_df=ehr, timeline_df=timeline_df, k=k, domains=["drugs"])

        summary_k, pairs_k = compute_domain_similarity(
            note_df=note_visit,
            ehr_df=ehr_k,
            domain="drugs",
            window_k=k,
            method_label=str(args.method_label),
        )
        all_summary.append(summary_k)
        all_pairs.append(pairs_k)

    summary_df = pd.concat(all_summary, ignore_index=True) if all_summary else pd.DataFrame()
    pairs_df = pd.concat(all_pairs, ignore_index=True) if all_pairs else pd.DataFrame()
    summary_df.to_csv(summary_csv, index=False)
    pairs_df.to_csv(pairs_csv, index=False)

    write_run_summary(
        summary_path,
        {
            "inputs": {
                "downstream_comparable_mentions_csv": str(mentions_path),
                "ehr_csv": str(ehr_path),
                "timeline_csv": str(timeline_path) if timeline_path.exists() else None,
                "windows": windows,
            },
            "counts": {
                "comparable_mentions": int(len(mentions)),
                "note_visits": int(len(note_visit)),
                "aligned_visit_pairs": int(len(pairs_df)),
            },
            "outputs": {
                "note_visit_csv": str(note_path),
                "summary_csv": str(summary_csv),
                "pairs_csv": str(pairs_csv),
            },
        },
    )

    print(f"Saved downstream concordance note-visit file: {note_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
