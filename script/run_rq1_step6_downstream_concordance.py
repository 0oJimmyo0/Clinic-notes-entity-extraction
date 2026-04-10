#!/usr/bin/env python3
"""
Step 6 downstream concordance: compare adjudicated comparable note drugs to structured EHR.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

import pandas as pd

from rq1_adjudication_utils import write_run_summary


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
        help="Visit timeline CSV",
    )
    p.add_argument(
        "--method-label",
        default="downstream_adjudicated_comparable",
        help="Method label forwarded to legacy concordance script.",
    )
    p.add_argument(
        "--output-dir",
        default="episode_extraction_results/rq1/downstream_concordance",
        help="Output directory.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[2]
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
    required = {"person_id", "visit_id", "adjudicated_canonical_label"}
    missing = required - set(mentions.columns)
    if missing:
        raise ValueError(f"Comparable mentions CSV missing columns: {sorted(missing)}")

    note_visit = (
        mentions[mentions["adjudicated_canonical_label"].astype(str).str.strip() != ""]
        .groupby(["person_id", "visit_id"], as_index=False)["adjudicated_canonical_label"]
        .agg(lambda x: json.dumps(sorted(set(str(v).strip().lower() for v in x if str(v).strip()))))
        .rename(columns={"adjudicated_canonical_label": "drugs"})
    )
    for col in ["conditions", "measurements", "procedures"]:
        note_visit[col] = "[]"

    note_path = out_dir / "rq1_downstream_note_entities_by_visit.csv"
    summary_path = out_dir / "rq1_step6_downstream_summary.json"
    summary_csv = out_dir / "rq1_similarity_summary.csv"
    pairs_csv = out_dir / "rq1_similarity_pairs.csv"
    note_visit.to_csv(note_path, index=False)

    step4_script = root / "resources/script/run_rq1_step4_similarity.py"
    cmd = [
        sys.executable,
        str(step4_script),
        "--note-csv",
        str(note_path),
        "--ehr-csv",
        str(ehr_path),
        "--timeline-csv",
        str(timeline_path),
        "--method-label",
        str(args.method_label),
        "--output-summary-csv",
        str(summary_csv),
        "--output-pairs-csv",
        str(pairs_csv),
    ]
    subprocess.run(cmd, check=True, cwd=root)

    write_run_summary(
        summary_path,
        {
            "inputs": {
                "downstream_comparable_mentions_csv": str(mentions_path),
                "ehr_csv": str(ehr_path),
                "timeline_csv": str(timeline_path) if timeline_path.exists() else None,
            },
            "counts": {
                "comparable_mentions": int(len(mentions)),
                "note_visits": int(len(note_visit)),
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
