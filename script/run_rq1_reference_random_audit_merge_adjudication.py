#!/usr/bin/env python3
"""
Merge manually adjudicated disagreement rows back into the consensus-ready random-audit file.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Merge adjudicated disagreement packet into consensus-ready audit CSV.")
    p.add_argument(
        "--consensus-ready-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/reference_random_audit_consensus_prep_n1000/rq1_reference_random_audit_consensus_ready.csv",
    )
    p.add_argument(
        "--disagreement-adjudicated-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/reference_random_audit_consensus_prep_n1000/rq1_reference_random_audit_disagreement_packet.csv",
    )
    p.add_argument(
        "--output-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/reference_random_audit_consensus_prep_n1000/rq1_reference_random_audit_final_adjudicated.csv",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    consensus_path = (root / args.consensus_ready_csv).resolve()
    disagreement_path = (root / args.disagreement_adjudicated_csv).resolve()
    output_path = (root / args.output_csv).resolve()

    consensus = pd.read_csv(consensus_path).fillna("")
    disagreement = pd.read_csv(disagreement_path).fillna("")

    adjud_cols = [
        "adjudicated_medication_valid",
        "adjudicated_span_valid",
        "adjudicated_canonical_correct",
        "adjudicated_corrected_canonical_label",
        "adjudicated_action_correct",
        "adjudicated_error_category",
        "adjudicated_confidence",
    ]
    merge_cols = ["row_id"] + [c for c in adjud_cols if c in disagreement.columns]
    disagreement_small = disagreement[merge_cols].drop_duplicates("row_id")

    merged = consensus.merge(disagreement_small, on="row_id", how="left", suffixes=("", "_new"))
    for c in adjud_cols:
        new_c = f"{c}_new"
        if new_c in merged.columns:
            nonblank_new = merged[new_c].fillna("").astype(str).str.strip() != ""
            merged[c] = merged[new_c].where(nonblank_new, merged[c])
    merged = merged.drop(columns=[c for c in merged.columns if c.endswith("_new")], errors="ignore")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(output_path, index=False)
    print(f"Saved merged adjudicated audit CSV to: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
