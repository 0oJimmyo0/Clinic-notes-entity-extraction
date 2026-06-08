#!/usr/bin/env python3
"""
Summarize candidate alias refinements for a post-hoc Path A v2 sensitivity table.

This does not change Path A v1. It ranks candidate alias targets from:
- existing Path A missing-alias failure examples
- unmapped note labels from OMOP/RxNorm mapping artifacts
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from rq1_adjudication_utils import write_run_summary
from rq1_drug_linking import normalize_drug_text


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build alias refinement summary for Path A v2 sensitivity.")
    p.add_argument(
        "--note-detailed-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/note_only_evidence_bibm_test/rq1_note_only_evidence_detailed.csv",
    )
    p.add_argument(
        "--patha-failure-examples-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/paper_outputs_patha/rq1_table_patha_failure_taxonomy_examples.csv",
    )
    p.add_argument(
        "--unmapped-note-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/omop_rxnorm_mapping/rq1_unmapped_note_labels.csv",
    )
    p.add_argument(
        "--output-dir",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/alias_refinement_summary",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    note_path = (root / args.note_detailed_csv).resolve()
    failure_path = (root / args.patha_failure_examples_csv).resolve()
    unmapped_path = (root / args.unmapped_note_csv).resolve()
    out_dir = (root / args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    note_df = pd.read_csv(note_path, usecols=["adjudicated_canonical_label"]).fillna("")
    note_df["note_label_norm"] = note_df["adjudicated_canonical_label"].map(normalize_drug_text)
    label_freq = (
        note_df[note_df["note_label_norm"] != ""]
        .groupby("note_label_norm", as_index=False)
        .size()
        .rename(columns={"size": "mention_count", "note_label_norm": "candidate_alias_norm"})
    )

    failure = pd.read_csv(failure_path).fillna("")
    failure = failure[failure["failure_category"] == "missing alias"].copy()
    failure["candidate_alias_norm"] = failure["example_raw_mention_text"].map(normalize_drug_text)
    failure = failure.rename(columns={"most_common_gold_canonical": "suggested_canonical_label", "example_count": "patha_failure_example_count"})

    unmapped = pd.read_csv(unmapped_path).fillna("")
    unmapped["candidate_alias_norm"] = unmapped["note_label"].map(normalize_drug_text)
    unmapped = unmapped[unmapped["candidate_alias_norm"] != ""].copy()
    unmapped = unmapped[["candidate_alias_norm", "note_label"]].drop_duplicates()

    merged = failure.merge(label_freq, on="candidate_alias_norm", how="left")
    merged = merged.merge(unmapped[["candidate_alias_norm"]].assign(also_unmapped_in_omop=1), on="candidate_alias_norm", how="left")
    merged["also_unmapped_in_omop"] = merged["also_unmapped_in_omop"].fillna(0).astype(int)
    merged["mention_count"] = merged["mention_count"].fillna(0).astype(int)
    merged["review_recommendation"] = "candidate_patha_v2_alias_review"
    merged = merged.sort_values(["patha_failure_example_count", "mention_count"], ascending=[False, False])

    oracle = (
        merged.groupby("suggested_canonical_label", as_index=False)
        .agg(
            candidate_alias_count=("candidate_alias_norm", "nunique"),
            summed_patha_failure_examples=("patha_failure_example_count", "sum"),
            summed_note_mentions=("mention_count", "sum"),
        )
        .sort_values(["summed_patha_failure_examples", "summed_note_mentions"], ascending=[False, False])
    )

    outputs = {
        "candidate_table_csv": out_dir / "rq1_alias_refinement_candidates.csv",
        "oracle_table_csv": out_dir / "rq1_alias_refinement_oracle_summary.csv",
        "summary_json": out_dir / "rq1_alias_refinement_summary.json",
    }
    merged.to_csv(outputs["candidate_table_csv"], index=False)
    oracle.to_csv(outputs["oracle_table_csv"], index=False)

    write_run_summary(
        outputs["summary_json"],
        {
            "inputs": {
                "note_detailed_csv": str(note_path),
                "patha_failure_examples_csv": str(failure_path),
                "unmapped_note_csv": str(unmapped_path),
            },
            "counts": {
                "missing_alias_candidates": int(len(merged)),
                "canonical_targets": int(len(oracle)),
            },
            "outputs": {k: str(v) for k, v in outputs.items()},
        },
    )

    print(f"Saved alias refinement summary to: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
