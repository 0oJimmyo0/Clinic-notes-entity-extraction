#!/usr/bin/env python3
"""
Create a random/stratified sample of currently unaudited reference rows for future manual reliability review.

This script prepares a review packet. It does not estimate correctness by itself because
manual review labels are not available in the current repo state.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from rq1_adjudication_utils import write_run_summary
from rq1_bibm_utils import action_cue


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sample unaudited reference rows for optional manual reliability review.")
    p.add_argument(
        "--final-reviewed-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/adjudicated/reviewed_adjudication_patched.csv",
    )
    p.add_argument(
        "--audit-review-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/adjudicated/strict_pathb_review_queue_completed_final.csv",
    )
    p.add_argument(
        "--packets-mentions-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/adjudication_packets/adjudication_packets_mentions.csv",
    )
    p.add_argument("--sample-size", type=int, default=300)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument(
        "--sampling-mode",
        choices=["random", "stratified"],
        default="stratified",
    )
    p.add_argument(
        "--output-dir",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/reference_random_audit_sample",
    )
    return p.parse_args()


def _normalize(df: pd.DataFrame, col: str) -> None:
    if col in df.columns:
        df[col] = df[col].fillna("").astype(str).str.strip()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    final_path = (root / args.final_reviewed_csv).resolve()
    audit_path = (root / args.audit_review_csv).resolve()
    packets_path = (root / args.packets_mentions_csv).resolve()
    out_dir = (root / args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    final = pd.read_csv(final_path).fillna("")
    audit = pd.read_csv(audit_path).fillna("")
    packets = pd.read_csv(packets_path).fillna("")
    for df in [final, audit, packets]:
        for col in ["adjudication_unit_id", "note_id", "person_id", "visit_id"]:
            _normalize(df, col)

    audited_ids = set(audit["adjudication_unit_id"].astype(str))
    sample_pool = final[~final["adjudication_unit_id"].astype(str).isin(audited_ids)].copy()

    packets_small = packets[
        [
            c
            for c in [
                "adjudication_unit_id",
                "note_title",
                "candidate_category",
                "seed_treatment_action",
                "seed_discontinuation_reason",
                "seed_certainty",
            ]
            if c in packets.columns
        ]
    ].copy()
    sample_pool = sample_pool.merge(packets_small, on="adjudication_unit_id", how="left")
    sample_pool["action_cue"] = sample_pool.apply(
        lambda r: action_cue(
            str(r.get("seed_treatment_action", "")),
            str(r.get("seed_discontinuation_reason", "")),
            str(r.get("context_text", "")),
        ),
        axis=1,
    )

    if args.sampling_mode == "random" or len(sample_pool) <= args.sample_size:
        sample_df = sample_pool.sample(n=min(args.sample_size, len(sample_pool)), random_state=args.seed)
    else:
        sample_pool["_strata"] = (
            sample_pool.get("note_title", "").astype(str).str.slice(0, 40)
            + "||"
            + sample_pool.get("candidate_category", "").astype(str)
            + "||"
            + sample_pool["action_cue"].astype(str)
        )
        counts = sample_pool["_strata"].value_counts(dropna=False)
        alloc = ((counts / counts.sum()) * args.sample_size).round().astype(int)
        alloc[alloc == 0] = 1
        parts = []
        for key, n_take in alloc.items():
            part = sample_pool[sample_pool["_strata"] == key]
            parts.append(part.sample(n=min(len(part), n_take), random_state=args.seed))
        sample_df = pd.concat(parts, ignore_index=True).drop_duplicates("adjudication_unit_id")
        if len(sample_df) > args.sample_size:
            sample_df = sample_df.sample(n=args.sample_size, random_state=args.seed)

    sample_df["manual_audit_canonical_label"] = ""
    sample_df["manual_audit_status"] = ""
    sample_df["manual_audit_compare_to_ehr"] = ""
    sample_df["manual_audit_notes"] = ""

    out_csv = out_dir / "rq1_reference_random_audit_sample.csv"
    sample_df.to_csv(out_csv, index=False)
    write_run_summary(
        out_dir / "rq1_reference_random_audit_sample_summary.json",
        {
            "inputs": {
                "final_reviewed_csv": str(final_path),
                "audit_review_csv": str(audit_path),
                "packets_mentions_csv": str(packets_path),
                "sampling_mode": args.sampling_mode,
                "sample_size_requested": int(args.sample_size),
                "seed": int(args.seed),
            },
            "counts": {
                "unaudited_pool_rows": int(len(sample_pool)),
                "sample_rows": int(len(sample_df)),
            },
            "outputs": {"sample_csv": str(out_csv)},
            "note": "Manual review required before label correctness / CI can be reported.",
        },
    )
    print(f"Saved random audit sample to: {out_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
