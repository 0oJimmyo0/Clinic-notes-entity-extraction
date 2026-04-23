#!/usr/bin/env python3
"""
Build strict Path B diagnostic slice from normalization detail output.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from rq1_adjudication_utils import write_run_summary


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build strict Path B diagnostic slice.")
    p.add_argument(
        "--normalization-detailed-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/normalization_eval/rq1_normalization_eval_detailed.csv",
    )
    p.add_argument(
        "--output-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/normalization_eval/rq1_strict_pathb_diagnostic_slice.csv",
    )
    p.add_argument(
        "--summary-json",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/normalization_eval/rq1_strict_pathb_slice_summary.json",
    )
    return p.parse_args()


def _vc(series: pd.Series) -> dict:
    s = series.fillna("").astype(str).str.strip().replace({"": "<EMPTY>"})
    vc = s.value_counts(dropna=False)
    return {str(k): int(v) for k, v in vc.items()}


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    detail_path = (root / args.normalization_detailed_csv).resolve()
    out_path = (root / args.output_csv).resolve()
    summary_path = (root / args.summary_json).resolve()

    if not detail_path.exists():
        raise FileNotFoundError(f"Missing normalization detail CSV: {detail_path}")

    df = pd.read_csv(detail_path).fillna("")
    required = ["patha_correct", "pathb_stage", "adjudication_unit_id"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Normalization detail missing required columns: {missing}")

    df["patha_correct"] = df["patha_correct"].astype(bool)
    df["pathb_stage"] = df["pathb_stage"].astype(str).str.strip()

    strict = df[(~df["patha_correct"]) & (df["pathb_stage"] != "path_a_exact_vocab")].copy()

    # Keep requested diagnostic columns.
    if "current_error_bucket" in strict.columns:
        strict["current_error_bucket"] = strict["current_error_bucket"].astype(str)
    elif "error_bucket" in strict.columns:
        strict["current_error_bucket"] = strict["error_bucket"].astype(str)
    else:
        strict["current_error_bucket"] = ""

    requested_cols = [
        "adjudication_unit_id",
        "person_id",
        "visit_id",
        "note_id",
        "raw_mention_text",
        "adjudicated_canonical_label",
        "mention_status",
        "compare_to_structured_ehr",
        "pathb_stage",
        "pathb_prediction",
        "pathb_score",
        "pathb_margin",
        "pathb_calibrated_confidence",
        "pathb_reason_codes_json",
        "pathb_top_k_candidates_json",
        "current_error_bucket",
    ]

    # Compatibility with existing detailed schema that stores gold label as gold_canonical.
    if "adjudicated_canonical_label" not in strict.columns and "gold_canonical" in strict.columns:
        strict["adjudicated_canonical_label"] = strict["gold_canonical"]

    for c in requested_cols:
        if c not in strict.columns:
            strict[c] = ""
    strict = strict[requested_cols].copy()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    strict.to_csv(out_path, index=False)

    write_run_summary(
        summary_path,
        {
            "inputs": {
                "normalization_detailed_csv": str(detail_path),
            },
            "counts": {
                "detailed_n": int(len(df)),
                "patha_leftover_n": int((~df["patha_correct"]).sum()),
                "strict_slice_n": int(len(strict)),
            },
            "stratified_counts": {
                "mention_status": _vc(strict["mention_status"]),
                "compare_to_structured_ehr": _vc(strict["compare_to_structured_ehr"]),
                "pathb_stage": _vc(strict["pathb_stage"]),
                "current_error_bucket": _vc(strict["current_error_bucket"]),
            },
            "outputs": {
                "strict_slice_csv": str(out_path),
                "strict_slice_summary_json": str(summary_path),
            },
        },
    )

    print(f"Saved strict Path B slice: {out_path}")
    print(f"Rows: {len(strict):,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
