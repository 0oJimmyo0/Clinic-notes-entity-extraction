#!/usr/bin/env python3
"""
Compute Path B leftover-specific metrics from normalization detail output.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from rq1_adjudication_utils import write_run_summary


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Compute Path B leftover-specific metrics.")
    p.add_argument(
        "--normalization-detailed-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/normalization_eval/rq1_normalization_eval_detailed.csv",
    )
    p.add_argument(
        "--output-json",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/normalization_eval/rq1_pathb_leftover_metrics.json",
    )
    return p.parse_args()


def _safe_rate(num: int, den: int) -> float:
    return float(num) / float(den) if den else 0.0


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    detail_path = (root / args.normalization_detailed_csv).resolve()
    out_path = (root / args.output_json).resolve()

    if not detail_path.exists():
        raise FileNotFoundError(f"Missing normalization detailed CSV: {detail_path}")

    df = pd.read_csv(detail_path).fillna("")
    required = {"patha_correct", "pathb_accepted", "pathb_correct"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Normalization detailed CSV missing columns: {sorted(missing)}")

    patha_correct = df["patha_correct"].astype(bool)
    pathb_accepted = df["pathb_accepted"].astype(bool)
    pathb_correct = df["pathb_correct"].astype(bool)

    leftover_mask = ~patha_correct
    leftover = df[leftover_mask].copy()
    accepted_leftover = leftover[leftover["pathb_accepted"].astype(bool)].copy()

    patha_leftover_n = int(len(leftover))
    pathb_leftover_accepted_n = int(len(accepted_leftover))
    pathb_leftover_recovered_n = int(accepted_leftover["pathb_correct"].astype(bool).sum())
    pathb_leftover_accepted_precision = (
        float(accepted_leftover["pathb_correct"].astype(bool).mean()) if pathb_leftover_accepted_n else 0.0
    )
    pathb_leftover_recovery_rate = _safe_rate(pathb_leftover_recovered_n, patha_leftover_n)
    pathb_leftover_abstention_rate = _safe_rate(
        int((~leftover["pathb_accepted"].astype(bool)).sum()),
        patha_leftover_n,
    )

    payload = {
        "inputs": {
            "normalization_detailed_csv": str(detail_path),
        },
        "metrics": {
            "patha_leftover_n": patha_leftover_n,
            "pathb_leftover_accepted_n": pathb_leftover_accepted_n,
            "pathb_leftover_accepted_precision": round(pathb_leftover_accepted_precision, 6),
            "pathb_leftover_recovered_n": pathb_leftover_recovered_n,
            "pathb_leftover_recovery_rate": round(pathb_leftover_recovery_rate, 6),
            "pathb_leftover_abstention_rate": round(pathb_leftover_abstention_rate, 6),
        },
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    write_run_summary(out_path, payload)
    print(f"Saved Path B leftover metrics: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
