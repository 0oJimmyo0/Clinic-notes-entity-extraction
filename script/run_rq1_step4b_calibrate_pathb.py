#!/usr/bin/env python3
"""
Calibrate Path B against adjudicated normalization truth.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List

import matplotlib.pyplot as plt
import pandas as pd

from rq1_adjudication_utils import write_run_summary


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Calibrate Path B on adjudicated leftovers.")
    p.add_argument(
        "--normalization-detailed-csv",
        default="episode_extraction_results/rq1/normalization_eval/rq1_normalization_eval_detailed.csv",
        help="Detailed normalization evaluation output.",
    )
    p.add_argument("--target-precision", type=float, default=0.90)
    p.add_argument(
        "--output-dir",
        default="episode_extraction_results/rq1/pathb_calibration",
        help="Output directory.",
    )
    return p.parse_args()


def _band_metrics(df: pd.DataFrame, value_col: str, bands: List[tuple[float, float]]) -> pd.DataFrame:
    rows = []
    for lo, hi in bands:
        sub = df[(df[value_col] >= lo) & (df[value_col] <= hi)].copy()
        rows.append(
            {
                "metric": value_col,
                "band_min": lo,
                "band_max": hi,
                "n": int(len(sub)),
                "accepted_n": int(sub["pathb_accepted"].sum()) if len(sub) else 0,
                "accepted_precision": float(sub.loc[sub["pathb_accepted"], "pathb_correct"].mean()) if (sub["pathb_accepted"].sum() > 0) else 0.0,
                "coverage": float(sub["pathb_accepted"].sum()) / float(len(df)) if len(df) else 0.0,
            }
        )
    return pd.DataFrame(rows)


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[2]
    detail_path = (root / args.normalization_detailed_csv).resolve()
    out_dir = (root / args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    if not detail_path.exists():
        raise FileNotFoundError(f"Missing normalization detail CSV: {detail_path}")

    df = pd.read_csv(detail_path).fillna("")
    leftovers = df[(~df["patha_correct"].astype(bool))].copy()
    leftovers["pathb_score"] = leftovers["pathb_score"].astype(float)
    leftovers["pathb_margin"] = leftovers["pathb_margin"].astype(float)
    leftovers["pathb_calibrated_confidence"] = leftovers["pathb_calibrated_confidence"].astype(float)
    leftovers["pathb_accepted"] = leftovers["pathb_accepted"].astype(bool)
    leftovers["pathb_correct"] = leftovers["pathb_correct"].astype(bool)

    score_bands = [(i / 10.0, min((i + 1) / 10.0, 1.0)) for i in range(0, 10)]
    margin_bands = [(0.0, 0.02), (0.02, 0.05), (0.05, 0.10), (0.10, 0.20), (0.20, 1.0)]
    conf_bands = score_bands

    score_df = _band_metrics(leftovers, "pathb_score", score_bands)
    margin_df = _band_metrics(leftovers, "pathb_margin", margin_bands)
    conf_df = _band_metrics(leftovers, "pathb_calibrated_confidence", conf_bands)

    eligible = conf_df[(conf_df["accepted_precision"] >= float(args.target_precision)) & (conf_df["accepted_n"] > 0)].copy()
    recommended = eligible.sort_values(["band_min"]).head(1)
    recommendation = (
        {
            "metric": str(recommended["metric"].iloc[0]),
            "min_value": float(recommended["band_min"].iloc[0]),
            "precision": float(recommended["accepted_precision"].iloc[0]),
            "accepted_n": int(recommended["accepted_n"].iloc[0]),
        }
        if len(recommended)
        else None
    )

    fig_path = out_dir / "rq1_pathb_precision_by_confidence.png"
    if len(conf_df):
        plt.figure(figsize=(7, 4))
        plt.plot(conf_df["band_min"], conf_df["accepted_precision"], marker="o")
        plt.axhline(float(args.target_precision), color="red", linestyle="--", linewidth=1)
        plt.xlabel("Confidence band minimum")
        plt.ylabel("Accepted-link precision")
        plt.title("Path B precision by confidence band")
        plt.tight_layout()
        plt.savefig(fig_path, dpi=150)
        plt.close()

    score_path = out_dir / "rq1_pathb_score_bands.csv"
    margin_path = out_dir / "rq1_pathb_margin_bands.csv"
    conf_path = out_dir / "rq1_pathb_confidence_bands.csv"
    summary_path = out_dir / "rq1_pathb_calibration_summary.json"
    score_df.to_csv(score_path, index=False)
    margin_df.to_csv(margin_path, index=False)
    conf_df.to_csv(conf_path, index=False)

    write_run_summary(
        summary_path,
        {
            "inputs": {"normalization_detailed_csv": str(detail_path)},
            "target_precision": float(args.target_precision),
            "leftover_mentions_n": int(len(leftovers)),
            "recommendation": recommendation,
            "outputs": {
                "score_bands_csv": str(score_path),
                "margin_bands_csv": str(margin_path),
                "confidence_bands_csv": str(conf_path),
                "precision_figure_png": str(fig_path) if fig_path.exists() else None,
            },
        },
    )

    print(f"Saved Path B calibration summary: {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
