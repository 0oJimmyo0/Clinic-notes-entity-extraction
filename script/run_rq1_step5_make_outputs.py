#!/usr/bin/env python3
"""
Build paper-ready tables and a small set of figures for the layered adjudication-first workflow.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from rq1_adjudication_utils import write_run_summary


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build layered paper outputs for RQ1.")
    p.add_argument("--cohort-summary-json", default="episode_notes/manifests/cohort_justification_summary.json")
    p.add_argument("--note-truth-summary-json", default="episode_extraction_results/rq1/note_truth_eval/rq1_step4_note_truth_summary.json")
    p.add_argument("--adjudicated-mentions-csv", default="episode_extraction_results/rq1/adjudicated/rq1_adjudicated_mentions.csv")
    p.add_argument("--normalization-summary-json", default="episode_extraction_results/rq1/normalization_eval/rq1_normalization_eval_summary.json")
    p.add_argument("--normalization-detailed-csv", default="episode_extraction_results/rq1/normalization_eval/rq1_normalization_eval_detailed.csv")
    p.add_argument("--pathb-calibration-summary-json", default="episode_extraction_results/rq1/pathb_calibration/rq1_pathb_calibration_summary.json")
    p.add_argument("--pathb-confidence-bands-csv", default="episode_extraction_results/rq1/pathb_calibration/rq1_pathb_confidence_bands.csv")
    p.add_argument("--downstream-summary-csv", default="episode_extraction_results/rq1/downstream_concordance/rq1_similarity_summary.csv")
    p.add_argument("--pathb-leftovers-csv", default="episode_extraction_results/rq1/adjudicated/rq1_pathb_leftovers.csv")
    p.add_argument("--status-confusion-csv", default="episode_extraction_results/rq1/note_truth_eval/rq1_step4_status_confusion.csv")
    p.add_argument("--output-dir", default="episode_extraction_results/rq1/paper_outputs")
    return p.parse_args()


def _load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _to_md_table(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "_No rows._\n"
    return df.to_markdown(index=False) + "\n"


def _write_table(df: pd.DataFrame, out_dir: Path, stem: str) -> None:
    df.to_csv(out_dir / f"{stem}.csv", index=False)
    (out_dir / f"{stem}.md").write_text(_to_md_table(df), encoding="utf-8")


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    out_dir = (root / args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    cohort_summary = _load_json((root / args.cohort_summary_json).resolve())
    note_truth_summary = _load_json((root / args.note_truth_summary_json).resolve())
    norm_summary = _load_json((root / args.normalization_summary_json).resolve())
    pathb_summary = _load_json((root / args.pathb_calibration_summary_json).resolve())

    adjud_path = (root / args.adjudicated_mentions_csv).resolve()
    norm_detail_path = (root / args.normalization_detailed_csv).resolve()
    conf_path = (root / args.pathb_confidence_bands_csv).resolve()
    downstream_path = (root / args.downstream_summary_csv).resolve()
    leftovers_path = (root / args.pathb_leftovers_csv).resolve()
    status_conf_path = (root / args.status_confusion_csv).resolve()

    adjud = pd.read_csv(adjud_path).fillna("") if adjud_path.exists() else pd.DataFrame()
    norm_detail = pd.read_csv(norm_detail_path).fillna("") if norm_detail_path.exists() else pd.DataFrame()
    conf_df = pd.read_csv(conf_path).fillna("") if conf_path.exists() else pd.DataFrame()
    downstream = pd.read_csv(downstream_path).fillna("") if downstream_path.exists() else pd.DataFrame()
    leftovers = pd.read_csv(leftovers_path).fillna("") if leftovers_path.exists() else pd.DataFrame()
    status_conf = pd.read_csv(status_conf_path).fillna("") if status_conf_path.exists() else pd.DataFrame()

    cohort_table = pd.DataFrame(
        [
            {
                "cohort": "full_eligible_cohort",
                "patients": cohort_summary.get("full_eligible_patients", 0),
                "visits": cohort_summary.get("full_eligible_visits", 0),
                "notes": cohort_summary.get("notes_after_dedup_text_within_visit", 0),
            },
            {
                "cohort": "downstream_evaluation_cohort",
                "patients": cohort_summary.get("evaluation_patients", 0),
                "visits": cohort_summary.get("evaluation_visits", 0),
                "notes": "",
            },
            {
                "cohort": "adjudication_subset",
                "patients": cohort_summary.get("adjudication_patients", 0),
                "visits": cohort_summary.get("adjudication_visits", 0),
                "notes": "",
            },
        ]
    )

    mention_metrics = (((note_truth_summary or {}).get("metrics") or {}).get("mention_level")) or {}
    extraction_table = pd.DataFrame(
        [
            {
                "tp": mention_metrics.get("tp", 0),
                "fp": mention_metrics.get("fp", 0),
                "fn": mention_metrics.get("fn", 0),
                "precision": mention_metrics.get("precision", 0),
                "recall": mention_metrics.get("recall", 0),
                "f1": mention_metrics.get("f1", 0),
            }
        ]
    )

    if len(adjud):
        status_table = pd.DataFrame(
            {
                "mention_status": adjud["mention_status"].astype(str).str.strip().str.lower().value_counts().index,
                "count": adjud["mention_status"].astype(str).str.strip().str.lower().value_counts().values,
            }
        )
        compare_table = pd.DataFrame(
            {
                "compare_to_structured_ehr": adjud["compare_to_structured_ehr"].astype(str).str.strip().str.lower().value_counts().index,
                "count": adjud["compare_to_structured_ehr"].astype(str).str.strip().str.lower().value_counts().values,
            }
        )
        status_compare_table = pd.concat(
            [status_table.assign(table="mention_status"), compare_table.assign(table="compare_to_structured_ehr")],
            ignore_index=True,
            sort=False,
        )
    else:
        status_compare_table = pd.DataFrame()

    norm_metrics = (norm_summary or {}).get("metrics", {})
    normalization_table = pd.DataFrame(
        [
            {
                "method": "baseline",
                "canonical_match_accuracy": norm_metrics.get("baseline_accuracy", 0),
                "delta_vs_previous": "",
                "pathb_abstention_rate": "",
                "pathb_accepted_link_precision": "",
            },
            {
                "method": "path_a",
                "canonical_match_accuracy": norm_metrics.get("path_a_accuracy", 0),
                "delta_vs_previous": norm_metrics.get("delta_path_a_vs_baseline", 0),
                "pathb_abstention_rate": "",
                "pathb_accepted_link_precision": "",
            },
            {
                "method": "path_b",
                "canonical_match_accuracy": norm_metrics.get("path_b_accuracy", 0),
                "delta_vs_previous": norm_metrics.get("delta_path_b_vs_path_a", 0),
                "pathb_abstention_rate": norm_metrics.get("path_b_abstention_rate", 0),
                "pathb_accepted_link_precision": norm_metrics.get("path_b_accepted_link_precision", 0),
            },
        ]
    )

    if len(downstream):
        d0 = downstream[downstream["window_k"].astype(str) == "0"].copy()
        downstream_table = d0[d0["domain"] == "drugs"][
            [
                "domain",
                "n_pairs",
                "mean_containment_note_in_ehr",
                "mean_containment_note_in_ehr_relaxed",
                "overlap_rate",
                "overlap_rate_relaxed",
                "mean_jaccard",
            ]
        ].copy()
    else:
        downstream_table = pd.DataFrame()

    unresolved_table = (
        leftovers["raw_mention_text"].value_counts().head(25).rename_axis("raw_mention_text").reset_index(name="count")
        if len(leftovers)
        else pd.DataFrame()
    )
    false_links_table = (
        norm_detail[norm_detail["error_bucket"] == "false_link"][
            ["raw_mention_text", "gold_canonical", "pathb_prediction", "pathb_score", "pathb_calibrated_confidence"]
        ]
        .head(25)
        .copy()
        if len(norm_detail)
        else pd.DataFrame()
    )
    status_confusion_table = status_conf.sort_values("count", ascending=False).head(25).copy() if len(status_conf) else pd.DataFrame()

    _write_table(cohort_table, out_dir, "rq1_table_cohort")
    _write_table(extraction_table, out_dir, "rq1_table_extraction_truth")
    _write_table(status_compare_table, out_dir, "rq1_table_status_distribution")
    _write_table(normalization_table, out_dir, "rq1_table_normalization")
    _write_table(downstream_table, out_dir, "rq1_table_downstream_concordance")
    _write_table(unresolved_table, out_dir, "rq1_table_top_unresolved_after_patha")
    _write_table(false_links_table, out_dir, "rq1_table_top_false_links_pathb")
    _write_table(status_confusion_table, out_dir, "rq1_table_top_status_confusion")

    fig_path = out_dir / "rq1_pathb_precision_by_confidence.png"
    if len(conf_df):
        plt.figure(figsize=(7, 4))
        plt.plot(conf_df["band_min"], conf_df["accepted_precision"], marker="o")
        plt.xlabel("Confidence band minimum")
        plt.ylabel("Accepted-link precision")
        plt.title("Path B precision by confidence band")
        plt.tight_layout()
        plt.savefig(fig_path, dpi=150)
        plt.close()

    write_run_summary(
        out_dir / "rq1_step5_make_outputs_summary.json",
        {
            "inputs": {
                "cohort_summary_json": args.cohort_summary_json,
                "note_truth_summary_json": args.note_truth_summary_json,
                "adjudicated_mentions_csv": args.adjudicated_mentions_csv,
                "normalization_summary_json": args.normalization_summary_json,
                "normalization_detailed_csv": args.normalization_detailed_csv,
                "pathb_calibration_summary_json": args.pathb_calibration_summary_json,
                "downstream_summary_csv": args.downstream_summary_csv,
            },
            "outputs_dir": str(out_dir),
            "generated_tables": [
                "rq1_table_cohort",
                "rq1_table_extraction_truth",
                "rq1_table_status_distribution",
                "rq1_table_normalization",
                "rq1_table_downstream_concordance",
                "rq1_table_top_unresolved_after_patha",
                "rq1_table_top_false_links_pathb",
                "rq1_table_top_status_confusion",
            ],
            "generated_figure": str(fig_path) if fig_path.exists() else None,
            "pathb_recommendation": (pathb_summary or {}).get("recommendation", {}),
        },
    )

    print(f"Saved layered paper outputs to: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
