#!/usr/bin/env python3
"""
Build paper-ready tables and a small set of figures for the layered adjudication-first workflow.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

try:
    import matplotlib.pyplot as plt
except Exception:  # pragma: no cover - optional plotting dependency
    plt = None
import pandas as pd

from rq1_adjudication_utils import write_run_summary


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build layered paper outputs for RQ1.")
    p.add_argument("--cohort-summary-json", default="episode_notes/manifests/cohort_justification_summary.json")
    p.add_argument("--note-truth-summary-json", default="episode_extraction_results/rq1/note_truth_eval/rq1_step4_note_truth_summary.json")
    p.add_argument("--adjudicated-mentions-csv", default="episode_extraction_results/rq1/adjudicated/rq1_adjudicated_mentions.csv")
    p.add_argument("--normalization-summary-json", default="episode_extraction_results/rq1/normalization_eval/rq1_normalization_eval_summary.json")
    p.add_argument("--normalization-detailed-csv", default="episode_extraction_results/rq1/normalization_eval/rq1_normalization_eval_detailed.csv")
    p.add_argument("--normalization-error-buckets-csv", default="episode_extraction_results/rq1/normalization_eval/rq1_normalization_error_buckets.csv")
    p.add_argument("--pathb-candidate-audit-csv", default="episode_extraction_results/rq1/normalization_eval/rq1_pathb_candidate_audit.csv")
    p.add_argument("--pathb-calibration-summary-json", default="episode_extraction_results/rq1/pathb_calibration/rq1_pathb_calibration_summary.json")
    p.add_argument("--pathb-confidence-bands-csv", default="episode_extraction_results/rq1/pathb_calibration/rq1_pathb_confidence_bands.csv")
    p.add_argument("--downstream-summary-csv", default="episode_extraction_results/rq1/downstream_concordance/rq1_similarity_summary.csv")
    p.add_argument("--downstream-ablation-summary-csv", default="episode_extraction_results/rq1/downstream_concordance/rq1_similarity_summary_method_ablation.csv")
    p.add_argument("--downstream-ablation-status-csv", default="episode_extraction_results/rq1/downstream_concordance/rq1_similarity_status_stratified_method_ablation.csv")
    p.add_argument("--pathb-leftovers-csv", default="episode_extraction_results/rq1/adjudicated/rq1_pathb_leftovers.csv")
    p.add_argument("--status-confusion-csv", default="episode_extraction_results/rq1/note_truth_eval/rq1_step4_status_confusion.csv")
    p.add_argument(
        "--cohort-scope-label",
        default="clinic_only",
        help="Label prefix describing cohort scope used in cohort table outputs.",
    )
    p.add_argument("--output-dir", default="episode_extraction_results/rq1/paper_outputs")
    return p.parse_args()


def _load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _to_md_table(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "_No rows._\n"
    try:
        return df.to_markdown(index=False) + "\n"
    except Exception:
        cols = list(df.columns)
        header = "| " + " | ".join(str(c) for c in cols) + " |"
        sep = "| " + " | ".join(["---"] * len(cols)) + " |"
        rows = []
        for _, r in df.iterrows():
            rows.append("| " + " | ".join(str(r.get(c, "")) for c in cols) + " |")
        return "\n".join([header, sep, *rows]) + "\n"


def _write_table(df: pd.DataFrame, out_dir: Path, stem: str) -> None:
    df.to_csv(out_dir / f"{stem}.csv", index=False)
    (out_dir / f"{stem}.md").write_text(_to_md_table(df), encoding="utf-8")


def _build_workflow_figure(path: Path) -> None:
    if plt is None:
        return
    fig, ax = plt.subplots(figsize=(12, 3.8))
    ax.axis("off")

    boxes = [
        (0.02, "Clinic Notes\nExtraction"),
        (0.24, "LLM+Human\nAdjudication"),
        (0.46, "Baseline / Path A / Path B\nNormalization"),
        (0.70, "Note-grounded\nEvaluation"),
        (0.88, "Downstream EHR\nVerification"),
    ]

    for x, label in boxes:
        ax.text(
            x,
            0.5,
            label,
            ha="center",
            va="center",
            fontsize=10,
            bbox={"boxstyle": "round,pad=0.45", "facecolor": "#f7f7f7", "edgecolor": "#333333", "linewidth": 1.0},
            transform=ax.transAxes,
        )

    for x0, x1 in [(0.11, 0.18), (0.33, 0.40), (0.58, 0.64), (0.79, 0.84)]:
        ax.annotate("", xy=(x1, 0.5), xytext=(x0, 0.5), xycoords="axes fraction", textcoords="axes fraction", arrowprops={"arrowstyle": "->", "lw": 1.6})

    fig.tight_layout()
    fig.savefig(path, dpi=180)
    plt.close(fig)


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
    norm_bucket_path = (root / args.normalization_error_buckets_csv).resolve()
    pathb_audit_path = (root / args.pathb_candidate_audit_csv).resolve()
    conf_path = (root / args.pathb_confidence_bands_csv).resolve()
    downstream_path = (root / args.downstream_summary_csv).resolve()
    downstream_ablation_path = (root / args.downstream_ablation_summary_csv).resolve()
    downstream_ablation_status_path = (root / args.downstream_ablation_status_csv).resolve()
    leftovers_path = (root / args.pathb_leftovers_csv).resolve()
    status_conf_path = (root / args.status_confusion_csv).resolve()

    adjud = pd.read_csv(adjud_path).fillna("") if adjud_path.exists() else pd.DataFrame()
    norm_detail = pd.read_csv(norm_detail_path).fillna("") if norm_detail_path.exists() else pd.DataFrame()
    norm_buckets = pd.read_csv(norm_bucket_path).fillna("") if norm_bucket_path.exists() else pd.DataFrame()
    pathb_audit = pd.read_csv(pathb_audit_path).fillna("") if pathb_audit_path.exists() else pd.DataFrame()
    conf_df = pd.read_csv(conf_path).fillna("") if conf_path.exists() else pd.DataFrame()
    downstream = pd.read_csv(downstream_path).fillna("") if downstream_path.exists() else pd.DataFrame()
    downstream_ablation = pd.read_csv(downstream_ablation_path).fillna("") if downstream_ablation_path.exists() else pd.DataFrame()
    downstream_ablation_status = pd.read_csv(downstream_ablation_status_path).fillna("") if downstream_ablation_status_path.exists() else pd.DataFrame()
    leftovers = pd.read_csv(leftovers_path).fillna("") if leftovers_path.exists() else pd.DataFrame()
    status_conf = pd.read_csv(status_conf_path).fillna("") if status_conf_path.exists() else pd.DataFrame()

    cohort_table = pd.DataFrame(
        [
            {
                "cohort": f"{args.cohort_scope_label}_full_eligible_cohort",
                "patients": cohort_summary.get("full_eligible_patients", 0),
                "visits": cohort_summary.get("full_eligible_visits", 0),
                "notes": cohort_summary.get("notes_after_dedup_text_within_visit", 0),
            },
            {
                "cohort": f"{args.cohort_scope_label}_downstream_evaluation_cohort",
                "patients": cohort_summary.get("evaluation_patients", 0),
                "visits": cohort_summary.get("evaluation_visits", 0),
                "notes": "",
            },
            {
                "cohort": f"{args.cohort_scope_label}_adjudication_subset",
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

    if len(downstream_ablation):
        dab0 = downstream_ablation[
            (downstream_ablation["window_k"].astype(str) == "0")
            & (downstream_ablation["domain"].astype(str).str.lower() == "drugs")
        ].copy()
        downstream_ablation_table = dab0[
            [
                "method_label",
                "n_pairs",
                "mean_containment_note_in_ehr",
                "mean_containment_note_in_ehr_relaxed",
                "overlap_rate",
                "overlap_rate_relaxed",
                "mean_jaccard",
            ]
        ].copy()
    else:
        downstream_ablation_table = pd.DataFrame()

    if len(downstream_ablation_status):
        keep_cols = [
            c
            for c in [
                "method_label",
                "mention_status",
                "n_pairs",
                "mean_containment_note_in_ehr",
                "mean_containment_note_in_ehr_relaxed",
                "overlap_rate_relaxed",
                "mean_jaccard",
            ]
            if c in downstream_ablation_status.columns
        ]
        downstream_status_table = downstream_ablation_status[keep_cols].copy() if keep_cols else pd.DataFrame()
    else:
        downstream_status_table = pd.DataFrame()

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

    if len(norm_buckets):
        error_taxonomy_table = norm_buckets.copy()
    elif len(norm_detail):
        error_taxonomy_table = (
            norm_detail.groupby("error_bucket", as_index=False)
            .size()
            .rename(columns={"size": "count", "error_bucket": "error_bucket"})
            .sort_values("count", ascending=False)
        )
    else:
        error_taxonomy_table = pd.DataFrame()

    if len(error_taxonomy_table):
        total_err = float(error_taxonomy_table["count"].sum())
        error_taxonomy_table["share"] = error_taxonomy_table["count"].map(lambda x: round(float(x) / total_err, 6) if total_err else 0.0)

    abstention_table = (
        norm_detail[norm_detail["error_bucket"] == "pathb_abstained"]
        .groupby("raw_mention_text", as_index=False)
        .size()
        .rename(columns={"size": "count"})
        .sort_values("count", ascending=False)
        .head(25)
        if len(norm_detail)
        else pd.DataFrame()
    )

    accepted_audit_table = (
        pathb_audit[pathb_audit["pathb_accepted"].astype(str).str.lower().isin({"true", "1"})]
        .sort_values(["pathb_calibrated_confidence", "pathb_score"], ascending=[False, False])
        .head(50)
        if len(pathb_audit)
        else pd.DataFrame()
    )
    rejected_audit_table = (
        pathb_audit[~pathb_audit["pathb_accepted"].astype(str).str.lower().isin({"true", "1"})]
        .sort_values(["pathb_calibrated_confidence", "pathb_score"], ascending=[False, False])
        .head(50)
        if len(pathb_audit)
        else pd.DataFrame()
    )

    ehr_gap_table = (
        adjud[adjud["compare_to_structured_ehr"].astype(str).str.lower() != "yes"]
        .groupby(["mention_status", "compare_to_structured_ehr"], as_index=False)
        .size()
        .rename(columns={"size": "count"})
        .sort_values("count", ascending=False)
        if len(adjud)
        else pd.DataFrame()
    )

    _write_table(cohort_table, out_dir, "rq1_table_cohort")
    _write_table(extraction_table, out_dir, "rq1_table_extraction_truth")
    _write_table(status_compare_table, out_dir, "rq1_table_status_distribution")
    _write_table(normalization_table, out_dir, "rq1_table_normalization")
    _write_table(downstream_table, out_dir, "rq1_table_downstream_concordance")
    _write_table(downstream_ablation_table, out_dir, "rq1_table_downstream_concordance_ablation")
    _write_table(downstream_status_table, out_dir, "rq1_table_downstream_concordance_status")
    _write_table(unresolved_table, out_dir, "rq1_table_top_unresolved_after_patha")
    _write_table(false_links_table, out_dir, "rq1_table_top_false_links_pathb")
    _write_table(error_taxonomy_table, out_dir, "rq1_table_error_taxonomy")
    _write_table(abstention_table, out_dir, "rq1_table_top_abstentions_pathb")
    _write_table(accepted_audit_table, out_dir, "rq1_table_pathb_accepted_examples")
    _write_table(rejected_audit_table, out_dir, "rq1_table_pathb_rejected_examples")
    _write_table(ehr_gap_table, out_dir, "rq1_table_ehr_gap_status_explanations")
    _write_table(status_confusion_table, out_dir, "rq1_table_top_status_confusion")

    fig_path = out_dir / "rq1_pathb_precision_by_confidence.png"
    if len(conf_df) and plt is not None:
        plt.figure(figsize=(7, 4))
        plt.plot(conf_df["band_min"], conf_df["accepted_precision"], marker="o")
        plt.xlabel("Confidence band minimum")
        plt.ylabel("Accepted-link precision")
        plt.title("Path B precision by confidence band")
        plt.tight_layout()
        plt.savefig(fig_path, dpi=150)
        plt.close()

    workflow_fig_path = out_dir / "rq1_workflow_figure.png"
    _build_workflow_figure(workflow_fig_path)

    workflow_mermaid_path = out_dir / "rq1_workflow_figure_mermaid.md"
    workflow_mermaid_path.write_text(
        """```mermaid
flowchart LR
    A[Clinic Notes\nTreatment-context extraction] --> B[LLM-assisted human adjudication\nNote-grounded truth]
    B --> C[Controlled normalization\nBaseline -> Path A -> Path B]
    C --> D[Primary evaluation\nExtraction + normalization]
    D --> E[Secondary downstream verification\nNote-to-EHR concordance]
```
""",
        encoding="utf-8",
    )

    stubs_path = out_dir / "rq1_manuscript_stubs_bibm.md"
    stubs_path.write_text(
        """## Problem Statement
We present a clinic-note-only, adjudication-first biomedical informatics study for treatment-context medication extraction and normalization. The primary truth source is note-grounded LLM-assisted human adjudication; structured EHR medications are used only for downstream concordance verification.

## Contributions
- A layered evaluation design separating extraction truth from downstream EHR concordance.
- A controlled normalization ablation (Baseline, Path A, Path B) with transparent, incremental roles.
- A calibrated, abstaining Path B linker over a canonical drug vocabulary with explicit reason codes.
- Error taxonomy and audit artifacts to support reproducibility and clinical interpretation.

## Why Note-grounded Truth Matters
Structured EHR medication lists are incomplete and temporally asynchronous relative to note narratives. Using adjudicated note-grounded mentions as primary truth avoids conflating extraction quality with documentation mismatch.

## Why Note-vs-EHR Gaps Matter
Post-adjudication note-to-EHR disagreement quantifies documentation and workflow gaps, informing data quality and integration strategy rather than being treated as extraction failure.

## Controlled Baseline Path A Path B Framing
- Baseline: surface normalization plus exact canonical-vocabulary lookup.
- Path A: deterministic high-precision extension via curated aliases and conservative decomposition.
- Path B: calibrated abstaining linker for unresolved mentions, with confidence thresholds and reject reasons.

## BIBM Fit
Primary fit: Biomedical and Health Informatics (information retrieval, ontology-linked NLP/text mining). Secondary fit: clinical information systems and EHR standards integration with explicit adjudication-first evaluation.
""",
        encoding="utf-8",
    )

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
                "rq1_table_downstream_concordance_ablation",
                "rq1_table_downstream_concordance_status",
                "rq1_table_top_unresolved_after_patha",
                "rq1_table_top_false_links_pathb",
                "rq1_table_error_taxonomy",
                "rq1_table_top_abstentions_pathb",
                "rq1_table_pathb_accepted_examples",
                "rq1_table_pathb_rejected_examples",
                "rq1_table_ehr_gap_status_explanations",
                "rq1_table_top_status_confusion",
            ],
            "generated_figures": {
                "pathb_precision_by_confidence": str(fig_path) if fig_path.exists() else None,
                "workflow_figure": str(workflow_fig_path) if workflow_fig_path.exists() else None,
            },
            "workflow_mermaid": str(workflow_mermaid_path),
            "manuscript_stubs": str(stubs_path),
            "pathb_recommendation": (pathb_summary or {}).get("recommendation", {}),
        },
    )

    print(f"Saved layered paper outputs to: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
