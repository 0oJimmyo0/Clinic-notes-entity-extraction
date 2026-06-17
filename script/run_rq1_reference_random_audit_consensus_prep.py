#!/usr/bin/env python3
"""
Prepare a consensus-ready random-audit annotation file from two reviewer passes.

Behavior:
- Auto-fill adjudicated_* fields when reviewer 1 and reviewer 2 agree.
- Leave disagreement rows for manual adjudication.
- Export a compact disagreement packet and agreement summary.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from rq1_adjudication_utils import write_run_summary


BOOL_FIELDS = [
    "medication_valid",
    "span_valid",
    "canonical_correct",
    "action_correct",
]

TEXT_FIELDS = [
    "corrected_canonical_label",
    "error_category",
    "confidence",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Auto-fill consensus rows and isolate disagreement rows for random audit.")
    p.add_argument(
        "--annotation-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/reference_random_audit_sample_n1000/7498fe8a-6222-480b-8043-9c846653e2a7_annotator1.csv",
    )
    p.add_argument(
        "--output-dir",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/reference_random_audit_consensus_prep_n1000",
    )
    return p.parse_args()


def _norm_bool(v: object) -> str:
    x = str(v).strip().lower()
    if x in {"yes", "y", "true", "1", "correct", "valid"}:
        return "yes"
    if x in {"no", "n", "false", "0", "incorrect", "invalid"}:
        return "no"
    if x in {"uncertain", "unclear", "unknown", "maybe"}:
        return "uncertain"
    return ""


def _norm_text(v: object) -> str:
    return str(v).strip()


def _cohen_kappa(labels1: list[str], labels2: list[str]) -> float:
    categories = sorted(set(labels1) | set(labels2))
    n = len(labels1)
    if n == 0:
        return float("nan")
    p0 = sum(1 for a, b in zip(labels1, labels2) if a == b) / n
    p1 = {c: sum(1 for x in labels1 if x == c) / n for c in categories}
    p2 = {c: sum(1 for x in labels2 if x == c) / n for c in categories}
    pe = sum(p1[c] * p2[c] for c in categories)
    if pe >= 1.0:
        return float("nan")
    return (p0 - pe) / (1 - pe)


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    ann_path = (root / args.annotation_csv).resolve()
    out_dir = (root / args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(ann_path).fillna("")

    for field in BOOL_FIELDS:
        df[f"reviewer_1_{field}_norm"] = df[f"reviewer_1_{field}"].map(_norm_bool)
        df[f"reviewer_2_{field}_norm"] = df[f"reviewer_2_{field}"].map(_norm_bool)
    for field in TEXT_FIELDS:
        df[f"reviewer_1_{field}_norm"] = df[f"reviewer_1_{field}"].map(_norm_text)
        df[f"reviewer_2_{field}_norm"] = df[f"reviewer_2_{field}"].map(_norm_text)

    agreement_rows = []
    for field in BOOL_FIELDS:
        c1 = f"reviewer_1_{field}_norm"
        c2 = f"reviewer_2_{field}_norm"
        sub = df[(df[c1] != "") & (df[c2] != "")]
        agree = float((sub[c1] == sub[c2]).mean()) if len(sub) else float("nan")
        kappa = _cohen_kappa(sub[c1].tolist(), sub[c2].tolist()) if len(sub) else float("nan")
        agreement_rows.append(
            {
                "field": field,
                "n_double_coded": int(len(sub)),
                "percent_agreement": agree,
                "cohen_kappa": kappa,
            }
        )
    for field in ["error_category"]:
        c1 = f"reviewer_1_{field}_norm"
        c2 = f"reviewer_2_{field}_norm"
        sub = df[(df[c1] != "") & (df[c2] != "")]
        agree = float((sub[c1] == sub[c2]).mean()) if len(sub) else float("nan")
        agreement_rows.append(
            {
                "field": field,
                "n_double_coded": int(len(sub)),
                "percent_agreement": agree,
                "cohen_kappa": float("nan"),
            }
        )
    agreement_df = pd.DataFrame(agreement_rows)

    disagreement_mask = pd.Series(False, index=df.index)
    for field in BOOL_FIELDS + ["error_category"]:
        c1 = f"reviewer_1_{field}_norm"
        c2 = f"reviewer_2_{field}_norm"
        disagreement_mask = disagreement_mask | (
            (df[c1] != "") & (df[c2] != "") & (df[c1] != df[c2])
        )

    # Auto-fill consensus adjudication fields where both reviewers agree.
    for field in BOOL_FIELDS:
        c1 = f"reviewer_1_{field}_norm"
        c2 = f"reviewer_2_{field}_norm"
        adj = f"adjudicated_{field}"
        agreed = (df[c1] != "") & (df[c1] == df[c2])
        df.loc[agreed, adj] = df.loc[agreed, c1]

    # Corrected canonical label:
    # if both reviewer labels agree exactly, keep it;
    # if one side is blank and canonical is agreed yes, leave blank;
    # otherwise leave for manual adjudication.
    c1 = "reviewer_1_corrected_canonical_label_norm"
    c2 = "reviewer_2_corrected_canonical_label_norm"
    same_label = (df[c1] != "") & (df[c1] == df[c2])
    df.loc[same_label, "adjudicated_corrected_canonical_label"] = df.loc[same_label, c1]

    # Error category / confidence only when agreed exactly.
    for field in ["error_category", "confidence"]:
        c1 = f"reviewer_1_{field}_norm"
        c2 = f"reviewer_2_{field}_norm"
        adj = f"adjudicated_{field}"
        agreed = (df[c1] != "") & (df[c1] == df[c2])
        df.loc[agreed, adj] = df.loc[agreed, c1]

    consensus_ready = df.copy()
    disagreement_df = df[disagreement_mask].copy()

    keep_cols = [
        c for c in [
            "row_id",
            "adjudication_unit_id",
            "visit_id",
            "note_id",
            "bounded_note_context",
            "mention_text",
            "proposed_canonical_label",
            "action_cue",
            "note_type",
            "candidate_category",
            "sample_stratum",
            "reviewer_1_medication_valid",
            "reviewer_2_medication_valid",
            "reviewer_1_span_valid",
            "reviewer_2_span_valid",
            "reviewer_1_canonical_correct",
            "reviewer_2_canonical_correct",
            "reviewer_1_corrected_canonical_label",
            "reviewer_2_corrected_canonical_label",
            "reviewer_1_action_correct",
            "reviewer_2_action_correct",
            "reviewer_1_error_category",
            "reviewer_2_error_category",
            "adjudicated_medication_valid",
            "adjudicated_span_valid",
            "adjudicated_canonical_correct",
            "adjudicated_corrected_canonical_label",
            "adjudicated_action_correct",
            "adjudicated_error_category",
            "adjudicated_confidence",
        ] if c in disagreement_df.columns
    ]
    disagreement_df = disagreement_df[keep_cols]

    # Reviewer 2 error-category distribution.
    dist = (
        df["reviewer_2_error_category_norm"]
        .replace("", "blank")
        .value_counts()
        .rename_axis("reviewer_2_error_category")
        .reset_index(name="count")
    )

    consensus_ready_path = out_dir / "rq1_reference_random_audit_consensus_ready.csv"
    disagreements_path = out_dir / "rq1_reference_random_audit_disagreement_packet.csv"
    agreement_path = out_dir / "rq1_reference_random_audit_agreement_summary.csv"
    r2_dist_path = out_dir / "rq1_reference_random_audit_reviewer2_distribution.csv"

    consensus_ready.to_csv(consensus_ready_path, index=False)
    disagreement_df.to_csv(disagreements_path, index=False)
    agreement_df.to_csv(agreement_path, index=False)
    dist.to_csv(r2_dist_path, index=False)

    payload = {
        "inputs": {
            "annotation_csv": str(ann_path),
        },
        "counts": {
            "rows_total": int(len(df)),
            "rows_with_any_disagreement": int(disagreement_mask.sum()),
            "rows_auto_consensus": int(len(df) - disagreement_mask.sum()),
        },
        "outputs": {
            "consensus_ready_csv": str(consensus_ready_path),
            "disagreement_packet_csv": str(disagreements_path),
            "agreement_summary_csv": str(agreement_path),
            "reviewer2_distribution_csv": str(r2_dist_path),
        },
    }
    write_run_summary(out_dir / "rq1_reference_random_audit_consensus_prep_summary.json", payload)
    print(f"Saved consensus prep outputs to: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
