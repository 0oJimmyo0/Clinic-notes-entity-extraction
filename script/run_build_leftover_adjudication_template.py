#!/usr/bin/env python3
"""
Build a reviewer-editable template for Path A leftovers only.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List

import pandas as pd

from rq1_adjudication_utils import write_run_summary


IDENTITY_COLS = [
    "adjudication_unit_id",
    "person_id",
    "visit_id",
    "note_id",
    "span_id_or_local_reference",
]

CONTEXT_COLS = [
    "raw_mention_text",
    "context_text",
    "note_date",
    "note_title",
    "candidate_category",
    "target_drug",
    "seed_extracted_drugs_json",
    "seed_treatment_action",
    "seed_discontinuation_reason",
    "seed_certainty",
]

CURRENT_HELPER_COLS = [
    "current_adjudicated_canonical_label",
    "current_mention_status",
    "current_compare_to_structured_ehr",
    "current_reviewer_notes",
    "current_patha_term",
    "current_patha_prediction",
    "current_patha_correct",
    "current_pathb_prediction",
    "current_pathb_accepted",
    "current_pathb_score",
    "current_pathb_margin",
    "current_pathb_calibrated_confidence",
    "current_pathb_reason_codes_json",
    "current_pathb_top_k_candidates_json",
    "current_error_bucket",
]

EDITABLE_COLS = [
    "review_action",
    "adjudicated_canonical_label",
    "mention_status",
    "compare_to_structured_ehr",
    "reviewer_notes",
]

OUTPUT_COL_ORDER = IDENTITY_COLS + CONTEXT_COLS + CURRENT_HELPER_COLS + EDITABLE_COLS


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build Path B leftover adjudication review template.")
    p.add_argument(
        "--leftovers-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/adjudicated/rq1_pathb_leftovers.csv",
    )
    p.add_argument(
        "--normalization-detailed-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/normalization_eval/rq1_normalization_eval_detailed.csv",
    )
    p.add_argument(
        "--packets-mentions-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/adjudication_packets/adjudication_packets_mentions.csv",
    )
    p.add_argument(
        "--current-reviewed-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/adjudicated/reviewed_adjudication_from_medications_jsonl.csv",
    )
    p.add_argument(
        "--output-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/adjudicated/pathb_leftover_review_template.csv",
    )
    p.add_argument(
        "--manifest-json",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/adjudicated/pathb_leftover_review_template_manifest.json",
    )
    return p.parse_args()


def _require_cols(df: pd.DataFrame, cols: List[str], tag: str) -> None:
    miss = [c for c in cols if c not in df.columns]
    if miss:
        raise ValueError(f"{tag} missing required columns: {miss}")


def _normalize_ids(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in IDENTITY_COLS:
        if c in out.columns:
            out[c] = out[c].astype(str).str.strip()
    return out


def _first_nonempty(a: str, b: str) -> str:
    a_s = str(a or "").strip()
    if a_s:
        return a_s
    return str(b or "").strip()


def _series_or_default(df: pd.DataFrame, col: str, default) -> pd.Series:
    if col in df.columns:
        return df[col]
    return pd.Series([default] * len(df), index=df.index)


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]

    leftovers_path = (root / args.leftovers_csv).resolve()
    detail_path = (root / args.normalization_detailed_csv).resolve()
    packets_path = (root / args.packets_mentions_csv).resolve()
    reviewed_path = (root / args.current_reviewed_csv).resolve()
    out_csv = (root / args.output_csv).resolve()
    manifest_json = (root / args.manifest_json).resolve()

    for p in [leftovers_path, detail_path, packets_path, reviewed_path]:
        if not p.exists():
            raise FileNotFoundError(f"Missing input file: {p}")

    leftovers = _normalize_ids(pd.read_csv(leftovers_path).fillna(""))
    detail = _normalize_ids(pd.read_csv(detail_path).fillna(""))
    packets = _normalize_ids(pd.read_csv(packets_path).fillna(""))
    reviewed = _normalize_ids(pd.read_csv(reviewed_path).fillna(""))

    _require_cols(leftovers, IDENTITY_COLS + ["raw_mention_text"], "leftovers")
    _require_cols(detail, ["adjudication_unit_id"], "normalization_detailed")
    _require_cols(packets, IDENTITY_COLS, "packets_mentions")
    _require_cols(
        reviewed,
        IDENTITY_COLS
        + ["raw_mention_text", "context_text", "adjudicated_canonical_label", "mention_status", "compare_to_structured_ehr", "reviewer_notes"],
        "current_reviewed",
    )

    keep_detail = [
        "adjudication_unit_id",
        "patha_term",
        "patha_prediction",
        "patha_correct",
        "pathb_prediction",
        "pathb_accepted",
        "pathb_score",
        "pathb_margin",
        "pathb_calibrated_confidence",
        "pathb_reason_codes_json",
        "pathb_top_k_candidates_json",
        "error_bucket",
    ]
    keep_detail = [c for c in keep_detail if c in detail.columns]
    detail_small = detail[keep_detail].drop_duplicates(subset=["adjudication_unit_id"], keep="first")

    keep_packets = IDENTITY_COLS + [c for c in CONTEXT_COLS if c != "raw_mention_text"]
    keep_packets = [c for c in keep_packets if c in packets.columns]
    packets_small = packets[keep_packets].drop_duplicates(subset=["adjudication_unit_id"], keep="first")

    keep_reviewed = IDENTITY_COLS + [
        "raw_mention_text",
        "context_text",
        "adjudicated_canonical_label",
        "mention_status",
        "compare_to_structured_ehr",
        "reviewer_notes",
    ]
    reviewed_small = reviewed[keep_reviewed].drop_duplicates(subset=["adjudication_unit_id"], keep="first")

    out = leftovers.copy()
    out = out.merge(
        packets_small,
        on=IDENTITY_COLS,
        how="left",
        suffixes=("", "_pkt"),
    )
    out = out.merge(
        reviewed_small,
        on=IDENTITY_COLS,
        how="left",
        suffixes=("", "_rev"),
    )
    out = out.merge(
        detail_small,
        on="adjudication_unit_id",
        how="left",
        suffixes=("", "_detail"),
    )

    # Resolve context fields from packet first, then reviewed/leftover.
    out["raw_mention_text"] = out.apply(
        lambda r: _first_nonempty(r.get("raw_mention_text", ""), r.get("raw_mention_text_rev", "")),
        axis=1,
    )
    out["context_text"] = out.apply(
        lambda r: _first_nonempty(r.get("context_text", ""), r.get("context_text_rev", "")),
        axis=1,
    )

    out["current_adjudicated_canonical_label"] = out.apply(
        lambda r: _first_nonempty(r.get("adjudicated_canonical_label_rev", ""), r.get("adjudicated_canonical_label", "")),
        axis=1,
    )
    out["current_mention_status"] = out.apply(
        lambda r: _first_nonempty(r.get("mention_status_rev", ""), r.get("mention_status", "")),
        axis=1,
    )
    out["current_compare_to_structured_ehr"] = out.apply(
        lambda r: _first_nonempty(r.get("compare_to_structured_ehr_rev", ""), r.get("compare_to_structured_ehr", "")),
        axis=1,
    )
    out["current_reviewer_notes"] = out.apply(
        lambda r: _first_nonempty(r.get("reviewer_notes_rev", ""), r.get("reviewer_notes", "")),
        axis=1,
    )

    out["current_patha_term"] = _series_or_default(out, "patha_term", "").astype(str)
    out["current_patha_prediction"] = _series_or_default(out, "patha_prediction", "").astype(str)
    out["current_patha_correct"] = _series_or_default(out, "patha_correct", False)
    out["current_pathb_prediction"] = _series_or_default(out, "pathb_prediction", "").astype(str)
    out["current_pathb_accepted"] = _series_or_default(out, "pathb_accepted", False)
    out["current_pathb_score"] = pd.to_numeric(_series_or_default(out, "pathb_score", 0.0), errors="coerce")
    out["current_pathb_margin"] = pd.to_numeric(_series_or_default(out, "pathb_margin", 0.0), errors="coerce")
    out["current_pathb_calibrated_confidence"] = pd.to_numeric(
        _series_or_default(out, "pathb_calibrated_confidence", 0.0), errors="coerce"
    )
    out["current_pathb_reason_codes_json"] = _series_or_default(out, "pathb_reason_codes_json", "").astype(str)
    out["current_pathb_top_k_candidates_json"] = _series_or_default(out, "pathb_top_k_candidates_json", "").astype(str)
    out["current_error_bucket"] = _series_or_default(out, "error_bucket", "").astype(str)

    # Reviewer-editable defaults.
    out["review_action"] = "keep"
    out["adjudicated_canonical_label"] = out["current_adjudicated_canonical_label"]
    out["mention_status"] = out["current_mention_status"]
    out["compare_to_structured_ehr"] = out["current_compare_to_structured_ehr"]
    out["reviewer_notes"] = out["current_reviewer_notes"]

    # Ensure required output columns exist.
    for c in OUTPUT_COL_ORDER:
        if c not in out.columns:
            out[c] = ""

    out["_sort_score"] = pd.to_numeric(out["current_pathb_score"], errors="coerce")
    out = out.sort_values(
        by=["current_error_bucket", "_sort_score", "note_id", "adjudication_unit_id"],
        ascending=[True, True, True, True],
        na_position="last",
    ).reset_index(drop=True)
    out = out[OUTPUT_COL_ORDER]

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_csv, index=False)

    summary: Dict[str, object] = {
        "inputs": {
            "leftovers_csv": str(leftovers_path),
            "normalization_detailed_csv": str(detail_path),
            "packets_mentions_csv": str(packets_path),
            "current_reviewed_csv": str(reviewed_path),
        },
        "counts": {
            "n_leftovers_input": int(len(leftovers)),
            "n_template_rows": int(len(out)),
            "n_rows_with_context_text": int(out["context_text"].astype(str).str.strip().ne("").sum()),
            "n_rows_with_detail": int(out["current_patha_prediction"].astype(str).str.strip().ne("").sum()),
            "n_rows_with_current_review": int(
                out["current_adjudicated_canonical_label"].astype(str).str.strip().ne("").sum()
            ),
        },
        "allowed_review_action_values": ["keep", "drop_row", "needs_manual_followup"],
        "outputs": {
            "template_csv": str(out_csv),
            "manifest_json": str(manifest_json),
        },
    }
    write_run_summary(manifest_json, summary)

    print(f"Saved leftover review template: {out_csv}")
    print(f"Rows: {len(out):,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
