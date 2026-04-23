#!/usr/bin/env python3
"""
Patch full reviewed adjudication CSV from completed Path B leftover review decisions.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from rq1_adjudication_utils import COMPARE_VALUES, STATUS_VALUES, write_run_summary


REQUIRED_JOIN_SCHEMA = [
    "adjudication_unit_id",
    "person_id",
    "visit_id",
    "note_id",
    "span_id_or_local_reference",
    "raw_mention_text",
    "context_text",
    "adjudicated_canonical_label",
    "mention_status",
    "compare_to_structured_ehr",
    "reviewer_notes",
]

PATCH_FIELDS = [
    "adjudicated_canonical_label",
    "mention_status",
    "compare_to_structured_ehr",
    "reviewer_notes",
]

ALLOWED_REVIEW_ACTIONS = {"keep", "drop_row", "needs_manual_followup"}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Patch reviewed adjudication CSV from leftover review decisions.")
    p.add_argument(
        "--base-reviewed-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/adjudicated/reviewed_adjudication_from_medications_jsonl.csv",
    )
    p.add_argument(
        "--leftover-review-completed-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/adjudicated/pathb_leftover_review_completed.csv",
    )
    p.add_argument(
        "--output-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/adjudicated/reviewed_adjudication_patched.csv",
    )
    p.add_argument(
        "--audit-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/adjudicated/reviewed_adjudication_patch_audit.csv",
    )
    p.add_argument(
        "--summary-json",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/adjudicated/reviewed_adjudication_patch_summary.json",
    )
    return p.parse_args()


def _normalize_col(df: pd.DataFrame, col: str) -> pd.Series:
    return df[col].fillna("").astype(str).str.strip()


def _validate_allowed_values(df: pd.DataFrame) -> Tuple[List[str], List[str]]:
    status = _normalize_col(df, "mention_status").str.lower()
    compare = _normalize_col(df, "compare_to_structured_ehr").str.lower()
    bad_status = sorted(set(status) - set(STATUS_VALUES))
    bad_compare = sorted(set(compare) - set(COMPARE_VALUES))
    return bad_status, bad_compare


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]

    base_path = (root / args.base_reviewed_csv).resolve()
    review_path = (root / args.leftover_review_completed_csv).resolve()
    out_path = (root / args.output_csv).resolve()
    audit_path = (root / args.audit_csv).resolve()
    summary_path = (root / args.summary_json).resolve()

    if not base_path.exists():
        raise FileNotFoundError(f"Missing base reviewed CSV: {base_path}")
    if not review_path.exists():
        raise FileNotFoundError(f"Missing completed leftover review CSV: {review_path}")

    base = pd.read_csv(base_path).fillna("")
    review = pd.read_csv(review_path).fillna("")

    miss_base = [c for c in REQUIRED_JOIN_SCHEMA if c not in base.columns]
    if miss_base:
        raise ValueError(f"Base reviewed CSV missing required columns: {miss_base}")

    required_review_cols = ["adjudication_unit_id", "review_action"] + PATCH_FIELDS
    miss_review = [c for c in required_review_cols if c not in review.columns]
    if miss_review:
        raise ValueError(f"Completed review CSV missing required columns: {miss_review}")

    base["adjudication_unit_id"] = _normalize_col(base, "adjudication_unit_id")
    review["adjudication_unit_id"] = _normalize_col(review, "adjudication_unit_id")
    review["review_action"] = _normalize_col(review, "review_action").str.lower()
    for c in PATCH_FIELDS:
        review[c] = _normalize_col(review, c)

    bad_actions = sorted(set(review["review_action"]) - ALLOWED_REVIEW_ACTIONS)
    if bad_actions:
        raise ValueError(f"Invalid review_action values: {bad_actions}")

    bad_status_review = sorted(set(review["mention_status"].str.lower()) - set(STATUS_VALUES))
    bad_compare_review = sorted(set(review["compare_to_structured_ehr"].str.lower()) - set(COMPARE_VALUES))
    if bad_status_review:
        raise ValueError(f"Completed review has invalid mention_status values: {bad_status_review}")
    if bad_compare_review:
        raise ValueError(
            f"Completed review has invalid compare_to_structured_ehr values: {bad_compare_review}"
        )

    patched = base.copy()
    audit_rows: List[Dict[str, object]] = []

    n_rows_changed = 0
    n_rows_removed = 0
    n_keep = int((review["review_action"] == "keep").sum())
    n_drop = int((review["review_action"] == "drop_row").sum())
    n_followup = int((review["review_action"] == "needs_manual_followup").sum())

    for r in review.itertuples(index=False):
        unit_id = str(getattr(r, "adjudication_unit_id", "")).strip()
        action = str(getattr(r, "review_action", "")).strip().lower()

        mask = patched["adjudication_unit_id"].astype(str).str.strip().eq(unit_id)
        matched_n = int(mask.sum())

        if matched_n == 0:
            audit_rows.append(
                {
                    "adjudication_unit_id": unit_id,
                    "review_action": action,
                    "matched_base_rows": 0,
                    "rows_changed": 0,
                    "rows_removed": 0,
                    "manual_followup_flag": int(action == "needs_manual_followup"),
                    "status": "unmatched_adjudication_unit_id",
                }
            )
            continue

        if action == "drop_row":
            patched = patched.loc[~mask].copy()
            n_rows_removed += matched_n
            audit_rows.append(
                {
                    "adjudication_unit_id": unit_id,
                    "review_action": action,
                    "matched_base_rows": matched_n,
                    "rows_changed": 0,
                    "rows_removed": matched_n,
                    "manual_followup_flag": 0,
                    "status": "removed",
                }
            )
            continue

        changed_here = 0
        for idx in patched.index[mask]:
            before = {c: str(patched.at[idx, c]) for c in PATCH_FIELDS}
            for c in PATCH_FIELDS:
                patched.at[idx, c] = str(getattr(r, c, "")).strip()
            after = {c: str(patched.at[idx, c]) for c in PATCH_FIELDS}
            if before != after:
                changed_here += 1

        n_rows_changed += changed_here
        audit_rows.append(
            {
                "adjudication_unit_id": unit_id,
                "review_action": action,
                "matched_base_rows": matched_n,
                "rows_changed": changed_here,
                "rows_removed": 0,
                "manual_followup_flag": int(action == "needs_manual_followup"),
                "status": "patched",
            }
        )

    # Validate final patched schema and value vocabularies.
    miss_final = [c for c in REQUIRED_JOIN_SCHEMA if c not in patched.columns]
    if miss_final:
        raise ValueError(f"Patched reviewed CSV missing required columns: {miss_final}")

    bad_status_final, bad_compare_final = _validate_allowed_values(patched)
    if bad_status_final:
        raise ValueError(f"Patched reviewed CSV has invalid mention_status values: {bad_status_final}")
    if bad_compare_final:
        raise ValueError(
            f"Patched reviewed CSV has invalid compare_to_structured_ehr values: {bad_compare_final}"
        )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    patched.to_csv(out_path, index=False)
    audit_df = pd.DataFrame(audit_rows)
    audit_df.to_csv(audit_path, index=False)

    summary = {
        "inputs": {
            "base_reviewed_csv": str(base_path),
            "leftover_review_completed_csv": str(review_path),
        },
        "counts": {
            "n_base_rows": int(len(base)),
            "n_leftover_rows_input": int(len(review)),
            "n_keep": n_keep,
            "n_drop_row": n_drop,
            "n_needs_manual_followup": n_followup,
            "n_rows_changed": int(n_rows_changed),
            "n_rows_removed": int(n_rows_removed),
            "n_final_rows": int(len(patched)),
        },
        "outputs": {
            "reviewed_adjudication_patched_csv": str(out_path),
            "reviewed_adjudication_patch_audit_csv": str(audit_path),
            "reviewed_adjudication_patch_summary_json": str(summary_path),
        },
    }
    write_run_summary(summary_path, summary)

    print(f"Saved patched reviewed adjudication CSV: {out_path}")
    print(f"Rows: {len(patched):,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
