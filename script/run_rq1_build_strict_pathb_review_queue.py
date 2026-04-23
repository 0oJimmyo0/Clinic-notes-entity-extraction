#!/usr/bin/env python3
"""
Build prioritized strict Path B human review queue from diagnostic slice.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import List

import pandas as pd

from rq1_adjudication_utils import write_run_summary
from rq1_drug_linking import normalize_drug_text


ERROR_TAXONOMY_ALLOWED = [
    "missing_alias",
    "abbreviation_ambiguous",
    "combo_ingredient_mismatch",
    "formulation_or_salt_variant",
    "vague_class_term",
    "candidate_generation_miss",
    "ranking_miss",
    "gating_too_strict",
    "genuinely_unclear_or_out_of_scope",
    "other",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build strict Path B review queue.")
    p.add_argument(
        "--strict-slice-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/normalization_eval/rq1_strict_pathb_diagnostic_slice.csv",
    )
    p.add_argument(
        "--output-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/adjudicated/strict_pathb_review_queue.csv",
    )
    p.add_argument(
        "--summary-json",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/adjudicated/strict_pathb_review_queue_summary.json",
    )
    p.add_argument("--score-threshold", type=float, default=0.45, help="Current Path B acceptance threshold.")
    return p.parse_args()


def _parse_top1_score(text: str) -> float:
    try:
        arr = json.loads(str(text))
    except Exception:
        return 0.0
    if not isinstance(arr, list) or not arr:
        return 0.0
    first = arr[0]
    if not isinstance(first, dict):
        return 0.0
    try:
        return float(first.get("score", 0.0))
    except Exception:
        return 0.0


def _parse_topk_labels(text: str) -> List[str]:
    try:
        arr = json.loads(str(text))
    except Exception:
        return []
    if not isinstance(arr, list):
        return []
    out = []
    for item in arr:
        if not isinstance(item, dict):
            continue
        label = normalize_drug_text(str(item.get("canonical_label", "") or ""))
        if label:
            out.append(label)
    return out


def _is_abbrev_like(raw: str) -> bool:
    t = str(raw).strip()
    if not t:
        return False
    compact = re.sub(r"[^A-Za-z0-9]", "", t)
    tn = normalize_drug_text(t)
    toks = [x for x in tn.split() if x]
    if compact and len(compact) <= 4:
        return True
    if len(toks) == 1 and len(toks[0]) <= 5:
        return True
    if re.fullmatch(r"[A-Z]{2,6}", t):
        return True
    return False


def _is_combo_salt_formulation_like(raw: str) -> bool:
    t = str(raw).strip().lower()
    if not t:
        return False
    combo_pat = r"(\/|\+|\band\b|\bwith\b|\bplus\b)"
    salt_terms = [
        "hcl",
        "hydrochloride",
        "sodium",
        "succinate",
        "acetate",
        "phosphate",
        "citrate",
        "tartrate",
        "mesylate",
        "besylate",
        "fumarate",
        "maleate",
    ]
    form_terms = [
        "er",
        "xr",
        "sr",
        "dr",
        "cr",
        "ir",
        "inj",
        "injection",
        "tablet",
        "capsule",
        "solution",
        "suspension",
        "cream",
        "ointment",
        "patch",
    ]
    if re.search(combo_pat, t):
        return True
    if any(re.search(rf"\b{re.escape(x)}\b", t) for x in salt_terms):
        return True
    if any(re.search(rf"\b{re.escape(x)}\b", t) for x in form_terms):
        return True
    return False


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    slice_path = (root / args.strict_slice_csv).resolve()
    out_path = (root / args.output_csv).resolve()
    summary_path = (root / args.summary_json).resolve()

    if not slice_path.exists():
        raise FileNotFoundError(f"Missing strict slice CSV: {slice_path}")

    df = pd.read_csv(slice_path).fillna("")
    if len(df) == 0:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_path, index=False)
        write_run_summary(
            summary_path,
            {
                "inputs": {"strict_slice_csv": str(slice_path)},
                "counts": {"strict_slice_n": 0, "queue_n": 0},
                "outputs": {"review_queue_csv": str(out_path)},
            },
        )
        print(f"Saved empty strict review queue: {out_path}")
        return 0

    df["pathb_score"] = pd.to_numeric(df.get("pathb_score", 0.0), errors="coerce").fillna(0.0)
    df["pathb_margin"] = pd.to_numeric(df.get("pathb_margin", 0.0), errors="coerce").fillna(0.0)
    df["threshold_gap"] = (df["pathb_score"] - float(args.score_threshold)).abs()
    df["top1_score"] = df.get("pathb_top_k_candidates_json", "").map(_parse_top1_score)
    df["topk_labels"] = df.get("pathb_top_k_candidates_json", "").map(_parse_topk_labels)
    df["topk_count"] = df["topk_labels"].map(len)
    df["abbrev_like"] = df.get("raw_mention_text", "").map(_is_abbrev_like)
    df["combo_salt_formulation_like"] = df.get("raw_mention_text", "").map(_is_combo_salt_formulation_like)

    # Priority logic:
    # A: highest score, near-threshold, plausible top-k
    # B: abbreviation/combo/salt/formulation patterns
    # C: remaining rows
    score_p90 = float(df["pathb_score"].quantile(0.90))
    near_threshold = df["threshold_gap"] <= 0.08
    high_score = df["pathb_score"] >= score_p90
    plausible_topk = (df["topk_count"] > 0) & (df["top1_score"] >= float(df["top1_score"].quantile(0.75)))
    pattern_flag = df["abbrev_like"] | df["combo_salt_formulation_like"]

    df["review_priority"] = "C"
    df.loc[pattern_flag, "review_priority"] = "B"
    df.loc[high_score | near_threshold | plausible_topk, "review_priority"] = "A"

    priority_rank = {"A": 0, "B": 1, "C": 2}
    df["priority_rank"] = df["review_priority"].map(priority_rank).fillna(9).astype(int)

    # Editable defaults.
    df["review_action"] = "keep"
    if "adjudicated_canonical_label" not in df.columns:
        df["adjudicated_canonical_label"] = ""
    df["mention_status"] = df.get("mention_status", "").astype(str)
    df["compare_to_structured_ehr"] = df.get("compare_to_structured_ehr", "").astype(str)
    df["reviewer_notes"] = ""
    df["error_taxonomy_manual"] = ""

    # Drop helper list column before export.
    df = df.drop(columns=["topk_labels"])

    # Sort by requested priority order.
    df = df.sort_values(
        by=["priority_rank", "pathb_score", "threshold_gap", "note_id", "adjudication_unit_id"],
        ascending=[True, False, True, True, True],
        na_position="last",
    ).reset_index(drop=True)

    # Output schema keeps diagnostics + editable fields.
    preferred_cols = [
        "review_priority",
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
        "top1_score",
        "topk_count",
        "abbrev_like",
        "combo_salt_formulation_like",
        "threshold_gap",
        "review_action",
        "reviewer_notes",
        "error_taxonomy_manual",
    ]
    for c in preferred_cols:
        if c not in df.columns:
            df[c] = ""
    out_df = df[preferred_cols]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_path, index=False)

    write_run_summary(
        summary_path,
        {
            "inputs": {
                "strict_slice_csv": str(slice_path),
                "score_threshold": float(args.score_threshold),
            },
            "counts": {
                "strict_slice_n": int(len(df)),
                "priority_A_n": int((out_df["review_priority"] == "A").sum()),
                "priority_B_n": int((out_df["review_priority"] == "B").sum()),
                "priority_C_n": int((out_df["review_priority"] == "C").sum()),
            },
            "allowed_error_taxonomy_manual_values": ERROR_TAXONOMY_ALLOWED,
            "outputs": {
                "strict_review_queue_csv": str(out_path),
            },
        },
    )

    print(f"Saved strict Path B review queue: {out_path}")
    print(f"Rows: {len(out_df):,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
