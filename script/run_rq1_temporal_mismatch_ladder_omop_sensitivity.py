#!/usr/bin/env python3
"""
Build OMOP/RxNorm-backed semantic+temporal mismatch sensitivity analyses.

This script uses explicit OMOP/RxNorm mapping artifacts to produce:
- a standardized temporal ladder for mapped note labels
- coverage-aware sensitivity summaries
- a manual audit packet from the final no-structured-overlap bucket
"""

from __future__ import annotations

import argparse
import math
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd

from rq1_adjudication_utils import write_run_summary
from rq1_drug_linking import normalize_drug_text


INTERNAL_BUCKET_ORDER = [
    "same_visit_exact",
    "same_visit_ingredient",
    "same_visit_category",
    "pm30_exact",
    "pm30_ingredient",
    "pm30_category",
    "pm90_exact",
    "pm90_ingredient",
    "pm90_category",
    "any_history_exact",
    "any_history_ingredient",
    "any_history_category",
    "no_structured_overlap",
]

COLLAPSED_BUCKET_ORDER = [
    "same_visit_exact",
    "same_visit_ingredient_only",
    "same_visit_category_only",
    "plusminus_30d_overlap",
    "plusminus_90d_overlap",
    "any_history_overlap",
    "no_structured_overlap",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="OMOP/RxNorm-backed temporal mismatch sensitivity analysis.")
    p.add_argument(
        "--temporal-detailed-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/temporal_mismatch_ladder/rq1_temporal_mismatch_ladder_detailed.csv",
    )
    p.add_argument(
        "--structured-mapping-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/omop_rxnorm_mapping/rq1_structured_concept_mapping.csv",
    )
    p.add_argument(
        "--note-mapping-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/omop_rxnorm_mapping/rq1_note_label_omop_rxnorm_mapping.csv",
    )
    p.add_argument(
        "--episode-drugs-dir",
        default="../resources/struct_data/episode_drugs",
    )
    p.add_argument(
        "--audit-sample-size",
        type=int,
        default=100,
    )
    p.add_argument(
        "--sample-seed",
        type=int,
        default=42,
    )
    p.add_argument(
        "--output-dir",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/temporal_mismatch_ladder_omop_sensitivity",
    )
    return p.parse_args()


def _parse_pipe_set(value: object) -> Set[str]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return set()
    return {str(x).strip() for x in str(value).split("|") if str(x).strip()}


def _distance_days(anchor: pd.Timestamp, start: pd.Timestamp, end: pd.Timestamp | None) -> float:
    if pd.isna(anchor) or pd.isna(start):
        return math.inf
    if end is not None and not pd.isna(end):
        if start <= anchor <= end:
            return 0.0
        return float(min(abs((anchor - start).days), abs((anchor - end).days)))
    return float(abs((anchor - start).days))


def _match_level(note_row: pd.Series, struct_row: pd.Series) -> Optional[str]:
    note_std = note_row.get("note_standard_concept_id")
    struct_std = struct_row.get("standard_concept_id")
    if pd.notna(note_std) and pd.notna(struct_std) and int(note_std) == int(struct_std):
        return "exact"

    note_ing = note_row.get("note_ingredient_set", set())
    struct_ing = struct_row.get("structured_ingredient_set", set())
    if note_ing and struct_ing and (note_ing & struct_ing):
        return "ingredient"

    note_cat = str(note_row.get("note_category_label_norm", "") or "")
    struct_cat = str(struct_row.get("category_label_norm", "") or "")
    if note_cat and struct_cat and note_cat == struct_cat:
        return "category"

    return None


def _assign_bucket(note_row: pd.Series, history: pd.DataFrame) -> Dict[str, object]:
    if history.empty:
        return {
            "omop_internal_bucket": "no_structured_overlap",
            "omop_collapsed_bucket": "no_structured_overlap",
            "omop_temporal_window": "none",
            "omop_semantic_match_level": "none",
            "omop_matched_structured_drug": "",
            "omop_matched_structured_date": "",
            "omop_day_difference": "",
            "omop_structured_source_visit_id": "",
        }

    checks = [
        ("same_visit", 0, "exact", "same_visit_exact", "same_visit_exact"),
        ("same_visit", 0, "ingredient", "same_visit_ingredient", "same_visit_ingredient_only"),
        ("same_visit", 0, "category", "same_visit_category", "same_visit_category_only"),
        ("plusminus_30d", 30, "exact", "pm30_exact", "plusminus_30d_overlap"),
        ("plusminus_30d", 30, "ingredient", "pm30_ingredient", "plusminus_30d_overlap"),
        ("plusminus_30d", 30, "category", "pm30_category", "plusminus_30d_overlap"),
        ("plusminus_90d", 90, "exact", "pm90_exact", "plusminus_90d_overlap"),
        ("plusminus_90d", 90, "ingredient", "pm90_ingredient", "plusminus_90d_overlap"),
        ("plusminus_90d", 90, "category", "pm90_category", "plusminus_90d_overlap"),
        ("any_history", None, "exact", "any_history_exact", "any_history_overlap"),
        ("any_history", None, "ingredient", "any_history_ingredient", "any_history_overlap"),
        ("any_history", None, "category", "any_history_category", "any_history_overlap"),
    ]

    for window_label, max_days, semantic_level, internal_bucket, collapsed_bucket in checks:
        if window_label == "same_visit":
            candidates = history[history["structured_source_visit_id"] == note_row["visit_id"]].copy()
        else:
            candidates = history.copy()
        if candidates.empty:
            continue
        if max_days is not None:
            candidates = candidates[candidates["day_difference"] <= max_days].copy()
            if candidates.empty:
                continue
        candidates["semantic_level"] = candidates.apply(lambda r: _match_level(note_row, r), axis=1)
        candidates = candidates[candidates["semantic_level"] == semantic_level].copy()
        if candidates.empty:
            continue
        candidates = candidates.sort_values(
            ["day_difference", "structured_exposure_start_date", "structured_source_visit_id", "drug_source_value"],
            na_position="last",
        )
        best = candidates.iloc[0]
        best_date = pd.to_datetime(best["structured_exposure_start_date"], errors="coerce")
        return {
            "omop_internal_bucket": internal_bucket,
            "omop_collapsed_bucket": collapsed_bucket,
            "omop_temporal_window": window_label,
            "omop_semantic_match_level": semantic_level,
            "omop_matched_structured_drug": best.get("drug_source_value", ""),
            "omop_matched_structured_date": best_date.date().isoformat() if pd.notna(best_date) else "",
            "omop_day_difference": int(best["day_difference"]) if pd.notna(best["day_difference"]) and best["day_difference"] != math.inf else "",
            "omop_structured_source_visit_id": best.get("structured_source_visit_id", ""),
        }

    return {
        "omop_internal_bucket": "no_structured_overlap",
        "omop_collapsed_bucket": "no_structured_overlap",
        "omop_temporal_window": "none",
        "omop_semantic_match_level": "none",
        "omop_matched_structured_drug": "",
        "omop_matched_structured_date": "",
        "omop_day_difference": "",
        "omop_structured_source_visit_id": "",
    }


def _build_indexes(history: pd.DataFrame) -> Tuple[Dict[Tuple[str, int], pd.DataFrame], Dict[Tuple[str, str], pd.DataFrame], Dict[Tuple[str, str], pd.DataFrame]]:
    exact_index: Dict[Tuple[str, int], pd.DataFrame] = {}
    ingredient_index: Dict[Tuple[str, str], pd.DataFrame] = {}
    category_index: Dict[Tuple[str, str], pd.DataFrame] = {}

    exact_rows = history[history["standard_concept_id"].notna()].copy()
    for (person_id, std_id), grp in exact_rows.groupby(["person_id", "standard_concept_id"], dropna=False):
        exact_index[(str(person_id), int(std_id))] = grp.copy()

    ing_rows = history[history["ingredient_name_norms"].astype(str) != ""].copy()
    for row in ing_rows.itertuples(index=False):
        for ing in getattr(row, "structured_ingredient_set", set()) or set():
            key = (str(row.person_id), str(ing))
            ingredient_index.setdefault(key, []).append(row._asdict())
    ingredient_index = {k: pd.DataFrame(v) for k, v in ingredient_index.items()}

    cat_rows = history[history["category_label_norm"].astype(str) != ""].copy()
    for (person_id, cat), grp in cat_rows.groupby(["person_id", "category_label_norm"], dropna=False):
        category_index[(str(person_id), str(cat))] = grp.copy()

    return exact_index, ingredient_index, category_index


def _semantic_candidates(
    note_row: pd.Series,
    semantic_level: str,
    exact_index: Dict[Tuple[str, int], pd.DataFrame],
    ingredient_index: Dict[Tuple[str, str], pd.DataFrame],
    category_index: Dict[Tuple[str, str], pd.DataFrame],
) -> pd.DataFrame:
    person_id = str(note_row["person_id"])
    if semantic_level == "exact":
        std_id = note_row.get("note_standard_concept_id")
        if pd.isna(std_id):
            return pd.DataFrame()
        return exact_index.get((person_id, int(std_id)), pd.DataFrame()).copy()
    if semantic_level == "ingredient":
        frames: List[pd.DataFrame] = []
        for ing in note_row.get("note_ingredient_set", set()) or set():
            df = ingredient_index.get((person_id, str(ing)))
            if df is not None and len(df):
                frames.append(df)
        if not frames:
            return pd.DataFrame()
        merged = pd.concat(frames, ignore_index=True)
        subset_cols = [
            c
            for c in [
                "drug_concept_id",
                "structured_source_visit_id",
                "drug_source_value",
                "structured_exposure_start_date",
                "structured_exposure_end_date",
            ]
            if c in merged.columns
        ]
        return merged.drop_duplicates(subset=subset_cols) if subset_cols else merged
    if semantic_level == "category":
        cat = str(note_row.get("note_category_label_norm", "") or "")
        if not cat:
            return pd.DataFrame()
        return category_index.get((person_id, cat), pd.DataFrame()).copy()
    return pd.DataFrame()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]

    detailed_path = (root / args.temporal_detailed_csv).resolve()
    structured_map_path = (root / args.structured_mapping_csv).resolve()
    note_map_path = (root / args.note_mapping_csv).resolve()
    episode_drugs_dir = (root / args.episode_drugs_dir).resolve()
    out_dir = (root / args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    detailed = pd.read_csv(detailed_path).fillna("")
    note_map = pd.read_csv(note_map_path).fillna("")
    structured_map = pd.read_csv(structured_map_path).fillna("")

    note_map = note_map.rename(
        columns={
            "standard_concept_id": "note_standard_concept_id",
            "standard_concept_name": "note_standard_concept_name",
            "standard_concept_name_norm": "note_standard_concept_name_norm",
            "category_label_norm": "note_category_label_norm",
            "note_mapping_status": "note_mapping_status_omop",
        }
    )
    keep_note_cols = [
        "note_label",
        "note_standard_concept_id",
        "note_standard_concept_name",
        "note_standard_concept_name_norm",
        "ingredient_name_norms",
        "note_category_label_norm",
        "note_mapping_status_omop",
    ]
    note_map = note_map[keep_note_cols].rename(columns={"ingredient_name_norms": "note_ingredient_name_norms"})

    detailed = detailed.merge(
        note_map,
        left_on="adjudicated_canonical_label",
        right_on="note_label",
        how="left",
    )
    detailed["note_standard_concept_id"] = pd.to_numeric(detailed["note_standard_concept_id"], errors="coerce")
    detailed["note_ingredient_set"] = detailed["note_ingredient_name_norms"].map(_parse_pipe_set)
    detailed["note_has_omop_mapping"] = detailed["note_standard_concept_id"].notna()
    detailed["note_has_ingredient_mapping"] = detailed["note_ingredient_set"].map(bool)
    detailed["note_has_category_mapping"] = detailed["note_category_label_norm"].astype(str).ne("")

    # Structured history with mapping artifacts
    parts: List[pd.DataFrame] = []
    for f in sorted(episode_drugs_dir.glob("*.parquet")):
        parts.append(
            pd.read_parquet(
                f,
                columns=[
                    "person_id",
                    "visit_occurrence_id",
                    "drug_concept_id",
                    "drug_source_value",
                    "drug_exposure_start_date",
                    "drug_exposure_end_date",
                ],
            )
        )
    history = pd.concat(parts, ignore_index=True)
    history["drug_concept_id"] = pd.to_numeric(history["drug_concept_id"], errors="coerce")
    history = history.rename(columns={"visit_occurrence_id": "structured_source_visit_id"})
    history = history.merge(
        structured_map[
            [
                "drug_concept_id",
                "standard_concept_id",
                "standard_concept_name_norm",
                "ingredient_name_norms",
                "category_label_norm",
                "structured_mapping_status",
            ]
        ],
        on="drug_concept_id",
        how="left",
    )
    history["standard_concept_id"] = pd.to_numeric(history["standard_concept_id"], errors="coerce")
    history["structured_exposure_start_date"] = pd.to_datetime(history["drug_exposure_start_date"], errors="coerce")
    history["structured_exposure_end_date"] = pd.to_datetime(history["drug_exposure_end_date"], errors="coerce")
    history["structured_ingredient_set"] = history["ingredient_name_norms"].map(_parse_pipe_set)
    history["person_id"] = history["person_id"].astype(str)
    history["structured_source_visit_id"] = history["structured_source_visit_id"].astype(str)
    exact_index, ingredient_index, category_index = _build_indexes(history)

    assigned_rows: List[Dict[str, object]] = []
    for row in detailed.itertuples(index=False):
        row_dict = row._asdict()
        has_any_mapping_signal = bool(
            row_dict.get("note_has_omop_mapping")
            or row_dict.get("note_has_ingredient_mapping")
            or row_dict.get("note_has_category_mapping")
        )
        if not has_any_mapping_signal:
            assigned = {
                "omop_internal_bucket": "no_structured_overlap",
                "omop_collapsed_bucket": "no_structured_overlap",
                "omop_temporal_window": "none",
                "omop_semantic_match_level": "none",
                "omop_matched_structured_drug": "",
                "omop_matched_structured_date": "",
                "omop_day_difference": "",
                "omop_structured_source_visit_id": "",
            }
        else:
            note_series = pd.Series(row_dict)
            checks = [
                ("same_visit", 0, "exact", "same_visit_exact", "same_visit_exact"),
                ("same_visit", 0, "ingredient", "same_visit_ingredient", "same_visit_ingredient_only"),
                ("same_visit", 0, "category", "same_visit_category", "same_visit_category_only"),
                ("plusminus_30d", 30, "exact", "pm30_exact", "plusminus_30d_overlap"),
                ("plusminus_30d", 30, "ingredient", "pm30_ingredient", "plusminus_30d_overlap"),
                ("plusminus_30d", 30, "category", "pm30_category", "plusminus_30d_overlap"),
                ("plusminus_90d", 90, "exact", "pm90_exact", "plusminus_90d_overlap"),
                ("plusminus_90d", 90, "ingredient", "pm90_ingredient", "plusminus_90d_overlap"),
                ("plusminus_90d", 90, "category", "pm90_category", "plusminus_90d_overlap"),
                ("any_history", None, "exact", "any_history_exact", "any_history_overlap"),
                ("any_history", None, "ingredient", "any_history_ingredient", "any_history_overlap"),
                ("any_history", None, "category", "any_history_category", "any_history_overlap"),
            ]
            assigned = None
            anchor = pd.to_datetime(row.note_anchor_date, errors="coerce")
            for window_label, max_days, semantic_level, internal_bucket, collapsed_bucket in checks:
                candidates = _semantic_candidates(
                    note_series,
                    semantic_level,
                    exact_index,
                    ingredient_index,
                    category_index,
                )
                if candidates.empty:
                    continue
                if window_label == "same_visit":
                    candidates = candidates[candidates["structured_source_visit_id"] == str(row.visit_id)].copy()
                    if candidates.empty:
                        continue
                candidates["day_difference"] = candidates.apply(
                    lambda r: _distance_days(anchor, r["structured_exposure_start_date"], r["structured_exposure_end_date"]),
                    axis=1,
                )
                if max_days is not None:
                    candidates = candidates[candidates["day_difference"] <= max_days].copy()
                    if candidates.empty:
                        continue
                candidates = candidates.sort_values(
                    ["day_difference", "structured_exposure_start_date", "structured_source_visit_id", "drug_source_value"],
                    na_position="last",
                )
                best = candidates.iloc[0]
                best_date = pd.to_datetime(best["structured_exposure_start_date"], errors="coerce")
                assigned = {
                    "omop_internal_bucket": internal_bucket,
                    "omop_collapsed_bucket": collapsed_bucket,
                    "omop_temporal_window": window_label,
                    "omop_semantic_match_level": semantic_level,
                    "omop_matched_structured_drug": best.get("drug_source_value", ""),
                    "omop_matched_structured_date": best_date.date().isoformat() if pd.notna(best_date) else "",
                    "omop_day_difference": int(best["day_difference"]) if pd.notna(best["day_difference"]) and best["day_difference"] != math.inf else "",
                    "omop_structured_source_visit_id": best.get("structured_source_visit_id", ""),
                }
                break
            if assigned is None:
                assigned = {
                    "omop_internal_bucket": "no_structured_overlap",
                    "omop_collapsed_bucket": "no_structured_overlap",
                    "omop_temporal_window": "none",
                    "omop_semantic_match_level": "none",
                    "omop_matched_structured_drug": "",
                    "omop_matched_structured_date": "",
                    "omop_day_difference": "",
                    "omop_structured_source_visit_id": "",
                }
        assigned_rows.append({**row._asdict(), **assigned})

    omop = pd.DataFrame(assigned_rows)

    # Sensitivity subsets
    subsets = {
        "all_rows": omop,
        "mapped_note_rows": omop[omop["note_has_omop_mapping"]].copy(),
        "ingredient_or_category_covered_rows": omop[
            omop["note_has_ingredient_mapping"] & omop["note_has_category_mapping"]
        ].copy(),
        "exclude_unmapped_note_rows": omop[omop["note_mapping_status_omop"] != "unmapped_note_label"].copy(),
    }

    sensitivity_rows: List[Dict[str, object]] = []
    for subset_name, df in subsets.items():
        denom = len(df)
        for bucket in COLLAPSED_BUCKET_ORDER:
            count = int((df["omop_collapsed_bucket"] == bucket).sum()) if denom else 0
            sensitivity_rows.append(
                {
                    "subset": subset_name,
                    "collapsed_bucket": bucket,
                    "mention_rows": count,
                    "percent_of_subset": round(count / denom, 6) if denom else 0.0,
                    "denominator": denom,
                }
            )
    sensitivity_summary = pd.DataFrame(sensitivity_rows)

    collapsed_summary = (
        omop.groupby("omop_collapsed_bucket", dropna=False)
        .agg(
            mention_rows=("adjudication_unit_id", "count"),
            unique_visits=("visit_id", "nunique"),
            unique_patients=("person_id", "nunique"),
        )
        .reset_index()
    )
    collapsed_summary["percent_of_mentions"] = collapsed_summary["mention_rows"] / len(omop)

    internal_summary = (
        omop.groupby(["omop_internal_bucket", "omop_temporal_window", "omop_semantic_match_level"], dropna=False)
        .agg(
            mention_rows=("adjudication_unit_id", "count"),
            unique_visits=("visit_id", "nunique"),
            unique_patients=("person_id", "nunique"),
        )
        .reset_index()
    )
    internal_summary["percent_of_mentions"] = internal_summary["mention_rows"] / len(omop)

    # Manual audit packet from strongest no-overlap bucket
    no_overlap = omop[omop["omop_collapsed_bucket"] == "no_structured_overlap"].copy()
    if len(no_overlap) > args.audit_sample_size:
        no_overlap = no_overlap.sample(n=args.audit_sample_size, random_state=args.sample_seed)
    audit_cols = [
        c
        for c in [
            "adjudication_unit_id",
            "person_id",
            "visit_id",
            "note_id",
            "raw_mention_text",
            "context_text",
            "adjudicated_canonical_label",
            "action_cue",
            "note_title_final",
            "note_anchor_date",
            "note_mapping_status_omop",
            "note_standard_concept_name",
            "note_ingredient_name_norms",
            "note_category_label_norm",
            "omop_internal_bucket",
            "omop_collapsed_bucket",
        ]
        if c in no_overlap.columns
    ]
    audit_packet = no_overlap[audit_cols].copy()
    for col in [
        "review_valid_medication",
        "review_correct_canonical",
        "review_correct_action",
        "review_likely_true_note_only",
        "review_likely_pipeline_or_mapping_error",
        "review_confidence",
        "review_notes",
    ]:
        audit_packet[col] = ""

    outputs = {
        "detailed_csv": out_dir / "rq1_temporal_mismatch_ladder_omop_detailed.csv",
        "collapsed_summary_csv": out_dir / "rq1_temporal_mismatch_ladder_omop_collapsed_summary.csv",
        "internal_summary_csv": out_dir / "rq1_temporal_mismatch_ladder_omop_internal_summary.csv",
        "sensitivity_summary_csv": out_dir / "rq1_temporal_mismatch_ladder_omop_sensitivity_summary.csv",
        "audit_packet_csv": out_dir / "rq1_no_structured_overlap_omop_audit_packet.csv",
        "summary_json": out_dir / "rq1_temporal_mismatch_ladder_omop_summary.json",
    }

    omop.to_csv(outputs["detailed_csv"], index=False)
    collapsed_summary.to_csv(outputs["collapsed_summary_csv"], index=False)
    internal_summary.to_csv(outputs["internal_summary_csv"], index=False)
    sensitivity_summary.to_csv(outputs["sensitivity_summary_csv"], index=False)
    audit_packet.to_csv(outputs["audit_packet_csv"], index=False)

    write_run_summary(
        outputs["summary_json"],
        {
            "inputs": {
                "temporal_detailed_csv": str(detailed_path),
                "structured_mapping_csv": str(structured_map_path),
                "note_mapping_csv": str(note_map_path),
                "episode_drugs_dir": str(episode_drugs_dir),
            },
            "counts": {
                "all_rows": int(len(omop)),
                "mapped_note_rows": int(len(subsets["mapped_note_rows"])),
                "no_structured_overlap_rows": int((omop["omop_collapsed_bucket"] == "no_structured_overlap").sum()),
            },
            "outputs": {k: str(v) for k, v in outputs.items()},
        },
    )

    print(f"Saved OMOP-backed temporal sensitivity outputs to: {out_dir}")
    print(f"Rows analyzed: {len(omop):,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
