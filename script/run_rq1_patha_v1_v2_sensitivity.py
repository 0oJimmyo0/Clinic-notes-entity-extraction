#!/usr/bin/env python3
"""
Post-hoc Path A v1 vs v2 normalization sensitivity for BIBM framing.

Purpose:
- keep the primary paper identity as note-grounded evidence characterization
- test whether a bounded note-side alias refinement materially changes
  OMOP/RxNorm mapping coverage and the semantic+temporal mismatch ladder

This script does NOT create a new benchmark. It applies:
- Path A v1 = current deterministic alias map
- Path A v2 = v1 + a transparent supplement derived from reviewed
  note-side mapping failures
to the note-grounded reference labels, then compares:
- note-side OMOP/RxNorm mapping coverage
- semantic+temporal mismatch ladder distributions
- resolution of manually reviewed note-side mapping failures
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from rq1_adjudication_utils import write_run_summary
from rq1_drug_linking import load_alias_map, normalize_drug_text
from run_rq1_temporal_mismatch_ladder_omop_sensitivity import (
    COLLAPSED_BUCKET_ORDER,
    _build_indexes,
    _distance_days,
    _parse_pipe_set,
    _semantic_candidates,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run Path A v1 vs v2 normalization sensitivity.")
    p.add_argument(
        "--omop-detailed-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/temporal_mismatch_ladder_omop_sensitivity/rq1_temporal_mismatch_ladder_omop_detailed.csv",
    )
    p.add_argument(
        "--structured-mapping-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/omop_rxnorm_mapping/rq1_structured_concept_mapping.csv",
    )
    p.add_argument(
        "--episode-drugs-dir",
        default="../resources/struct_data/episode_drugs",
    )
    p.add_argument(
        "--note-mapping-base-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/omop_rxnorm_mapping/rq1_note_label_omop_rxnorm_mapping.csv",
    )
    p.add_argument(
        "--patha-v1-alias-artifact",
        default="../resources/manual/pathA_alias_map.json",
    )
    p.add_argument(
        "--patha-v2-supplement-csv",
        default="../resources/manual/pathA_v2_alias_supplement.csv",
    )
    p.add_argument(
        "--manual-review-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/temporal_mismatch_ladder_omop_sensitivity/manual_review_mismatch_bucket_annotated (1).csv",
    )
    p.add_argument(
        "--output-dir",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/patha_v1_v2_sensitivity",
    )
    return p.parse_args()


def _load_combined_alias_maps(v1_path: Path, v2_path: Path) -> Tuple[Dict[str, str], Dict[str, str]]:
    v1 = load_alias_map(v1_path) if v1_path.exists() else {}
    v2 = dict(v1)
    if v2_path.exists():
        v2.update(load_alias_map(v2_path))
    return v1, v2


def _apply_alias(labels: pd.Series, alias_map: Dict[str, str]) -> pd.Series:
    out = []
    for raw in labels.astype(str):
        base = normalize_drug_text(raw)
        out.append(alias_map.get(base, base) if base else "")
    return pd.Series(out, index=labels.index)


def _build_note_label_mapping(note_labels: Sequence[str], note_mapping_base_path: Path) -> pd.DataFrame:
    labels = pd.DataFrame({"note_label_norm": sorted(set(x for x in note_labels if x))})
    if labels.empty:
        return pd.DataFrame(columns=["note_label_norm", "standard_concept_id", "standard_concept_name", "ingredient_name_norms", "category_label_norm", "note_mapping_status"])

    base = pd.read_csv(note_mapping_base_path).fillna("")
    if "note_label_norm" not in base.columns:
        base["note_label_norm"] = base["note_label"].map(normalize_drug_text)
    keep = [
        "note_label_norm",
        "standard_concept_id",
        "standard_concept_name",
        "ingredient_name_norms",
        "category_label_norm",
        "note_mapping_status",
    ]
    base = base[keep].drop_duplicates("note_label_norm")
    out = labels.merge(base, on="note_label_norm", how="left")
    out["note_mapping_status"] = out["note_mapping_status"].replace("", pd.NA).fillna("unmapped_note_label")
    return out


def _prepare_structured_history(episode_drugs_dir: Path, structured_mapping_path: Path) -> pd.DataFrame:
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
    history = history[history["drug_concept_id"].notna()].copy()
    history["drug_concept_id"] = history["drug_concept_id"].astype("int64")
    history = history.rename(columns={"visit_occurrence_id": "structured_source_visit_id"})
    history["structured_exposure_start_date"] = pd.to_datetime(history["drug_exposure_start_date"], errors="coerce")
    history["structured_exposure_end_date"] = pd.to_datetime(history["drug_exposure_end_date"], errors="coerce")

    structured_map = pd.read_csv(structured_mapping_path).fillna("")
    structured_map["drug_concept_id"] = pd.to_numeric(structured_map["drug_concept_id"], errors="coerce").astype("Int64")
    structured_map["standard_concept_id"] = pd.to_numeric(structured_map["standard_concept_id"], errors="coerce")
    history = history.merge(
        structured_map[
            [
                "drug_concept_id",
                "standard_concept_id",
                "standard_concept_name",
                "ingredient_name_norms",
                "category_label_norm",
                "structured_mapping_status",
            ]
        ],
        on="drug_concept_id",
        how="left",
    )
    history["structured_ingredient_set"] = history["ingredient_name_norms"].map(_parse_pipe_set)
    history["standard_concept_id"] = pd.to_numeric(history["standard_concept_id"], errors="coerce")
    return history


def _compute_version_frame(
    detailed: pd.DataFrame,
    transformed_col: str,
    note_map: pd.DataFrame,
    history: pd.DataFrame,
) -> pd.DataFrame:
    df = detailed.copy()
    note_map = note_map.rename(
        columns={
            "standard_concept_id": f"{transformed_col}_standard_concept_id",
            "standard_concept_name": f"{transformed_col}_standard_concept_name",
            "ingredient_name_norms": f"{transformed_col}_ingredient_name_norms",
            "category_label_norm": f"{transformed_col}_category_label_norm",
            "note_mapping_status": f"{transformed_col}_mapping_status",
        }
    )
    df = df.merge(note_map, left_on=transformed_col, right_on="note_label_norm", how="left")
    df = df.drop(columns=["note_label_norm"], errors="ignore")
    std_col = f"{transformed_col}_standard_concept_id"
    ing_col = f"{transformed_col}_ingredient_name_norms"
    cat_col = f"{transformed_col}_category_label_norm"
    status_col = f"{transformed_col}_mapping_status"
    df[std_col] = pd.to_numeric(df[std_col], errors="coerce")
    df[ing_col] = df[ing_col].fillna("")
    df[cat_col] = df[cat_col].fillna("")
    df[status_col] = df[status_col].fillna("unmapped_note_label")
    df[f"{transformed_col}_ingredient_set"] = df[ing_col].map(_parse_pipe_set)
    df[f"{transformed_col}_ingredient_key"] = df[ing_col].astype(str)

    exact_index, ingredient_index, category_index = _build_indexes(history)
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
    unique_cols = [
        "person_id",
        "visit_id",
        "note_anchor_date",
        std_col,
        ing_col,
        cat_col,
    ]
    dedup = df[unique_cols].drop_duplicates().copy()
    bucket_rows: List[Dict[str, object]] = []
    for row in dedup.itertuples(index=False):
        anchor = pd.to_datetime(getattr(row, "note_anchor_date", ""), errors="coerce")
        note_row = pd.Series(
            {
                "person_id": str(getattr(row, "person_id")),
                "visit_id": getattr(row, "visit_id"),
                "note_standard_concept_id": getattr(row, std_col),
                "note_ingredient_set": _parse_pipe_set(getattr(row, ing_col)),
                "note_category_label_norm": getattr(row, cat_col),
            }
        )
        bucket = {
            "omop_internal_bucket": "no_structured_overlap",
            "omop_collapsed_bucket": "no_structured_overlap",
            "omop_temporal_window": "none",
            "omop_semantic_match_level": "none",
            "omop_matched_structured_drug": "",
            "omop_matched_structured_date": "",
            "omop_day_difference": "",
            "omop_structured_source_visit_id": "",
        }
        for window_label, max_days, semantic_level, internal_bucket, collapsed_bucket in checks:
            candidates = _semantic_candidates(note_row, semantic_level, exact_index, ingredient_index, category_index)
            if candidates.empty:
                continue
            if window_label == "same_visit":
                candidates = candidates[candidates["structured_source_visit_id"] == note_row["visit_id"]].copy()
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
            bucket = {
                "omop_internal_bucket": internal_bucket,
                "omop_collapsed_bucket": collapsed_bucket,
                "omop_temporal_window": window_label,
                "omop_semantic_match_level": semantic_level,
                "omop_matched_structured_drug": best.get("drug_source_value", ""),
                "omop_matched_structured_date": best_date.date().isoformat() if pd.notna(best_date) else "",
                "omop_day_difference": int(best["day_difference"]) if pd.notna(best["day_difference"]) else "",
                "omop_structured_source_visit_id": best.get("structured_source_visit_id", ""),
            }
            break
        for col in unique_cols:
            bucket[col] = getattr(row, col)
        bucket_rows.append(bucket)
    bucket_df = pd.DataFrame(bucket_rows)
    bucket_df = bucket_df.rename(
        columns={
            "omop_internal_bucket": f"{transformed_col}_internal_bucket",
            "omop_collapsed_bucket": f"{transformed_col}_collapsed_bucket",
            "omop_temporal_window": f"{transformed_col}_temporal_window",
            "omop_semantic_match_level": f"{transformed_col}_semantic_match_level",
            "omop_matched_structured_drug": f"{transformed_col}_matched_structured_drug",
            "omop_matched_structured_date": f"{transformed_col}_matched_structured_date",
            "omop_day_difference": f"{transformed_col}_day_difference",
            "omop_structured_source_visit_id": f"{transformed_col}_structured_source_visit_id",
        }
    )
    return df.merge(bucket_df, on=unique_cols, how="left")


def _coverage_summary(df: pd.DataFrame, version: str, label_col: str) -> pd.DataFrame:
    mapping_status = f"{label_col}_mapping_status"
    std_col = f"{label_col}_standard_concept_id"
    ing_col = f"{label_col}_ingredient_name_norms"
    cat_col = f"{label_col}_category_label_norm"
    out = [
        {"version": version, "metric": "mention_rows", "value": len(df)},
        {"version": version, "metric": "unique_transformed_labels", "value": int(df[label_col].nunique())},
        {"version": version, "metric": "mapped_rows", "value": int(df[std_col].notna().sum())},
        {"version": version, "metric": "mapped_row_rate", "value": round(float(df[std_col].notna().mean()), 6)},
        {"version": version, "metric": "ingredient_covered_rows", "value": int(df[ing_col].astype(str).ne("").sum())},
        {"version": version, "metric": "ingredient_covered_row_rate", "value": round(float(df[ing_col].astype(str).ne("").mean()), 6)},
        {"version": version, "metric": "category_covered_rows", "value": int(df[cat_col].astype(str).ne("").sum())},
        {"version": version, "metric": "category_covered_row_rate", "value": round(float(df[cat_col].astype(str).ne("").mean()), 6)},
        {"version": version, "metric": "unmapped_rows", "value": int(df[mapping_status].eq("unmapped_note_label").sum())},
        {"version": version, "metric": "unmapped_row_rate", "value": round(float(df[mapping_status].eq("unmapped_note_label").mean()), 6)},
    ]
    return pd.DataFrame(out)


def _ladder_summary(df: pd.DataFrame, version: str, bucket_col: str) -> pd.DataFrame:
    counts = df[bucket_col].value_counts().to_dict()
    rows = []
    for bucket in COLLAPSED_BUCKET_ORDER:
        n = int(counts.get(bucket, 0))
        rows.append(
            {
                "version": version,
                "collapsed_bucket": bucket,
                "mention_rows": n,
                "percent_rows": round(n / len(df), 6) if len(df) else 0.0,
            }
        )
    return pd.DataFrame(rows)


def _transition_summary(df: pd.DataFrame, v1_bucket_col: str, v2_bucket_col: str) -> pd.DataFrame:
    out = (
        df.groupby([v1_bucket_col, v2_bucket_col], dropna=False)
        .size()
        .reset_index(name="mention_rows")
        .sort_values("mention_rows", ascending=False)
    )
    out["percent_rows"] = (out["mention_rows"] / len(df)).round(6) if len(df) else 0.0
    return out


def _review_resolution(review_df: pd.DataFrame, label_map: pd.DataFrame) -> pd.DataFrame:
    lookup = (
        label_map.sort_values(["adjudicated_canonical_label"])
        .drop_duplicates(subset=["adjudicated_canonical_label"])[
            [
                "adjudicated_canonical_label",
                "patha_v1_label_norm",
                "patha_v2_label_norm",
                "patha_v1_label_norm_mapping_status",
                "patha_v2_label_norm_mapping_status",
                "patha_v1_label_norm_collapsed_bucket",
                "patha_v2_label_norm_collapsed_bucket",
            ]
        ]
    )
    merged = review_df.merge(
        lookup,
        on="adjudicated_canonical_label",
        how="left",
    )
    merged["resolved_by_v2"] = (
        merged["patha_v1_label_norm_mapping_status"].eq("unmapped_note_label")
        & merged["patha_v2_label_norm_mapping_status"].ne("unmapped_note_label")
    )
    summary = (
        merged.groupby(
            [
                "primary_root_cause",
                "patha_v1_label_norm_mapping_status",
                "patha_v2_label_norm_mapping_status",
                "resolved_by_v2",
            ],
            dropna=False,
        )
        .size()
        .reset_index(name="review_rows")
        .sort_values(["primary_root_cause", "review_rows"], ascending=[True, False])
    )
    return merged, summary


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]

    detailed_path = (root / args.omop_detailed_csv).resolve()
    structured_map_path = (root / args.structured_mapping_csv).resolve()
    episode_drugs_dir = (root / args.episode_drugs_dir).resolve()
    note_mapping_base_path = (root / args.note_mapping_base_csv).resolve()
    v1_alias_path = (root / args.patha_v1_alias_artifact).resolve()
    v2_supplement_path = (root / args.patha_v2_supplement_csv).resolve()
    review_path = (root / args.manual_review_csv).resolve()
    out_dir = (root / args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Loading OMOP-backed baseline detailed ladder...", flush=True)
    detailed = pd.read_csv(detailed_path).fillna("")
    detailed["note_anchor_date"] = pd.to_datetime(detailed["note_anchor_date"], errors="coerce")
    detailed["visit_start_date"] = pd.to_datetime(detailed["visit_start_date"], errors="coerce")
    detailed["note_anchor_date"] = detailed["note_anchor_date"].fillna(detailed["visit_start_date"])

    print("Loading Path A alias artifacts...", flush=True)
    v1_alias, v2_alias = _load_combined_alias_maps(v1_alias_path, v2_supplement_path)
    detailed["base_label_norm"] = detailed["adjudicated_canonical_label"].map(normalize_drug_text)
    detailed["patha_v1_label_norm"] = _apply_alias(detailed["adjudicated_canonical_label"], v1_alias)
    detailed["patha_v2_label_norm"] = _apply_alias(detailed["adjudicated_canonical_label"], v2_alias)
    detailed["patha_v2_changed_label"] = detailed["patha_v1_label_norm"] != detailed["patha_v2_label_norm"]

    print("Loading structured medication history...", flush=True)
    history = _prepare_structured_history(episode_drugs_dir, structured_map_path)

    detailed["base_mapping_status"] = detailed["note_mapping_status_omop"].astype(str)
    detailed["base_standard_concept_id"] = pd.to_numeric(detailed["note_standard_concept_id"], errors="coerce")
    detailed["base_standard_concept_name"] = detailed["note_standard_concept_name"].astype(str)
    detailed["base_ingredient_name_norms"] = detailed["note_ingredient_name_norms"].astype(str)
    detailed["base_category_label_norm"] = detailed["note_category_label_norm"].astype(str)
    detailed["base_collapsed_bucket"] = detailed["omop_collapsed_bucket"].astype(str)
    detailed["base_internal_bucket"] = detailed["omop_internal_bucket"].astype(str)

    changed_v1 = detailed["patha_v1_label_norm"] != detailed["base_label_norm"]
    changed_v2 = detailed["patha_v2_label_norm"] != detailed["patha_v1_label_norm"]
    print(
        f"Changed rows: v1={int(changed_v1.sum())}, v2={int(changed_v2.sum())}; "
        f"unique v2 changed labels={int(detailed.loc[changed_v2, 'adjudicated_canonical_label'].nunique())}",
        flush=True,
    )

    union_labels = sorted(
        set(detailed.loc[changed_v1, "patha_v1_label_norm"].astype(str))
        | set(detailed.loc[changed_v2, "patha_v2_label_norm"].astype(str))
    )
    union_labels = [x for x in union_labels if x]
    print(f"Building note-label mapping lookup for {len(union_labels)} transformed labels...", flush=True)
    note_map = _build_note_label_mapping(union_labels, note_mapping_base_path) if union_labels else pd.DataFrame()
    note_map.to_csv(out_dir / "rq1_patha_v1_v2_note_label_mapping.csv", index=False)

    v1_df = detailed.copy()
    v1_df["patha_v1_label_norm_standard_concept_id"] = v1_df["base_standard_concept_id"]
    v1_df["patha_v1_label_norm_mapping_status"] = v1_df["base_mapping_status"]
    v1_df["patha_v1_label_norm_standard_concept_name"] = v1_df["base_standard_concept_name"]
    v1_df["patha_v1_label_norm_ingredient_name_norms"] = v1_df["base_ingredient_name_norms"]
    v1_df["patha_v1_label_norm_category_label_norm"] = v1_df["base_category_label_norm"]
    v1_df["patha_v1_label_norm_collapsed_bucket"] = v1_df["base_collapsed_bucket"]
    v1_df["patha_v1_label_norm_internal_bucket"] = v1_df["base_internal_bucket"]
    if changed_v1.any():
        print(f"Recomputing OMOP ladder for {int(changed_v1.sum())} Path A v1-changed rows...", flush=True)
        recomputed_v1 = _compute_version_frame(detailed.loc[changed_v1].copy(), "patha_v1_label_norm", note_map, history)
        v1_cols = [
            "patha_v1_label_norm_standard_concept_id",
            "patha_v1_label_norm_mapping_status",
            "patha_v1_label_norm_standard_concept_name",
            "patha_v1_label_norm_ingredient_name_norms",
            "patha_v1_label_norm_category_label_norm",
            "patha_v1_label_norm_collapsed_bucket",
            "patha_v1_label_norm_internal_bucket",
        ]
        v1_df.loc[changed_v1, v1_cols] = recomputed_v1[v1_cols].values

    v2_df = v1_df.copy()
    v2_df["patha_v2_label_norm_standard_concept_id"] = v2_df["patha_v1_label_norm_standard_concept_id"]
    v2_df["patha_v2_label_norm_mapping_status"] = v2_df["patha_v1_label_norm_mapping_status"]
    v2_df["patha_v2_label_norm_standard_concept_name"] = v2_df["patha_v1_label_norm_standard_concept_name"]
    v2_df["patha_v2_label_norm_ingredient_name_norms"] = v2_df["patha_v1_label_norm_ingredient_name_norms"]
    v2_df["patha_v2_label_norm_category_label_norm"] = v2_df["patha_v1_label_norm_category_label_norm"]
    v2_df["patha_v2_label_norm_collapsed_bucket"] = v2_df["patha_v1_label_norm_collapsed_bucket"]
    v2_df["patha_v2_label_norm_internal_bucket"] = v2_df["patha_v1_label_norm_internal_bucket"]
    if changed_v2.any():
        print(f"Recomputing OMOP ladder for {int(changed_v2.sum())} Path A v2-changed rows...", flush=True)
        recomputed_v2 = _compute_version_frame(v1_df.loc[changed_v2].copy(), "patha_v2_label_norm", note_map, history)
        v2_cols = [
            "patha_v2_label_norm_standard_concept_id",
            "patha_v2_label_norm_mapping_status",
            "patha_v2_label_norm_standard_concept_name",
            "patha_v2_label_norm_ingredient_name_norms",
            "patha_v2_label_norm_category_label_norm",
            "patha_v2_label_norm_collapsed_bucket",
            "patha_v2_label_norm_internal_bucket",
        ]
        v2_df.loc[changed_v2, v2_cols] = recomputed_v2[v2_cols].values

    keep_cols = [
        "adjudication_unit_id",
        "person_id",
        "visit_id",
        "note_id",
        "adjudicated_canonical_label",
        "action_cue",
        "drug_class",
        "note_title_final",
        "note_anchor_date",
        "base_label_norm",
        "patha_v1_label_norm",
        "patha_v1_label_norm_mapping_status",
        "patha_v1_label_norm_standard_concept_name",
        "patha_v1_label_norm_ingredient_name_norms",
        "patha_v1_label_norm_category_label_norm",
        "patha_v1_label_norm_collapsed_bucket",
        "patha_v1_label_norm_internal_bucket",
        "patha_v2_label_norm",
        "patha_v2_label_norm_mapping_status",
        "patha_v2_label_norm_standard_concept_name",
        "patha_v2_label_norm_ingredient_name_norms",
        "patha_v2_label_norm_category_label_norm",
        "patha_v2_label_norm_collapsed_bucket",
        "patha_v2_label_norm_internal_bucket",
        "patha_v2_changed_label",
    ]
    combined = v1_df[keep_cols[:16]].copy()
    combined = combined.merge(
        v2_df[
            [
                "adjudication_unit_id",
                "patha_v2_label_norm",
                "patha_v2_label_norm_mapping_status",
                "patha_v2_label_norm_standard_concept_name",
                "patha_v2_label_norm_ingredient_name_norms",
                "patha_v2_label_norm_category_label_norm",
                "patha_v2_label_norm_collapsed_bucket",
                "patha_v2_label_norm_internal_bucket",
            ]
        ],
        on="adjudication_unit_id",
        how="left",
    )
    combined["patha_v2_changed_label"] = combined["patha_v1_label_norm"] != combined["patha_v2_label_norm"]
    combined.to_csv(out_dir / "rq1_patha_v1_v2_detailed.csv", index=False)

    coverage = pd.concat(
        [
            _coverage_summary(v1_df, "patha_v1", "patha_v1_label_norm"),
            _coverage_summary(v2_df, "patha_v2", "patha_v2_label_norm"),
        ],
        ignore_index=True,
    )
    coverage.to_csv(out_dir / "rq1_patha_v1_v2_mapping_coverage.csv", index=False)

    ladder = pd.concat(
        [
            _ladder_summary(v1_df, "patha_v1", "patha_v1_label_norm_collapsed_bucket"),
            _ladder_summary(v2_df, "patha_v2", "patha_v2_label_norm_collapsed_bucket"),
        ],
        ignore_index=True,
    )
    ladder.to_csv(out_dir / "rq1_patha_v1_v2_temporal_ladder_summary.csv", index=False)

    transitions = _transition_summary(
        combined,
        "patha_v1_label_norm_collapsed_bucket",
        "patha_v2_label_norm_collapsed_bucket",
    )
    transitions.to_csv(out_dir / "rq1_patha_v1_v2_bucket_transitions.csv", index=False)

    changed_labels = (
        combined[combined["patha_v2_changed_label"]]
        .groupby(["adjudicated_canonical_label", "patha_v1_label_norm", "patha_v2_label_norm"], dropna=False)
        .size()
        .reset_index(name="mention_rows")
        .sort_values("mention_rows", ascending=False)
    )
    changed_labels.to_csv(out_dir / "rq1_patha_v1_v2_changed_labels.csv", index=False)

    review_df = pd.read_csv(review_path).fillna("")
    review_rows, review_summary = _review_resolution(review_df, combined)
    review_rows.to_csv(out_dir / "rq1_patha_v1_v2_reviewed_failure_rows.csv", index=False)
    review_summary.to_csv(out_dir / "rq1_patha_v1_v2_reviewed_failure_resolution.csv", index=False)

    def _metric(df: pd.DataFrame, version: str, metric: str) -> float:
        return float(df[(df["version"] == version) & (df["metric"] == metric)]["value"].iloc[0])

    def _bucket(df: pd.DataFrame, version: str, bucket: str, column: str) -> float:
        return float(df[(df["version"] == version) & (df["collapsed_bucket"] == bucket)][column].iloc[0])

    compact = pd.DataFrame(
        [
            {
                "comparison": "mapped_row_rate",
                "patha_v1": _metric(coverage, "patha_v1", "mapped_row_rate"),
                "patha_v2": _metric(coverage, "patha_v2", "mapped_row_rate"),
            },
            {
                "comparison": "ingredient_covered_row_rate",
                "patha_v1": _metric(coverage, "patha_v1", "ingredient_covered_row_rate"),
                "patha_v2": _metric(coverage, "patha_v2", "ingredient_covered_row_rate"),
            },
            {
                "comparison": "category_covered_row_rate",
                "patha_v1": _metric(coverage, "patha_v1", "category_covered_row_rate"),
                "patha_v2": _metric(coverage, "patha_v2", "category_covered_row_rate"),
            },
            {
                "comparison": "no_structured_overlap_rows",
                "patha_v1": _bucket(ladder, "patha_v1", "no_structured_overlap", "mention_rows"),
                "patha_v2": _bucket(ladder, "patha_v2", "no_structured_overlap", "mention_rows"),
            },
            {
                "comparison": "no_structured_overlap_rate",
                "patha_v1": _bucket(ladder, "patha_v1", "no_structured_overlap", "percent_rows"),
                "patha_v2": _bucket(ladder, "patha_v2", "no_structured_overlap", "percent_rows"),
            },
            {
                "comparison": "same_visit_exact_rate",
                "patha_v1": _bucket(ladder, "patha_v1", "same_visit_exact", "percent_rows"),
                "patha_v2": _bucket(ladder, "patha_v2", "same_visit_exact", "percent_rows"),
            },
        ]
    )
    compact["absolute_delta"] = pd.to_numeric(compact["patha_v2"]) - pd.to_numeric(compact["patha_v1"])
    compact.to_csv(out_dir / "rq1_patha_v1_v2_compact_summary.csv", index=False)

    note_side_review = review_summary[review_summary["primary_root_cause"] == "note-side mapping failure"].copy()
    resolved_review_rows = int(note_side_review[note_side_review["resolved_by_v2"] == True]["review_rows"].sum())
    total_review_rows = int(note_side_review["review_rows"].sum())
    review_compact = pd.DataFrame(
        [
            {
                "review_group": "note-side mapping failure",
                "resolved_review_rows": resolved_review_rows,
                "total_review_rows": total_review_rows,
                "resolved_rate": round(resolved_review_rows / total_review_rows, 6) if total_review_rows else 0.0,
            }
        ]
    )
    review_compact.to_csv(out_dir / "rq1_patha_v2_review_resolution_compact.csv", index=False)

    supplement_df = pd.read_csv(v2_supplement_path).fillna("")
    supplement_df.to_csv(out_dir / "rq1_patha_v2_alias_supplement_effective.csv", index=False)

    print("Writing Path A v1 vs v2 outputs...", flush=True)
    write_run_summary(
        out_dir / "rq1_patha_v1_v2_summary.json",
        {
            "inputs": {
                "omop_detailed_csv": str(detailed_path),
                "structured_mapping_csv": str(structured_map_path),
                "episode_drugs_dir": str(episode_drugs_dir),
                "note_mapping_base_csv": str(note_mapping_base_path),
                "patha_v1_alias_artifact": str(v1_alias_path),
                "patha_v2_supplement_csv": str(v2_supplement_path),
                "manual_review_csv": str(review_path),
            },
            "patha_v1_alias_count": len(v1_alias),
            "patha_v2_alias_count": len(v2_alias),
            "patha_v2_added_alias_count": max(len(v2_alias) - len(v1_alias), 0),
            "outputs": sorted(p.name for p in out_dir.iterdir() if p.is_file()),
        },
    )

    print(f"Saved Path A v1 vs v2 sensitivity outputs to: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
