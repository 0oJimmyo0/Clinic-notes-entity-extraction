#!/usr/bin/env python3
"""
Build a semantic + temporal mismatch ladder between note-derived medication evidence
and structured EHR medication history.
"""

from __future__ import annotations

import argparse
import math
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import numpy as np
import pandas as pd

from rq1_adjudication_utils import write_run_summary
from rq1_bibm_utils import (
    broad_drug_category,
    ingredient_level_label,
    load_brand_generic_map,
    safe_div,
)
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
    p = argparse.ArgumentParser(description="Build semantic + temporal mismatch ladder for BIBM.")
    p.add_argument(
        "--note-detailed-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/note_only_evidence_bibm_test/rq1_note_only_evidence_detailed.csv",
    )
    p.add_argument(
        "--packets-mentions-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/adjudication_packets/adjudication_packets_mentions.csv",
    )
    p.add_argument(
        "--visit-timeline-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/rq1_visit_timeline.csv",
    )
    p.add_argument(
        "--episode-drugs-dir",
        default="../resources/struct_data/episode_drugs",
    )
    p.add_argument(
        "--alias-artifacts",
        default="../resources/manual/pathA_alias_map.json,../resources/lexicons/rq1_drug_aliases.json",
    )
    p.add_argument(
        "--no-overlap-sample-size",
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
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/temporal_mismatch_ladder",
    )
    return p.parse_args()


def _normalize(df: pd.DataFrame, col: str) -> None:
    if col in df.columns:
        df[col] = df[col].fillna("").astype(str).str.strip()


def _read_episode_drugs(dir_path: Path, person_ids: set[str]) -> pd.DataFrame:
    files = sorted(list(dir_path.glob("*.parquet")) + list(dir_path.glob("*.csv")))
    if not files:
        raise FileNotFoundError(f"No structured drug files found in: {dir_path}")

    keep_cols = [
        "person_id",
        "visit_occurrence_id",
        "drug_exposure_id",
        "drug_exposure_start_date",
        "drug_exposure_end_date",
        "drug_exposure_start_datetime",
        "drug_exposure_end_datetime",
        "drug_concept_id",
        "drug_source_value",
    ]
    parts: List[pd.DataFrame] = []
    for f in files:
        if f.suffix.lower() == ".parquet":
            df = pd.read_parquet(f)
        else:
            df = pd.read_csv(f)
        cols = [c for c in keep_cols if c in df.columns]
        if not cols:
            continue
        df = df[cols].copy()
        _normalize(df, "person_id")
        if person_ids:
            df = df[df["person_id"].isin(person_ids)].copy()
        if len(df):
            parts.append(df)
    if not parts:
        return pd.DataFrame(columns=keep_cols)
    out = pd.concat(parts, ignore_index=True)
    out = out.drop_duplicates()
    return out


def _date_distance_days(anchor: pd.Timestamp, start: pd.Timestamp, end: pd.Timestamp | None) -> float:
    if pd.isna(anchor) or pd.isna(start):
        return math.inf
    if end is not None and not pd.isna(end):
        if start <= anchor <= end:
            return 0.0
        return float(min(abs((anchor - start).days), abs((anchor - end).days)))
    return float(abs((anchor - start).days))


def _distance_series(
    anchor: pd.Timestamp,
    start_series: pd.Series,
    end_series: pd.Series,
) -> pd.Series:
    if pd.isna(anchor):
        return pd.Series(np.inf, index=start_series.index, dtype="float64")

    start = pd.to_datetime(start_series, errors="coerce")
    end = pd.to_datetime(end_series, errors="coerce")
    anchor_ts = pd.Timestamp(anchor)

    start_diff = (start - anchor_ts).dt.days.abs()
    end_diff = (end - anchor_ts).dt.days.abs()
    within_interval = start.notna() & end.notna() & (start <= anchor_ts) & (anchor_ts <= end)

    distances = start_diff.astype("float64")
    distances = distances.where(start.notna(), np.inf)
    distances = distances.where(end.isna(), np.minimum(distances, end_diff.astype("float64")))
    distances = distances.where(~within_interval, 0.0)
    return distances


def _semantic_subset(history: pd.DataFrame, semantic_level: str, note_exact: str, note_ing: str, note_cat: str) -> pd.DataFrame:
    if history.empty:
        return history
    if semantic_level == "exact":
        if not note_exact:
            return history.iloc[0:0].copy()
        return history[history["structured_exact_norm"] == note_exact].copy()
    if semantic_level == "ingredient":
        if not note_ing:
            return history.iloc[0:0].copy()
        return history[history["structured_ingredient_norm"] == note_ing].copy()
    if semantic_level == "category":
        if not note_cat or note_cat == "unknown":
            return history.iloc[0:0].copy()
        return history[history["structured_category"] == note_cat].copy()
    raise ValueError(f"Unknown semantic level: {semantic_level}")


def _candidate_rows(
    history: pd.DataFrame,
    note_exact: str,
    note_ing: str,
    note_cat: str,
    anchor_date: pd.Timestamp,
    *,
    semantic_level: str,
    visit_id: str,
    same_visit_only: bool = False,
    max_days: Optional[int] = None,
) -> pd.DataFrame:
    work = _semantic_subset(history, semantic_level, note_exact, note_ing, note_cat)
    if work.empty:
        return work
    if same_visit_only:
        work = work[work["structured_source_visit_id"] == visit_id].copy()
        if work.empty:
            return work
    work["day_difference"] = _distance_series(
        anchor_date,
        work["structured_exposure_start_date"],
        work["structured_exposure_end_date"],
    )
    if max_days is not None:
        work = work[work["day_difference"] <= max_days].copy()
    return work


def _pick_best(
    candidates: pd.DataFrame,
    semantic_level: str,
) -> Optional[pd.Series]:
    if candidates.empty:
        return None
    sub = candidates.sort_values(
        by=["day_difference", "structured_exposure_start_date", "structured_source_visit_id", "drug_source_value"],
        ascending=[True, True, True, True],
        na_position="last",
    )
    return sub.iloc[0]


def _assign_bucket(
    history: pd.DataFrame,
    note_exact: str,
    note_ing: str,
    note_cat: str,
    anchor_date: pd.Timestamp,
    visit_id: str,
) -> Dict[str, object]:
    if history.empty:
        return {
            "final_internal_bucket": "no_structured_overlap",
            "collapsed_bucket": "no_structured_overlap",
            "temporal_window": "none",
            "semantic_match_level": "none",
            "matched_structured_drug": "",
            "matched_structured_date": "",
            "day_difference": "",
            "structured_source_visit_id": "",
        }

    checks = [
        ("same_visit", None, "exact", "same_visit_exact", "same_visit_exact"),
        ("same_visit", None, "ingredient", "same_visit_ingredient", "same_visit_ingredient_only"),
        ("same_visit", None, "category", "same_visit_category", "same_visit_category_only"),
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

    for window_label, max_days, semantic, internal_bucket, collapsed_bucket in checks:
        candidates = _candidate_rows(
            history,
            note_exact,
            note_ing,
            note_cat,
            anchor_date,
            semantic_level=semantic,
            visit_id=visit_id,
            same_visit_only=(window_label == "same_visit"),
            max_days=max_days,
        )
        match = _pick_best(candidates, semantic)
        if match is not None:
            matched_date = match["structured_exposure_start_date"]
            matched_date_str = (
                pd.to_datetime(matched_date).date().isoformat()
                if not pd.isna(pd.to_datetime(matched_date, errors="coerce"))
                else ""
            )
            day_diff = match["day_difference"]
            return {
                "final_internal_bucket": internal_bucket,
                "collapsed_bucket": collapsed_bucket,
                "temporal_window": window_label,
                "semantic_match_level": semantic,
                "matched_structured_drug": match.get("structured_source_value", ""),
                "matched_structured_date": matched_date_str,
                "day_difference": int(day_diff) if pd.notna(day_diff) and day_diff != math.inf else "",
                "structured_source_visit_id": match.get("structured_source_visit_id", ""),
            }

    return {
        "final_internal_bucket": "no_structured_overlap",
        "collapsed_bucket": "no_structured_overlap",
        "temporal_window": "none",
        "semantic_match_level": "none",
        "matched_structured_drug": "",
        "matched_structured_date": "",
        "day_difference": "",
        "structured_source_visit_id": "",
    }


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]

    note_path = (root / args.note_detailed_csv).resolve()
    packets_path = (root / args.packets_mentions_csv).resolve()
    timeline_path = (root / args.visit_timeline_csv).resolve()
    episode_drugs_dir = (root / args.episode_drugs_dir).resolve()
    out_dir = (root / args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    alias_paths = [(root / x.strip()).resolve() for x in str(args.alias_artifacts).split(",") if x.strip()]
    brand_generic_map = load_brand_generic_map(alias_paths)

    notes = pd.read_csv(note_path).fillna("")
    packets = pd.read_csv(packets_path).fillna("")
    timeline = pd.read_csv(timeline_path).fillna("")

    for df in [notes, packets, timeline]:
        for col in ["adjudication_unit_id", "person_id", "visit_id", "note_id"]:
            _normalize(df, col)

    note_cols = [
        c
        for c in [
            "adjudication_unit_id",
            "person_id",
            "visit_id",
            "note_id",
            "adjudicated_canonical_label",
            "note_canonical_norm",
            "note_ingredient_norm",
            "drug_class",
            "action_cue",
            "note_title_final",
            "candidate_category",
            "semantic_mismatch_bucket",
        ]
        if c in notes.columns
    ]
    work = notes[note_cols].copy()

    packet_dates = packets[[c for c in ["adjudication_unit_id", "note_date"] if c in packets.columns]].copy()
    work = work.merge(packet_dates, on="adjudication_unit_id", how="left")

    timeline_small = timeline[[c for c in ["person_id", "visit_id", "visit_start_date"] if c in timeline.columns]].copy()
    work = work.merge(timeline_small, on=["person_id", "visit_id"], how="left")

    work["note_anchor_date"] = pd.to_datetime(work.get("note_date", ""), errors="coerce")
    fallback_dates = pd.to_datetime(work.get("visit_start_date", ""), errors="coerce")
    work["note_anchor_date"] = work["note_anchor_date"].fillna(fallback_dates)
    work["note_anchor_date_str"] = work["note_anchor_date"].map(lambda x: x.date().isoformat() if pd.notna(x) else "")

    if "note_canonical_norm" not in work.columns:
        work["note_canonical_norm"] = work["adjudicated_canonical_label"].map(normalize_drug_text)
    if "note_ingredient_norm" not in work.columns:
        work["note_ingredient_norm"] = work["adjudicated_canonical_label"].map(
            lambda x: ingredient_level_label(x, brand_generic_map)
        )
    if "drug_class" not in work.columns:
        work["drug_class"] = work["adjudicated_canonical_label"].map(broad_drug_category)

    person_ids = set(work["person_id"].astype(str))
    structured = _read_episode_drugs(episode_drugs_dir, person_ids)
    if structured.empty:
        raise RuntimeError("No structured drug rows found for note-side person IDs.")

    _normalize(structured, "visit_occurrence_id")
    structured = structured.rename(columns={"visit_occurrence_id": "structured_source_visit_id"})
    structured["structured_exact_norm"] = structured["drug_source_value"].map(normalize_drug_text)
    structured["structured_ingredient_norm"] = structured["structured_exact_norm"].map(
        lambda x: ingredient_level_label(x, brand_generic_map)
    )
    structured["structured_category"] = structured["structured_ingredient_norm"].map(broad_drug_category)
    structured["structured_exposure_start_date"] = pd.to_datetime(
        structured.get("drug_exposure_start_date", ""), errors="coerce"
    )
    structured["structured_exposure_end_date"] = pd.to_datetime(
        structured.get("drug_exposure_end_date", ""), errors="coerce"
    )
    structured = structured[
        (structured["structured_exact_norm"].astype(str).str.strip() != "")
        | (structured["structured_ingredient_norm"].astype(str).str.strip() != "")
    ].copy()

    note_map = (
        work[["adjudicated_canonical_label", "note_canonical_norm", "note_ingredient_norm", "drug_class"]]
        .drop_duplicates()
        .sort_values(["adjudicated_canonical_label", "note_canonical_norm"])
        .reset_index(drop=True)
    )

    structured_map = (
        structured[
            [
                "drug_concept_id",
                "drug_source_value",
                "structured_exact_norm",
                "structured_ingredient_norm",
                "structured_category",
            ]
        ]
        .copy()
    )
    structured_map["row_count"] = 1
    structured_map = (
        structured_map.groupby(
            [
                "drug_concept_id",
                "drug_source_value",
                "structured_exact_norm",
                "structured_ingredient_norm",
                "structured_category",
            ],
            dropna=False,
            as_index=False,
        )["row_count"]
        .sum()
        .sort_values("row_count", ascending=False)
    )

    history_by_person = {
        pid: grp.copy()
        for pid, grp in structured.groupby("person_id", dropna=False)
    }

    assigned_rows: List[Dict[str, object]] = []
    for row in work.itertuples(index=False):
        history = history_by_person.get(str(row.person_id), pd.DataFrame())
        assigned = _assign_bucket(
            history,
            str(row.note_canonical_norm),
            str(row.note_ingredient_norm),
            str(row.drug_class),
            row.note_anchor_date,
            str(row.visit_id),
        )
        assigned_rows.append(
            {
                **row._asdict(),
                **assigned,
                "note_anchor_date": row.note_anchor_date_str,
            }
        )

    detailed = pd.DataFrame(assigned_rows)
    detailed["internal_bucket_order"] = detailed["final_internal_bucket"].map(
        {k: i for i, k in enumerate(INTERNAL_BUCKET_ORDER)}
    )
    detailed["collapsed_bucket_order"] = detailed["collapsed_bucket"].map(
        {k: i for i, k in enumerate(COLLAPSED_BUCKET_ORDER)}
    )

    internal_summary = (
        detailed.groupby(["final_internal_bucket", "temporal_window", "semantic_match_level"], dropna=False)
        .agg(
            mention_rows=("adjudication_unit_id", "count"),
            unique_visits=("visit_id", "nunique"),
            unique_patients=("person_id", "nunique"),
            unique_medications=("note_canonical_norm", "nunique"),
        )
        .reset_index()
    )
    internal_summary["percent_of_mentions"] = internal_summary["mention_rows"].map(
        lambda x: round(safe_div(x, len(detailed)), 6)
    )
    internal_summary["bucket_order"] = internal_summary["final_internal_bucket"].map(
        {k: i for i, k in enumerate(INTERNAL_BUCKET_ORDER)}
    )
    internal_summary = internal_summary.sort_values(["bucket_order"]).drop(columns=["bucket_order"])

    collapsed_summary = (
        detailed.groupby("collapsed_bucket", dropna=False)
        .agg(
            mention_rows=("adjudication_unit_id", "count"),
            unique_visits=("visit_id", "nunique"),
            unique_patients=("person_id", "nunique"),
            unique_medications=("note_canonical_norm", "nunique"),
        )
        .reset_index()
    )
    collapsed_summary["percent_of_mentions"] = collapsed_summary["mention_rows"].map(
        lambda x: round(safe_div(x, len(detailed)), 6)
    )
    collapsed_summary["bucket_order"] = collapsed_summary["collapsed_bucket"].map(
        {k: i for i, k in enumerate(COLLAPSED_BUCKET_ORDER)}
    )
    collapsed_summary = collapsed_summary.sort_values(["bucket_order"]).drop(columns=["bucket_order"])

    by_action = (
        detailed.groupby(["action_cue", "collapsed_bucket"], dropna=False)
        .size()
        .reset_index(name="mention_rows")
        .sort_values(["action_cue", "mention_rows"], ascending=[True, False])
    )
    by_note_type = (
        detailed.groupby(["note_title_final", "collapsed_bucket"], dropna=False)
        .size()
        .reset_index(name="mention_rows")
        .sort_values(["note_title_final", "mention_rows"], ascending=[True, False])
    )
    by_drug_class = (
        detailed.groupby(["drug_class", "collapsed_bucket"], dropna=False)
        .size()
        .reset_index(name="mention_rows")
        .sort_values(["drug_class", "mention_rows"], ascending=[True, False])
    )

    examples = (
        detailed.groupby(
            ["final_internal_bucket", "collapsed_bucket", "note_canonical_norm", "action_cue", "note_title_final", "drug_class"],
            dropna=False,
        )
        .agg(
            mention_rows=("adjudication_unit_id", "count"),
            example_context=("context_text", "first") if "context_text" in detailed.columns else ("adjudication_unit_id", "first"),
            matched_structured_drug=("matched_structured_drug", "first"),
            matched_structured_date=("matched_structured_date", "first"),
            day_difference=("day_difference", "first"),
        )
        .reset_index()
        .sort_values(["final_internal_bucket", "mention_rows"], ascending=[True, False])
    )
    examples["example_context"] = examples["example_context"].astype(str).str.slice(0, 300)

    no_overlap_sample = detailed[detailed["collapsed_bucket"] == "no_structured_overlap"].copy()
    if len(no_overlap_sample) > args.no_overlap_sample_size:
        no_overlap_sample = no_overlap_sample.sample(
            n=args.no_overlap_sample_size,
            random_state=args.sample_seed,
        )
    no_overlap_sample = no_overlap_sample.sort_values(["person_id", "visit_id", "note_id"]).reset_index(drop=True)

    outputs = {
        "detailed_csv": out_dir / "rq1_temporal_mismatch_ladder_detailed.csv",
        "internal_summary_csv": out_dir / "rq1_temporal_mismatch_ladder_internal_summary.csv",
        "collapsed_summary_csv": out_dir / "rq1_temporal_mismatch_ladder_collapsed_summary.csv",
        "examples_csv": out_dir / "rq1_temporal_mismatch_ladder_examples.csv",
        "by_action_csv": out_dir / "rq1_temporal_mismatch_ladder_by_action.csv",
        "by_note_type_csv": out_dir / "rq1_temporal_mismatch_ladder_by_note_type.csv",
        "by_drug_class_csv": out_dir / "rq1_temporal_mismatch_ladder_by_drug_class.csv",
        "note_mapping_csv": out_dir / "rq1_temporal_mismatch_note_label_mapping.csv",
        "structured_mapping_csv": out_dir / "rq1_temporal_mismatch_structured_drug_mapping.csv",
        "no_overlap_sample_csv": out_dir / "rq1_temporal_mismatch_no_structured_overlap_review_sample.csv",
        "summary_json": out_dir / "rq1_temporal_mismatch_ladder_summary.json",
    }

    detailed.drop(columns=["internal_bucket_order", "collapsed_bucket_order"], errors="ignore").to_csv(
        outputs["detailed_csv"], index=False
    )
    internal_summary.to_csv(outputs["internal_summary_csv"], index=False)
    collapsed_summary.to_csv(outputs["collapsed_summary_csv"], index=False)
    examples.to_csv(outputs["examples_csv"], index=False)
    by_action.to_csv(outputs["by_action_csv"], index=False)
    by_note_type.to_csv(outputs["by_note_type_csv"], index=False)
    by_drug_class.to_csv(outputs["by_drug_class_csv"], index=False)
    note_map.to_csv(outputs["note_mapping_csv"], index=False)
    structured_map.to_csv(outputs["structured_mapping_csv"], index=False)
    no_overlap_sample.to_csv(outputs["no_overlap_sample_csv"], index=False)

    write_run_summary(
        outputs["summary_json"],
        {
            "inputs": {
                "note_detailed_csv": str(note_path),
                "packets_mentions_csv": str(packets_path),
                "visit_timeline_csv": str(timeline_path),
                "episode_drugs_dir": str(episode_drugs_dir),
            },
            "counts": {
                "note_rows": int(len(detailed)),
                "structured_rows_filtered": int(len(structured)),
                "no_structured_overlap_rows": int((detailed["collapsed_bucket"] == "no_structured_overlap").sum()),
            },
            "outputs": {k: str(v) for k, v in outputs.items()},
        },
    )

    print(f"Saved temporal mismatch ladder outputs to: {out_dir}")
    print(f"Note rows analyzed: {len(detailed):,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
