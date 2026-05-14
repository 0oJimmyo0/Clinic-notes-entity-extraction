#!/usr/bin/env python3
"""
Secondary semantic note-to-EHR concordance analysis.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List

import pandas as pd

from rq1_adjudication_utils import parse_list_cell, write_run_summary
from rq1_bibm_utils import (
    broad_drug_category,
    ingredient_level_label,
    load_brand_generic_map,
    safe_div,
    set_metrics,
)
from rq1_drug_linking import normalize_drug_text


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run secondary semantic note-to-EHR concordance for BIBM.")
    p.add_argument(
        "--adjudicated-mentions-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/adjudicated/rq1_adjudicated_mentions.csv",
    )
    p.add_argument(
        "--ehr-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/rq1_ehr_entities_by_visit.csv",
    )
    p.add_argument(
        "--alias-artifacts",
        default="../resources/manual/pathA_alias_map.json,../resources/lexicons/rq1_drug_aliases.json",
    )
    p.add_argument(
        "--output-dir",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/semantic_concordance",
    )
    return p.parse_args()


def _normalize(df: pd.DataFrame, col: str) -> None:
    if col in df.columns:
        df[col] = df[col].fillna("").astype(str).str.strip()


def _summarize_level(pairs: pd.DataFrame, level_name: str) -> Dict[str, object]:
    return {
        "concordance_level": level_name,
        "visit_pairs": int(len(pairs)),
        "mean_note_med_count": round(pairs["note_count"].mean() if len(pairs) else 0.0, 6),
        "mean_ehr_med_count": round(pairs["ehr_count"].mean() if len(pairs) else 0.0, 6),
        "exact_set_match_rate": round(pairs["exact_set_match"].mean() if len(pairs) else 0.0, 6),
        "overlap_rate": round(pairs["overlap_any"].mean() if len(pairs) else 0.0, 6),
        "mean_jaccard": round(pairs["jaccard"].mean() if len(pairs) else 0.0, 6),
        "mean_note_in_ehr_containment": round(
            pairs["left_in_right_containment"].mean() if len(pairs) else 0.0, 6
        ),
        "mean_ehr_in_note_containment": round(
            pairs["right_in_left_containment"].mean() if len(pairs) else 0.0, 6
        ),
    }


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]

    adjud_path = (root / args.adjudicated_mentions_csv).resolve()
    ehr_path = (root / args.ehr_csv).resolve()
    out_dir = (root / args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    alias_paths = [(root / x.strip()).resolve() for x in str(args.alias_artifacts).split(",") if x.strip()]
    brand_generic_map = load_brand_generic_map(alias_paths)

    adjud = pd.read_csv(adjud_path).fillna("")
    ehr = pd.read_csv(ehr_path).fillna("")

    for df in [adjud, ehr]:
        for col in ["person_id", "visit_id"]:
            _normalize(df, col)

    adjud = adjud[adjud["adjudicated_canonical_label"].astype(str).str.strip() != ""].copy()
    adjud["exact_label"] = adjud["adjudicated_canonical_label"].map(normalize_drug_text)
    adjud["ingredient_label"] = adjud["adjudicated_canonical_label"].map(
        lambda x: ingredient_level_label(x, brand_generic_map)
    )
    adjud["category_label"] = adjud["adjudicated_canonical_label"].map(broad_drug_category)

    note_exact = (
        adjud.groupby(["person_id", "visit_id"])["exact_label"]
        .agg(lambda x: sorted({v for v in x if v}))
        .reset_index(name="note_exact_set")
    )
    note_ing = (
        adjud.groupby(["person_id", "visit_id"])["ingredient_label"]
        .agg(lambda x: sorted({v for v in x if v}))
        .reset_index(name="note_ingredient_set")
    )
    note_cat = (
        adjud.groupby(["person_id", "visit_id"])["category_label"]
        .agg(lambda x: sorted({v for v in x if v and v != "unknown"}))
        .reset_index(name="note_category_set")
    )
    note_sets = note_exact.merge(note_ing, on=["person_id", "visit_id"], how="outer").merge(
        note_cat, on=["person_id", "visit_id"], how="outer"
    )

    ehr["ehr_exact_set"] = ehr.get("drugs", "[]").apply(
        lambda x: sorted({normalize_drug_text(v) for v in parse_list_cell(x) if normalize_drug_text(v)})
    )
    ehr["ehr_ingredient_set"] = ehr["ehr_exact_set"].apply(
        lambda xs: sorted({ingredient_level_label(v, brand_generic_map) for v in xs if ingredient_level_label(v, brand_generic_map)})
    )
    ehr["ehr_category_set"] = ehr["ehr_ingredient_set"].apply(
        lambda xs: sorted({broad_drug_category(v) for v in xs if broad_drug_category(v) != "unknown"})
    )
    ehr_sets = ehr[["person_id", "visit_id", "ehr_exact_set", "ehr_ingredient_set", "ehr_category_set"]].copy()

    pairs = note_sets.merge(ehr_sets, on=["person_id", "visit_id"], how="left")
    for col in [
        "note_exact_set",
        "note_ingredient_set",
        "note_category_set",
        "ehr_exact_set",
        "ehr_ingredient_set",
        "ehr_category_set",
    ]:
        pairs[col] = pairs[col].apply(lambda x: x if isinstance(x, list) else [])

    level_specs = [
        ("exact_canonical", "note_exact_set", "ehr_exact_set"),
        ("ingredient_level", "note_ingredient_set", "ehr_ingredient_set"),
        ("clinical_category_level", "note_category_set", "ehr_category_set"),
    ]

    level_summaries: List[Dict[str, object]] = []
    pair_frames: List[pd.DataFrame] = []
    for level_name, note_col, ehr_col in level_specs:
        rows: List[Dict[str, object]] = []
        for row in pairs.itertuples(index=False):
            metrics = set_metrics(getattr(row, note_col), getattr(row, ehr_col))
            rows.append(
                {
                    "person_id": row.person_id,
                    "visit_id": row.visit_id,
                    "concordance_level": level_name,
                    "note_set": getattr(row, note_col),
                    "ehr_set": getattr(row, ehr_col),
                    "note_count": metrics["left_n"],
                    "ehr_count": metrics["right_n"],
                    "intersection_n": metrics["intersection_n"],
                    "union_n": metrics["union_n"],
                    "exact_set_match": metrics["exact_set_match"],
                    "overlap_any": metrics["overlap_any"],
                    "jaccard": round(float(metrics["jaccard"]), 6),
                    "left_in_right_containment": round(float(metrics["left_in_right_containment"]), 6),
                    "right_in_left_containment": round(float(metrics["right_in_left_containment"]), 6),
                }
            )
        pair_df = pd.DataFrame(rows)
        pair_frames.append(pair_df)
        level_summaries.append(_summarize_level(pair_df, level_name))

    summary_df = pd.DataFrame(level_summaries)
    pairs_df = pd.concat(pair_frames, ignore_index=True)

    outputs: Dict[str, Path] = {
        "summary_csv": out_dir / "rq1_semantic_concordance_summary.csv",
        "pairs_csv": out_dir / "rq1_semantic_concordance_pairs.csv",
        "summary_json": out_dir / "rq1_semantic_concordance_summary.json",
    }
    summary_df.to_csv(outputs["summary_csv"], index=False)
    pairs_df.to_csv(outputs["pairs_csv"], index=False)

    write_run_summary(
        outputs["summary_json"],
        {
            "inputs": {
                "adjudicated_mentions_csv": str(adjud_path),
                "ehr_csv": str(ehr_path),
            },
            "counts": {
                "visit_pairs": int(len(pairs)),
                "note_visits_with_medications": int(len(note_sets)),
                "ehr_visits_available": int(len(ehr_sets)),
            },
            "outputs": {k: str(v) for k, v in outputs.items()},
        },
    )

    print(f"Saved semantic concordance outputs to: {out_dir}")
    print(f"Visit pairs: {len(pairs):,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
