#!/usr/bin/env python3
"""
Analyze note-derived medication evidence absent from structured EHR medication sets.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List

import pandas as pd

from rq1_adjudication_utils import parse_list_cell, write_run_summary
from rq1_bibm_utils import (
    action_cue,
    broad_drug_category,
    ingredient_level_label,
    load_brand_generic_map,
    safe_div,
)
from rq1_drug_linking import normalize_drug_text


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Analyze note-only medication evidence for the BIBM paper.")
    p.add_argument(
        "--adjudicated-mentions-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/adjudicated/rq1_adjudicated_mentions.csv",
    )
    p.add_argument(
        "--packets-mentions-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/adjudication_packets/adjudication_packets_mentions.csv",
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
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/note_only_evidence",
    )
    return p.parse_args()


def _normalize_series(df: pd.DataFrame, col: str) -> None:
    if col in df.columns:
        df[col] = df[col].fillna("").astype(str).str.strip()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]

    adjud_path = (root / args.adjudicated_mentions_csv).resolve()
    packets_path = (root / args.packets_mentions_csv).resolve()
    ehr_path = (root / args.ehr_csv).resolve()
    out_dir = (root / args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    alias_paths = [(root / x.strip()).resolve() for x in str(args.alias_artifacts).split(",") if x.strip()]
    brand_generic_map = load_brand_generic_map(alias_paths)

    adjud = pd.read_csv(adjud_path).fillna("")
    packets = pd.read_csv(packets_path).fillna("")
    ehr = pd.read_csv(ehr_path).fillna("")

    for df in [adjud, packets, ehr]:
        for col in ["adjudication_unit_id", "person_id", "visit_id", "note_id"]:
            _normalize_series(df, col)

    packets_small = packets[
        [
            c
            for c in [
                "adjudication_unit_id",
                "note_title",
                "candidate_category",
                "seed_treatment_action",
                "seed_discontinuation_reason",
                "seed_certainty",
            ]
            if c in packets.columns
        ]
    ].copy()
    merged = adjud.merge(packets_small, on="adjudication_unit_id", how="left")
    merged = merged[merged["adjudicated_canonical_label"].astype(str).str.strip() != ""].copy()

    merged["note_canonical_norm"] = merged["adjudicated_canonical_label"].map(normalize_drug_text)
    merged["note_ingredient_norm"] = merged["adjudicated_canonical_label"].map(
        lambda x: ingredient_level_label(x, brand_generic_map)
    )
    merged["action_cue"] = merged.apply(
        lambda r: action_cue(
            seed_treatment_action=str(r.get("seed_treatment_action", "")),
            seed_discontinuation_reason=str(r.get("seed_discontinuation_reason", "")),
            context_text=str(r.get("context_text", "")),
        ),
        axis=1,
    )
    merged["drug_class"] = merged["adjudicated_canonical_label"].map(broad_drug_category)
    merged["note_title_final"] = merged.get("note_title", "").astype(str).str.strip()
    merged["candidate_category"] = merged.get("candidate_category", "").astype(str).str.strip().replace({"": "unknown"})

    ehr["ehr_exact_set"] = ehr.get("drugs", "[]").apply(
        lambda x: sorted({normalize_drug_text(v) for v in parse_list_cell(x) if normalize_drug_text(v)})
    )
    ehr["ehr_ingredient_set"] = ehr["ehr_exact_set"].apply(
        lambda xs: sorted({ingredient_level_label(v, brand_generic_map) for v in xs if ingredient_level_label(v, brand_generic_map)})
    )
    ehr["ehr_category_set"] = ehr["ehr_ingredient_set"].apply(
        lambda xs: sorted({broad_drug_category(v) for v in xs if broad_drug_category(v) != "unknown"})
    )
    ehr_small = ehr[["person_id", "visit_id", "ehr_exact_set", "ehr_ingredient_set", "ehr_category_set"]].copy()

    merged = merged.merge(ehr_small, on=["person_id", "visit_id"], how="left")
    merged["ehr_exact_set"] = merged["ehr_exact_set"].apply(lambda x: x if isinstance(x, list) else [])
    merged["ehr_ingredient_set"] = merged["ehr_ingredient_set"].apply(lambda x: x if isinstance(x, list) else [])
    merged["ehr_category_set"] = merged["ehr_category_set"].apply(lambda x: x if isinstance(x, list) else [])
    merged["note_only_exact"] = merged.apply(lambda r: r["note_canonical_norm"] not in set(r["ehr_exact_set"]), axis=1)
    merged["note_only_ingredient"] = merged.apply(
        lambda r: r["note_ingredient_norm"] not in set(r["ehr_ingredient_set"]),
        axis=1,
    )
    merged["note_only_category"] = merged.apply(
        lambda r: (str(r["drug_class"]).strip() not in set(r["ehr_category_set"])) if str(r["drug_class"]).strip() else True,
        axis=1,
    )
    merged["evidence_relation"] = merged.apply(
        lambda r: "note_only_exact"
        if r["note_only_exact"]
        else ("note_only_ingredient" if r["note_only_ingredient"] else "structured_overlap"),
        axis=1,
    )

    visit_category_context = (
        merged.groupby(["person_id", "visit_id"], dropna=False)["drug_class"]
        .agg(lambda x: sorted({str(v).strip() for v in x if str(v).strip() and str(v).strip() != "unknown"}))
        .reset_index(name="note_visit_category_set")
    )
    merged = merged.merge(visit_category_context, on=["person_id", "visit_id"], how="left")
    merged["note_visit_category_set"] = merged["note_visit_category_set"].apply(lambda x: x if isinstance(x, list) else [])
    merged["visit_has_any_category_overlap"] = merged.apply(
        lambda r: bool(set(r["note_visit_category_set"]) & set(r["ehr_category_set"])),
        axis=1,
    )

    def _semantic_bucket(row: pd.Series) -> str:
        if not bool(row["note_only_exact"]):
            return "exact_label_overlap"
        if not bool(row["note_only_ingredient"]):
            return "exact_only_mismatch_but_ingredient_overlap"
        if not bool(row["note_only_category"]):
            return "ingredient_mismatch_but_category_overlap"
        if bool(row["visit_has_any_category_overlap"]):
            return "category_overlap_only"
        return "no_category_overlap"

    merged["semantic_mismatch_bucket"] = merged.apply(_semantic_bucket, axis=1)

    summary = pd.DataFrame(
        [
            {"item": "Mention rows with canonical labels", "value": int(len(merged)), "unit": "mention"},
            {
                "item": "Not represented in same-visit structured EHR medication set at exact-label level",
                "value": int(merged["note_only_exact"].sum()),
                "unit": "mention",
            },
            {
                "item": "Not represented in same-visit structured EHR medication set at ingredient level",
                "value": int(merged["note_only_ingredient"].sum()),
                "unit": "mention",
            },
            {
                "item": "Not represented in same-visit structured EHR medication set at broad category level",
                "value": int(merged["note_only_category"].sum()),
                "unit": "mention",
            },
            {
                "item": "No same-visit structured EHR category overlap",
                "value": int((merged["semantic_mismatch_bucket"] == "no_category_overlap").sum()),
                "unit": "mention",
            },
            {
                "item": "Exact-level not-represented rate",
                "value": round(safe_div(int(merged["note_only_exact"].sum()), len(merged)), 6),
                "unit": "fraction",
            },
            {
                "item": "Ingredient-level not-represented rate",
                "value": round(safe_div(int(merged["note_only_ingredient"].sum()), len(merged)), 6),
                "unit": "fraction",
            },
            {
                "item": "Category-level not-represented rate",
                "value": round(safe_div(int(merged["note_only_category"].sum()), len(merged)), 6),
                "unit": "fraction",
            },
            {
                "item": "Strongest no-category-overlap rate",
                "value": round(
                    safe_div(int((merged["semantic_mismatch_bucket"] == "no_category_overlap").sum()), len(merged)),
                    6,
                ),
                "unit": "fraction",
            },
        ]
    )

    by_action = (
        merged.groupby("action_cue", dropna=False)
        .agg(
            mention_rows=("adjudication_unit_id", "count"),
            note_only_exact_rows=("note_only_exact", "sum"),
            note_only_ingredient_rows=("note_only_ingredient", "sum"),
            unique_medications=("note_canonical_norm", "nunique"),
            unique_visits=("visit_id", "nunique"),
        )
        .reset_index()
        .sort_values("note_only_exact_rows", ascending=False)
    )
    for col in ["note_only_exact_rows", "note_only_ingredient_rows"]:
        by_action[f"{col}_rate"] = (
            by_action[col].astype(float) / by_action["mention_rows"].replace({0: 1}).astype(float)
        ).round(6)

    by_note_type = (
        merged.groupby("note_title_final", dropna=False)
        .agg(
            mention_rows=("adjudication_unit_id", "count"),
            note_only_exact_rows=("note_only_exact", "sum"),
            note_only_ingredient_rows=("note_only_ingredient", "sum"),
            unique_medications=("note_canonical_norm", "nunique"),
        )
        .reset_index()
        .sort_values("note_only_exact_rows", ascending=False)
    )
    by_note_type["note_only_exact_rate"] = (
        by_note_type["note_only_exact_rows"].astype(float)
        / by_note_type["mention_rows"].replace({0: 1}).astype(float)
    ).round(6)

    by_drug_class = (
        merged.groupby("drug_class", dropna=False)
        .agg(
            mention_rows=("adjudication_unit_id", "count"),
            note_only_exact_rows=("note_only_exact", "sum"),
            note_only_ingredient_rows=("note_only_ingredient", "sum"),
            unique_medications=("note_canonical_norm", "nunique"),
        )
        .reset_index()
        .sort_values("note_only_exact_rows", ascending=False)
    )
    by_drug_class["note_only_exact_rate"] = (
        by_drug_class["note_only_exact_rows"].astype(float)
        / by_drug_class["mention_rows"].replace({0: 1}).astype(float)
    ).round(6)

    top_examples = (
        merged[merged["note_only_exact"]]
        .groupby(["note_canonical_norm", "action_cue", "note_title_final", "drug_class"], dropna=False)
        .agg(
            mention_rows=("adjudication_unit_id", "count"),
            example_context=("context_text", "first"),
        )
        .reset_index()
        .sort_values("mention_rows", ascending=False)
    )
    top_examples["example_context"] = top_examples["example_context"].astype(str).str.slice(0, 300)

    relation_breakdown = (
        merged.groupby("evidence_relation", dropna=False)
        .size()
        .reset_index(name="mention_rows")
        .sort_values("mention_rows", ascending=False)
    )

    mismatch_ladder = (
        merged.groupby("semantic_mismatch_bucket", dropna=False)
        .agg(
            mention_rows=("adjudication_unit_id", "count"),
            unique_medications=("note_canonical_norm", "nunique"),
            unique_visits=("visit_id", "nunique"),
            note_only_exact_rows=("note_only_exact", "sum"),
            note_only_ingredient_rows=("note_only_ingredient", "sum"),
            note_only_category_rows=("note_only_category", "sum"),
        )
        .reset_index()
    )
    mismatch_order = {
        "exact_label_overlap": 0,
        "exact_only_mismatch_but_ingredient_overlap": 1,
        "ingredient_mismatch_but_category_overlap": 2,
        "category_overlap_only": 3,
        "no_category_overlap": 4,
    }
    mismatch_ladder["bucket_order"] = mismatch_ladder["semantic_mismatch_bucket"].map(mismatch_order).fillna(99)
    mismatch_ladder = mismatch_ladder.sort_values(["bucket_order", "mention_rows"], ascending=[True, False]).drop(
        columns=["bucket_order"]
    )
    mismatch_ladder["mention_rate"] = mismatch_ladder["mention_rows"].map(lambda x: round(safe_div(x, len(merged)), 6))

    mismatch_examples = (
        merged.groupby(
            ["semantic_mismatch_bucket", "note_canonical_norm", "action_cue", "note_title_final", "drug_class"],
            dropna=False,
        )
        .agg(
            mention_rows=("adjudication_unit_id", "count"),
            example_context=("context_text", "first"),
        )
        .reset_index()
        .sort_values(
            ["semantic_mismatch_bucket", "mention_rows"],
            ascending=[True, False],
        )
    )
    mismatch_examples["example_context"] = mismatch_examples["example_context"].astype(str).str.slice(0, 300)

    outputs: Dict[str, Path] = {
        "summary_csv": out_dir / "rq1_note_only_evidence_summary.csv",
        "by_action_csv": out_dir / "rq1_note_only_by_action.csv",
        "by_note_type_csv": out_dir / "rq1_note_only_by_note_type.csv",
        "by_drug_class_csv": out_dir / "rq1_note_only_by_drug_class.csv",
        "top_examples_csv": out_dir / "rq1_note_only_top_examples.csv",
        "relation_breakdown_csv": out_dir / "rq1_note_only_relation_breakdown.csv",
        "mismatch_ladder_csv": out_dir / "rq1_note_only_semantic_mismatch_ladder.csv",
        "mismatch_ladder_summary_csv": out_dir / "rq1_note_only_semantic_mismatch_ladder_summary.csv",
        "mismatch_examples_csv": out_dir / "rq1_note_only_semantic_mismatch_ladder_examples.csv",
        "detailed_csv": out_dir / "rq1_note_only_evidence_detailed.csv",
        "summary_json": out_dir / "rq1_note_only_evidence_summary.json",
    }

    summary.to_csv(outputs["summary_csv"], index=False)
    by_action.to_csv(outputs["by_action_csv"], index=False)
    by_note_type.to_csv(outputs["by_note_type_csv"], index=False)
    by_drug_class.to_csv(outputs["by_drug_class_csv"], index=False)
    top_examples.to_csv(outputs["top_examples_csv"], index=False)
    relation_breakdown.to_csv(outputs["relation_breakdown_csv"], index=False)
    mismatch_examples.to_csv(outputs["mismatch_examples_csv"], index=False)
    mismatch_ladder.to_csv(outputs["mismatch_ladder_csv"], index=False)
    mismatch_ladder.to_csv(outputs["mismatch_ladder_summary_csv"], index=False)
    merged.to_csv(outputs["detailed_csv"], index=False)

    write_run_summary(
        outputs["summary_json"],
        {
            "inputs": {
                "adjudicated_mentions_csv": str(adjud_path),
                "packets_mentions_csv": str(packets_path),
                "ehr_csv": str(ehr_path),
            },
            "counts": {
                "canonical_mention_rows": int(len(merged)),
                "note_only_exact_rows": int(merged["note_only_exact"].sum()),
                "note_only_ingredient_rows": int(merged["note_only_ingredient"].sum()),
                "note_only_category_rows": int(merged["note_only_category"].sum()),
                "no_category_overlap_rows": int((merged["semantic_mismatch_bucket"] == "no_category_overlap").sum()),
            },
            "outputs": {k: str(v) for k, v in outputs.items()},
        },
    )

    print(f"Saved note-only evidence outputs to: {out_dir}")
    print(f"Note-only exact mentions: {int(merged['note_only_exact'].sum()):,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
