#!/usr/bin/env python3
"""
Build explicit OMOP/RxNorm mapping artifacts for the clinic-note medication study.

Outputs:
- structured drug_concept_id -> standard concept / ingredient / therapeutic category
- note canonical label -> RxNorm/OMOP candidate / ingredient / therapeutic category
- coverage summaries and unmapped lists
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from rq1_adjudication_utils import write_run_summary
from rq1_drug_linking import normalize_drug_text


ATC_PRIORITY = ["ATC 2nd", "ATC 3rd", "ATC 1st", "ATC 4th", "ATC 5th"]
CONCEPT_CLASS_PRIORITY = {
    "Ingredient": 0,
    "Precise Ingredient": 1,
    "Multiple Ingredients": 2,
    "Brand Name": 3,
    "Clinical Drug Form": 4,
    "Clinical Drug": 5,
    "Clinical Drug Comp": 6,
    "Branded Drug Form": 7,
    "Branded Drug": 8,
    "Branded Drug Comp": 9,
    "Clinical Dose Group": 10,
    "Branded Dose Group": 11,
}
VOCAB_PRIORITY = {
    "RxNorm": 0,
    "RxNorm Extension": 1,
    "ATC": 2,
    "VA Class": 3,
    "NDC": 4,
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build OMOP/RxNorm mapping artifacts for RQ1.")
    p.add_argument(
        "--note-detailed-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/note_only_evidence_bibm_test/rq1_note_only_evidence_detailed.csv",
    )
    p.add_argument(
        "--episode-drugs-dir",
        default="../resources/struct_data/episode_drugs",
    )
    p.add_argument(
        "--omop-dir",
        default="../resources/raw/OMOP_related",
    )
    p.add_argument(
        "--output-dir",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/omop_rxnorm_mapping",
    )
    return p.parse_args()


def _priority_rank(series: pd.Series, lookup: Dict[str, int], default: int = 9999) -> pd.Series:
    return series.astype(str).map(lambda x: lookup.get(x, default))


def _load_vocab_tables(omop_dir: Path) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    concept = pd.read_csv(
        omop_dir / "CONCEPT.csv",
        sep="\t",
        usecols=[
            "concept_id",
            "concept_name",
            "domain_id",
            "vocabulary_id",
            "concept_class_id",
            "standard_concept",
            "invalid_reason",
        ],
        low_memory=False,
    )
    rel = pd.read_csv(
        omop_dir / "CONCEPT_RELATIONSHIP.csv",
        sep="\t",
        usecols=["concept_id_1", "concept_id_2", "relationship_id", "invalid_reason"],
        low_memory=False,
    )
    ancestor = pd.read_csv(
        omop_dir / "CONCEPT_ANCESTOR.csv",
        sep="\t",
        usecols=["ancestor_concept_id", "descendant_concept_id", "min_levels_of_separation"],
        low_memory=False,
    )
    strength = pd.read_csv(
        omop_dir / "DRUG_STRENGTH.csv",
        sep="\t",
        usecols=["drug_concept_id", "ingredient_concept_id", "invalid_reason"],
        low_memory=False,
    )
    synonym = pd.read_csv(
        omop_dir / "CONCEPT_SYNONYM.csv",
        sep="\t",
        usecols=["concept_id", "concept_synonym_name"],
        low_memory=False,
    )
    return concept, rel, ancestor, strength, synonym


def _build_standard_bridge(used_concept_ids: pd.Series, concept: pd.DataFrame, rel: pd.DataFrame) -> pd.DataFrame:
    used = pd.DataFrame({"drug_concept_id": used_concept_ids.dropna().astype("int64").unique()})
    direct = concept[
        concept["concept_id"].isin(used["drug_concept_id"]) & concept["standard_concept"].isin(["S", "C"])
    ][["concept_id"]].copy()
    direct = direct.rename(columns={"concept_id": "drug_concept_id"})
    direct["standard_concept_id"] = direct["drug_concept_id"]
    direct["standard_mapping_method"] = "direct_standard"

    maps_to = rel[(rel["relationship_id"] == "Maps to") & (rel["invalid_reason"].isna())][
        ["concept_id_1", "concept_id_2"]
    ].drop_duplicates()
    mapped = used.merge(maps_to, left_on="drug_concept_id", right_on="concept_id_1", how="left")
    mapped = mapped[mapped["concept_id_2"].notna()][["drug_concept_id", "concept_id_2"]].copy()
    mapped["concept_id_2"] = mapped["concept_id_2"].astype("int64")
    mapped = mapped.rename(columns={"concept_id_2": "standard_concept_id"})
    mapped["standard_mapping_method"] = "maps_to"

    bridge = pd.concat([direct, mapped], ignore_index=True)
    bridge = bridge.sort_values(
        by=["drug_concept_id", "standard_mapping_method"],
        key=lambda s: s.map({"direct_standard": 0, "maps_to": 1}),
    ).drop_duplicates("drug_concept_id")
    return used.merge(bridge, on="drug_concept_id", how="left")


def _build_ingredient_map(concept: pd.DataFrame, strength: pd.DataFrame, target_standard_ids: set[int]) -> pd.DataFrame:
    concept_small = concept[["concept_id", "concept_name", "concept_class_id", "vocabulary_id"]].copy()
    ingredient_names = concept_small.rename(
        columns={
            "concept_id": "ingredient_concept_id",
            "concept_name": "ingredient_name",
            "concept_class_id": "ingredient_concept_class_id",
            "vocabulary_id": "ingredient_vocabulary_id",
        }
    )

    strength = strength[strength["invalid_reason"].isna()][["drug_concept_id", "ingredient_concept_id"]].drop_duplicates()
    if target_standard_ids:
        strength = strength[strength["drug_concept_id"].isin(target_standard_ids)].copy()
    merged = strength.merge(ingredient_names, on="ingredient_concept_id", how="left")
    merged["ingredient_name_norm"] = merged["ingredient_name"].map(normalize_drug_text)

    agg = (
        merged.groupby("drug_concept_id", dropna=False)
        .agg(
            ingredient_concept_ids=("ingredient_concept_id", lambda xs: "|".join(str(int(x)) for x in sorted(set(x for x in xs if pd.notna(x))))),
            ingredient_names=("ingredient_name", lambda xs: "|".join(sorted(set(str(x) for x in xs if str(x) != "nan")))),
            ingredient_name_norms=("ingredient_name_norm", lambda xs: "|".join(sorted(set(x for x in xs if x)))),
            ingredient_count=("ingredient_concept_id", lambda xs: len(set(x for x in xs if pd.notna(x)))),
        )
        .reset_index()
    )
    return agg


def _build_category_map(concept: pd.DataFrame, ancestor: pd.DataFrame, target_standard_ids: set[int]) -> pd.DataFrame:
    drug_concepts = concept[concept["domain_id"] == "Drug"][
        ["concept_id", "concept_name", "vocabulary_id", "concept_class_id", "standard_concept"]
    ].copy()
    atc_va = drug_concepts[drug_concepts["vocabulary_id"].isin(["ATC", "VA Class"])].copy()
    atc_va = atc_va.rename(
        columns={
            "concept_id": "category_concept_id",
            "concept_name": "category_name",
            "vocabulary_id": "category_vocabulary_id",
            "concept_class_id": "category_concept_class_id",
            "standard_concept": "category_standard_concept",
        }
    )

    anc = ancestor[ancestor["descendant_concept_id"].isin(target_standard_ids)].copy()
    anc = anc.merge(atc_va, left_on="ancestor_concept_id", right_on="category_concept_id", how="inner")
    anc["category_priority"] = _priority_rank(anc["category_concept_class_id"], {k: i for i, k in enumerate(ATC_PRIORITY)})
    anc["vocab_priority"] = _priority_rank(anc["category_vocabulary_id"], {"ATC": 0, "VA Class": 1})
    anc = anc.sort_values(
        ["descendant_concept_id", "vocab_priority", "category_priority", "min_levels_of_separation", "category_name"]
    )
    best = anc.drop_duplicates("descendant_concept_id").copy()
    best["category_label_norm"] = best["category_name"].map(normalize_drug_text)
    return best[
        [
            "descendant_concept_id",
            "category_concept_id",
            "category_name",
            "category_label_norm",
            "category_vocabulary_id",
            "category_concept_class_id",
        ]
    ].rename(columns={"descendant_concept_id": "standard_concept_id"})


def _pick_note_candidates(candidates: pd.DataFrame) -> pd.DataFrame:
    if candidates.empty:
        return candidates
    candidates = candidates.copy()
    candidates["match_priority"] = candidates["match_method"].map({"concept_name_exact": 0, "concept_synonym_exact": 1}).fillna(9)
    candidates["class_priority"] = _priority_rank(candidates["concept_class_id"], CONCEPT_CLASS_PRIORITY)
    candidates["vocab_priority"] = _priority_rank(candidates["vocabulary_id"], VOCAB_PRIORITY)
    candidates = candidates.sort_values(
        ["note_label", "match_priority", "class_priority", "vocab_priority", "standard_concept_id"],
    )
    return candidates.drop_duplicates("note_label").drop(columns=["match_priority"], errors="ignore")


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]

    note_path = (root / args.note_detailed_csv).resolve()
    episode_drugs_dir = (root / args.episode_drugs_dir).resolve()
    omop_dir = (root / args.omop_dir).resolve()
    out_dir = (root / args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    notes = pd.read_csv(note_path, usecols=["adjudicated_canonical_label"]).fillna("")
    note_labels = pd.DataFrame({"note_label": sorted(set(notes["adjudicated_canonical_label"].astype(str).str.strip()))})
    note_labels = note_labels[note_labels["note_label"] != ""].copy()
    note_labels["note_label_norm"] = note_labels["note_label"].map(normalize_drug_text)
    note_norm_set = set(note_labels["note_label_norm"].dropna().astype(str))

    concept, rel, ancestor, strength, synonym = _load_vocab_tables(omop_dir)

    # Structured concept ids used in the cohort
    used_parts: List[pd.DataFrame] = []
    for f in sorted(episode_drugs_dir.glob("*.parquet")):
        used_parts.append(pd.read_parquet(f, columns=["drug_concept_id", "drug_source_value"]))
    used = pd.concat(used_parts, ignore_index=True)
    used["drug_concept_id"] = pd.to_numeric(used["drug_concept_id"], errors="coerce")
    used = used[used["drug_concept_id"].notna()].copy()
    used["drug_concept_id"] = used["drug_concept_id"].astype("int64")
    used_counts = used.groupby("drug_concept_id", as_index=False).agg(
        structured_row_count=("drug_source_value", "size"),
        example_drug_source_value=("drug_source_value", "first"),
    )

    bridge = _build_standard_bridge(used["drug_concept_id"], concept, rel)
    concept_meta = concept.rename(
        columns={
            "concept_id": "standard_concept_id",
            "concept_name": "standard_concept_name",
            "vocabulary_id": "standard_vocabulary_id",
            "concept_class_id": "standard_concept_class_id",
            "standard_concept": "standard_concept_flag",
            "invalid_reason": "standard_invalid_reason",
        }
    )
    structured_map = used_counts.merge(bridge, on="drug_concept_id", how="left")
    structured_map = structured_map.merge(concept_meta, on="standard_concept_id", how="left")
    structured_map["standard_concept_name_norm"] = structured_map["standard_concept_name"].map(
        lambda x: normalize_drug_text(x) if pd.notna(x) else ""
    )
    structured_standard_ids = set(
        structured_map["standard_concept_id"].dropna().astype("int64").tolist()
    )

    ingredient_map = _build_ingredient_map(concept, strength, structured_standard_ids)
    structured_map = structured_map.merge(ingredient_map, left_on="standard_concept_id", right_on="drug_concept_id", how="left", suffixes=("", "_ingredient"))
    structured_map = structured_map.drop(columns=["drug_concept_id_ingredient"], errors="ignore")

    category_map = _build_category_map(concept, ancestor, structured_standard_ids)
    structured_map = structured_map.merge(category_map, on="standard_concept_id", how="left")

    structured_map["structured_mapping_status"] = "mapped"
    structured_map.loc[structured_map["standard_concept_id"].isna(), "structured_mapping_status"] = "unmapped_standard_concept"
    structured_map.loc[
        structured_map["standard_concept_id"].notna() & structured_map["ingredient_name_norms"].fillna("").eq(""),
        "structured_mapping_status",
    ] = "mapped_no_ingredient"
    structured_map.loc[
        structured_map["standard_concept_id"].notna() & structured_map["category_label_norm"].fillna("").eq(""),
        "structured_mapping_status",
    ] = structured_map["structured_mapping_status"].where(
        structured_map["structured_mapping_status"] != "mapped", "mapped_no_category"
    )

    # Note label mapping via standard concepts and synonyms
    standard_drugs = concept[
        (concept["domain_id"] == "Drug") & concept["invalid_reason"].isna() & concept["standard_concept"].isin(["S", "C"])
    ][["concept_id", "concept_name", "vocabulary_id", "concept_class_id"]].copy()
    standard_drugs["note_label_norm"] = standard_drugs["concept_name"].map(normalize_drug_text)
    standard_drugs = standard_drugs[standard_drugs["note_label_norm"].isin(note_norm_set)].copy()

    name_hits = note_labels.merge(
        standard_drugs.rename(columns={"concept_id": "matched_concept_id", "concept_name": "matched_concept_name"}),
        on="note_label_norm",
        how="left",
    )
    name_hits["match_method"] = "concept_name_exact"

    synonym["note_label_norm"] = synonym["concept_synonym_name"].map(normalize_drug_text)
    synonym = synonym[synonym["note_label_norm"].isin(note_norm_set)].copy()
    synonym_hits = note_labels.merge(
        synonym.rename(columns={"concept_id": "matched_concept_id", "concept_synonym_name": "matched_synonym_name"})[
            ["matched_concept_id", "matched_synonym_name", "note_label_norm"]
        ],
        on="note_label_norm",
        how="left",
    )
    synonym_hits = synonym_hits[synonym_hits["matched_concept_id"].notna()].copy()
    synonym_hits = synonym_hits.merge(
        standard_drugs.rename(columns={"concept_id": "matched_concept_id", "concept_name": "matched_concept_name"}),
        on="matched_concept_id",
        how="left",
    )
    synonym_hits["match_method"] = "concept_synonym_exact"

    note_candidates = pd.concat([name_hits, synonym_hits], ignore_index=True)
    note_candidates = note_candidates[note_candidates["matched_concept_id"].notna()].copy()
    note_candidates = note_candidates.rename(
        columns={
            "matched_concept_id": "standard_concept_id",
            "matched_concept_name": "standard_concept_name",
        }
    )
    note_candidates["standard_concept_id"] = note_candidates["standard_concept_id"].astype("int64")
    note_candidates = _pick_note_candidates(note_candidates.rename(columns={"matched_synonym_name": "matched_synonym"}))

    note_map = note_labels.merge(
        note_candidates[
            [
                "note_label",
                "note_label_norm",
                "standard_concept_id",
                "standard_concept_name",
                "vocabulary_id",
                "concept_class_id",
                "match_method",
                "matched_synonym",
            ]
        ],
        on=["note_label", "note_label_norm"],
        how="left",
    )
    note_map = note_map.rename(
        columns={
            "vocabulary_id": "standard_vocabulary_id",
            "concept_class_id": "standard_concept_class_id",
        }
    )
    note_map["standard_concept_name_norm"] = note_map["standard_concept_name"].map(
        lambda x: normalize_drug_text(x) if pd.notna(x) else ""
    )
    note_standard_ids = set(note_map["standard_concept_id"].dropna().astype("int64").tolist())
    all_target_standard_ids = structured_standard_ids | note_standard_ids
    ingredient_map = _build_ingredient_map(concept, strength, all_target_standard_ids)
    category_map = _build_category_map(concept, ancestor, all_target_standard_ids)

    structured_map = structured_map.drop(
        columns=[
            "ingredient_concept_ids",
            "ingredient_names",
            "ingredient_name_norms",
            "ingredient_count",
            "category_concept_id",
            "category_name",
            "category_label_norm",
            "category_vocabulary_id",
            "category_concept_class_id",
        ],
        errors="ignore",
    )
    structured_map = structured_map.merge(ingredient_map, left_on="standard_concept_id", right_on="drug_concept_id", how="left", suffixes=("", "_ingredient"))
    structured_map = structured_map.drop(columns=["drug_concept_id_ingredient"], errors="ignore")
    structured_map = structured_map.merge(category_map, on="standard_concept_id", how="left")

    note_map = note_map.merge(ingredient_map, left_on="standard_concept_id", right_on="drug_concept_id", how="left", suffixes=("", "_ingredient"))
    note_map = note_map.drop(columns=["drug_concept_id"], errors="ignore")
    note_map = note_map.merge(category_map, on="standard_concept_id", how="left")
    note_map["note_mapping_status"] = "mapped"
    note_map.loc[note_map["standard_concept_id"].isna(), "note_mapping_status"] = "unmapped_note_label"
    note_map.loc[
        note_map["standard_concept_id"].notna() & note_map["ingredient_name_norms"].fillna("").eq(""),
        "note_mapping_status",
    ] = "mapped_no_ingredient"
    note_map.loc[
        note_map["standard_concept_id"].notna() & note_map["category_label_norm"].fillna("").eq(""),
        "note_mapping_status",
    ] = note_map["note_mapping_status"].where(note_map["note_mapping_status"] != "mapped", "mapped_no_category")

    # Coverage summary
    structured_total_rows = int(used_counts["structured_row_count"].sum())
    structured_cov = pd.DataFrame(
        [
            {
                "entity": "structured_concepts",
                "metric": "unique_drug_concept_ids",
                "count": int(len(structured_map)),
                "percent": "",
            },
            {
                "entity": "structured_concepts",
                "metric": "mapped_standard_concept_ids",
                "count": int(structured_map["standard_concept_id"].notna().sum()),
                "percent": round(structured_map["standard_concept_id"].notna().mean(), 6),
            },
            {
                "entity": "structured_rows",
                "metric": "mapped_standard_concept_rows",
                "count": int(structured_map.loc[structured_map["standard_concept_id"].notna(), "structured_row_count"].sum()),
                "percent": round(
                    structured_map.loc[structured_map["standard_concept_id"].notna(), "structured_row_count"].sum() / structured_total_rows,
                    6,
                ),
            },
            {
                "entity": "structured_rows",
                "metric": "ingredient_covered_rows",
                "count": int(structured_map.loc[structured_map["ingredient_name_norms"].fillna("") != "", "structured_row_count"].sum()),
                "percent": round(
                    structured_map.loc[structured_map["ingredient_name_norms"].fillna("") != "", "structured_row_count"].sum() / structured_total_rows,
                    6,
                ),
            },
            {
                "entity": "structured_rows",
                "metric": "category_covered_rows",
                "count": int(structured_map.loc[structured_map["category_label_norm"].fillna("") != "", "structured_row_count"].sum()),
                "percent": round(
                    structured_map.loc[structured_map["category_label_norm"].fillna("") != "", "structured_row_count"].sum() / structured_total_rows,
                    6,
                ),
            },
            {
                "entity": "note_labels",
                "metric": "unique_note_labels",
                "count": int(len(note_map)),
                "percent": "",
            },
            {
                "entity": "note_labels",
                "metric": "mapped_note_labels",
                "count": int(note_map["standard_concept_id"].notna().sum()),
                "percent": round(note_map["standard_concept_id"].notna().mean(), 6),
            },
            {
                "entity": "note_labels",
                "metric": "ingredient_covered_note_labels",
                "count": int((note_map["ingredient_name_norms"].fillna("") != "").sum()),
                "percent": round((note_map["ingredient_name_norms"].fillna("") != "").mean(), 6),
            },
            {
                "entity": "note_labels",
                "metric": "category_covered_note_labels",
                "count": int((note_map["category_label_norm"].fillna("") != "").sum()),
                "percent": round((note_map["category_label_norm"].fillna("") != "").mean(), 6),
            },
        ]
    )

    note_unmapped = note_map[note_map["standard_concept_id"].isna()].copy()
    structured_unmapped = structured_map[structured_map["standard_concept_id"].isna()].copy()

    outputs = {
        "structured_mapping_csv": out_dir / "rq1_structured_concept_mapping.csv",
        "note_mapping_csv": out_dir / "rq1_note_label_omop_rxnorm_mapping.csv",
        "mapping_coverage_csv": out_dir / "rq1_omop_rxnorm_mapping_coverage.csv",
        "unmapped_note_csv": out_dir / "rq1_unmapped_note_labels.csv",
        "unmapped_structured_csv": out_dir / "rq1_unmapped_structured_drug_concepts.csv",
        "summary_json": out_dir / "rq1_omop_rxnorm_mapping_summary.json",
    }

    structured_map.to_csv(outputs["structured_mapping_csv"], index=False)
    note_map.to_csv(outputs["note_mapping_csv"], index=False)
    structured_cov.to_csv(outputs["mapping_coverage_csv"], index=False)
    note_unmapped.to_csv(outputs["unmapped_note_csv"], index=False)
    structured_unmapped.to_csv(outputs["unmapped_structured_csv"], index=False)

    write_run_summary(
        outputs["summary_json"],
        {
            "inputs": {
                "note_detailed_csv": str(note_path),
                "episode_drugs_dir": str(episode_drugs_dir),
                "omop_dir": str(omop_dir),
            },
            "counts": {
                "unique_note_labels": int(len(note_map)),
                "mapped_note_labels": int(note_map["standard_concept_id"].notna().sum()),
                "unique_structured_drug_concept_ids": int(len(structured_map)),
                "mapped_structured_drug_concept_ids": int(structured_map["standard_concept_id"].notna().sum()),
            },
            "outputs": {k: str(v) for k, v in outputs.items()},
        },
    )

    print(f"Saved mapping artifacts to: {out_dir}")
    print(f"Mapped note labels: {int(note_map['standard_concept_id'].notna().sum()):,} / {len(note_map):,}")
    print(f"Mapped structured concept ids: {int(structured_map['standard_concept_id'].notna().sum()):,} / {len(structured_map):,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
