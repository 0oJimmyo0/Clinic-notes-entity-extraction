#!/usr/bin/env python3
"""
Evaluate simple non-deep-learning comparators against the note-grounded
adjudicated canonical labels on the frozen clinic-like cohort.

Purpose:
- Keep the denominator identical to the paper-facing Path A evaluation.
- Compare Path A against lightweight lexical linkers rather than LLM or
  deep-learning baselines.
- Produce paper-safe outputs without reviving legacy Path B framing.
"""

from __future__ import annotations

import argparse
import json
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

import pandas as pd

from rq1_drug_linking import (
    CharNgramLinker,
    build_canonical_drug_universe,
    canonicalize_drug,
    load_alias_exclusions,
    load_alias_map,
    normalize_drug_text,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run classical lexical comparators on adjudicated medication mentions.")
    p.add_argument(
        "--adjudicated-mentions-csv",
        default="episode_extraction_results/clinic_like_20k_30k/rq1/adjudicated/rq1_adjudicated_mentions.csv",
    )
    p.add_argument(
        "--normalization-detailed-csv",
        default="episode_extraction_results/clinic_like_20k_30k/rq1/normalization_eval/rq1_normalization_eval_detailed.csv",
    )
    p.add_argument(
        "--canonical-vocab-path",
        default="resources/lexicons/rq1_drug_canonical_vocab.csv",
    )
    p.add_argument(
        "--alias-artifact",
        default="resources/lexicons/rq1_drug_aliases.csv",
    )
    p.add_argument(
        "--patha-exclusions-csv",
        default="resources/manual/pathA_alias_exclusions.csv",
    )
    p.add_argument(
        "--sequence-topk-from-char",
        type=int,
        default=40,
        help="Candidate shortlist for token-jaccard and edit-distance comparators.",
    )
    p.add_argument(
        "--omop-dir",
        default="resources/raw/OMOP_related",
        help="OMOP vocabulary directory for the public ontology/synonym baseline.",
    )
    p.add_argument(
        "--output-dir",
        default="episode_extraction_results/clinic_like_20k_30k/rq1/classical_comparators",
    )
    return p.parse_args()


def _basic_surface_norm(text: str) -> str:
    return " ".join(str(text).strip().lower().split())


def _tokenize(text: str) -> list[str]:
    return [tok for tok in str(text).split() if tok]


def _token_jaccard(left: str, right: str) -> float:
    ls = set(_tokenize(left))
    rs = set(_tokenize(right))
    if not ls and not rs:
        return 1.0
    if not ls or not rs:
        return 0.0
    return len(ls & rs) / len(ls | rs)


def _pick_best_by_score(query: str, candidates: Sequence[str], scorer) -> tuple[str, float]:
    best_label = ""
    best_score = -1.0
    for candidate in candidates:
        score = float(scorer(query, candidate))
        if score > best_score:
            best_label = candidate
            best_score = score
    return best_label, max(best_score, 0.0)


def _priority_rank(series: pd.Series, lookup: Dict[str, int], default: int = 9999) -> pd.Series:
    return series.astype(str).map(lambda x: lookup.get(x, default))


def _build_public_ontology_exact_map(omop_dir: Path) -> tuple[Dict[str, str], set[str]]:
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
    synonym = pd.read_csv(
        omop_dir / "CONCEPT_SYNONYM.csv",
        sep="\t",
        usecols=["concept_id", "concept_synonym_name"],
        low_memory=False,
    )
    strength = pd.read_csv(
        omop_dir / "DRUG_STRENGTH.csv",
        sep="\t",
        usecols=["drug_concept_id", "ingredient_concept_id", "invalid_reason"],
        low_memory=False,
    )

    class_priority = {
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
    vocab_priority = {
        "RxNorm": 0,
        "RxNorm Extension": 1,
        "ATC": 2,
        "VA Class": 3,
        "NDC": 4,
    }

    standard = concept[
        (concept["domain_id"] == "Drug")
        & concept["invalid_reason"].isna()
        & concept["standard_concept"].isin(["S", "C"])
    ][["concept_id", "concept_name", "vocabulary_id", "concept_class_id"]].copy()

    ingredient_names = concept[["concept_id", "concept_name"]].rename(
        columns={"concept_id": "ingredient_concept_id", "concept_name": "ingredient_name"}
    )
    strength = strength[strength["invalid_reason"].isna()].drop_duplicates()
    ing = strength.merge(ingredient_names, on="ingredient_concept_id", how="left")
    ing["ingredient_name_norm"] = ing["ingredient_name"].map(normalize_drug_text)
    ing_agg = (
        ing.groupby("drug_concept_id", as_index=False)
        .agg(
            ingredient_name_norms=("ingredient_name_norm", lambda xs: "|".join(sorted(set(x for x in xs if x)))),
            ingredient_count=("ingredient_concept_id", lambda xs: len(set(x for x in xs if pd.notna(x)))),
        )
    )

    standard = standard.merge(ing_agg, left_on="concept_id", right_on="drug_concept_id", how="left")
    standard["concept_name_norm"] = standard["concept_name"].map(normalize_drug_text)
    standard["class_priority"] = _priority_rank(standard["concept_class_id"], class_priority)
    standard["vocab_priority"] = _priority_rank(standard["vocabulary_id"], vocab_priority)

    def _public_label(row: pd.Series) -> str:
        ingredients = [x for x in str(row.get("ingredient_name_norms", "") or "").split("|") if x]
        if len(ingredients) == 1:
            return ingredients[0]
        return str(row.get("concept_name_norm", "") or "")

    standard["public_predicted_label"] = standard.apply(_public_label, axis=1)

    name_hits = standard[["concept_id", "concept_name_norm", "public_predicted_label", "class_priority", "vocab_priority"]].copy()
    name_hits = name_hits.rename(columns={"concept_name_norm": "term_norm"})
    name_hits["match_method"] = "concept_name_exact"

    synonym["term_norm"] = synonym["concept_synonym_name"].map(normalize_drug_text)
    synonym_hits = synonym[synonym["term_norm"].astype(str) != ""].merge(
        standard[["concept_id", "public_predicted_label", "class_priority", "vocab_priority"]],
        on="concept_id",
        how="inner",
    )
    synonym_hits["match_method"] = "concept_synonym_exact"

    candidates = pd.concat(
        [
            name_hits[["term_norm", "public_predicted_label", "class_priority", "vocab_priority", "match_method"]],
            synonym_hits[["term_norm", "public_predicted_label", "class_priority", "vocab_priority", "match_method"]],
        ],
        ignore_index=True,
    )
    candidates = candidates[candidates["term_norm"].astype(str) != ""].copy()
    candidates["match_priority"] = candidates["match_method"].map({"concept_name_exact": 0, "concept_synonym_exact": 1}).fillna(9)
    candidates = candidates.sort_values(
        ["term_norm", "match_priority", "class_priority", "vocab_priority", "public_predicted_label"]
    ).drop_duplicates("term_norm")
    return dict(zip(candidates["term_norm"], candidates["public_predicted_label"])), set(candidates["term_norm"].astype(str))


def _build_summary_rows(
    detail: pd.DataFrame,
    methods: Sequence[tuple[str, str]],
    patha_col: str,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for label, col in methods:
        pred = detail[col].fillna("").astype(str).str.strip().str.lower()
        gold = detail["gold_norm"]
        exact = float((pred == gold).mean()) if len(detail) else 0.0
        nonempty = float((pred != "").mean()) if len(detail) else 0.0
        delta_vs_patha = exact - float((detail[patha_col] == gold).mean()) if len(detail) else 0.0
        rows.append(
            {
                "method": label,
                "n_mentions": int(len(detail)),
                "exact_canonical_agreement": round(exact, 6),
                "nonempty_prediction_rate": round(nonempty, 6),
                "delta_vs_full_patha": round(delta_vs_patha, 6),
            }
        )
    return pd.DataFrame(rows)


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[2]
    adjudicated_path = repo_root / args.adjudicated_mentions_csv
    norm_detail_path = repo_root / args.normalization_detailed_csv
    canonical_vocab_path = repo_root / args.canonical_vocab_path
    alias_path = repo_root / args.alias_artifact
    exclusions_path = repo_root / args.patha_exclusions_csv
    omop_dir = repo_root / args.omop_dir
    out_dir = repo_root / args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    if not adjudicated_path.exists():
        raise FileNotFoundError(f"Missing adjudicated mentions CSV: {adjudicated_path}")
    if not norm_detail_path.exists():
        raise FileNotFoundError(f"Missing normalization detail CSV: {norm_detail_path}")

    adjudicated = pd.read_csv(adjudicated_path).fillna("")
    adjudicated = adjudicated[adjudicated["adjudicated_canonical_label"].astype(str).str.strip() != ""].copy()
    norm_detail = pd.read_csv(norm_detail_path).fillna("")

    join_cols = ["adjudication_unit_id", "raw_mention_text", "gold_canonical", "patha_prediction"]
    missing = [c for c in join_cols if c not in norm_detail.columns]
    if missing:
        raise ValueError(f"Normalization detail CSV missing columns: {missing}")

    detail = adjudicated.merge(
        norm_detail[join_cols].drop_duplicates("adjudication_unit_id"),
        on=["adjudication_unit_id", "raw_mention_text"],
        how="left",
    )
    detail["gold_norm"] = detail["adjudicated_canonical_label"].map(normalize_drug_text)
    detail["surface_exact_norm"] = detail["raw_mention_text"].map(_basic_surface_norm)
    detail["lexical_cleanup_norm"] = detail["raw_mention_text"].map(normalize_drug_text)

    exclusions = load_alias_exclusions(exclusions_path if exclusions_path.exists() else None)
    alias_map = load_alias_map(alias_path, exclusions=exclusions, enforce_one_to_one=True) if alias_path.exists() else {}
    detail["curated_alias_norm"] = detail["raw_mention_text"].map(lambda x: canonicalize_drug(str(x), alias_map))
    detail["patha_full_norm"] = detail["patha_prediction"].astype(str).str.strip().str.lower()
    public_ontology_map, public_ontology_term_set = _build_public_ontology_exact_map(omop_dir) if omop_dir.exists() else ({}, set())
    detail["public_ontology_preserve_norm"] = detail["lexical_cleanup_norm"].map(
        lambda x: str(x) if str(x) in public_ontology_term_set else ""
    )
    detail["public_ontology_synonym_norm"] = detail["lexical_cleanup_norm"].map(lambda x: public_ontology_map.get(str(x), ""))

    universe = build_canonical_drug_universe(
        alias_map=alias_map,
        canonical_vocab_path=canonical_vocab_path if canonical_vocab_path.exists() else None,
        adjudicated_labels_path=adjudicated_path,
    )
    canonical_concepts = sorted(universe.by_norm.keys())
    char_linker = CharNgramLinker(canonical_concepts)
    shortlist_k = max(int(args.sequence_topk_from_char), 5)

    query_cache: dict[str, dict[str, object]] = {}
    for query in sorted(set(detail["lexical_cleanup_norm"].astype(str))):
        if not query:
            query_cache[query] = {
                "char_pred": "",
                "char_score": 0.0,
                "seq_pred": "",
                "seq_score": 0.0,
                "jacc_pred": "",
                "jacc_score": 0.0,
            }
            continue

        char_best, char_score = char_linker.best(query)
        shortlist = [label for label, _score in char_linker.topk(query, k=shortlist_k)]
        if not shortlist:
            shortlist = canonical_concepts

        seq_best, seq_score = _pick_best_by_score(
            query,
            shortlist,
            lambda q, cand: SequenceMatcher(None, q, cand).ratio(),
        )
        jacc_best, jacc_score = _pick_best_by_score(query, shortlist, _token_jaccard)
        query_cache[query] = {
            "char_pred": char_best,
            "char_score": round(char_score, 6),
            "seq_pred": seq_best,
            "seq_score": round(seq_score, 6),
            "jacc_pred": jacc_best,
            "jacc_score": round(jacc_score, 6),
        }

    char_predictions = [query_cache[q]["char_pred"] for q in detail["lexical_cleanup_norm"].astype(str)]
    char_scores = [query_cache[q]["char_score"] for q in detail["lexical_cleanup_norm"].astype(str)]
    seq_predictions = [query_cache[q]["seq_pred"] for q in detail["lexical_cleanup_norm"].astype(str)]
    seq_scores = [query_cache[q]["seq_score"] for q in detail["lexical_cleanup_norm"].astype(str)]
    jacc_predictions = [query_cache[q]["jacc_pred"] for q in detail["lexical_cleanup_norm"].astype(str)]
    jacc_scores = [query_cache[q]["jacc_score"] for q in detail["lexical_cleanup_norm"].astype(str)]

    detail["char_ngram_top1"] = char_predictions
    detail["char_ngram_score"] = char_scores
    detail["sequence_matcher_top1"] = seq_predictions
    detail["sequence_matcher_score"] = seq_scores
    detail["token_jaccard_top1"] = jacc_predictions
    detail["token_jaccard_score"] = jacc_scores

    methods = [
        ("surface-exact baseline", "surface_exact_norm"),
        ("lexical cleanup", "lexical_cleanup_norm"),
        ("curated alias map", "curated_alias_norm"),
        ("OMOP/RxNorm public synonym preserve-term", "public_ontology_preserve_norm"),
        ("OMOP/RxNorm public synonym exact", "public_ontology_synonym_norm"),
        ("char-ngram nearest canonical", "char_ngram_top1"),
        ("SequenceMatcher nearest canonical", "sequence_matcher_top1"),
        ("token-Jaccard nearest canonical", "token_jaccard_top1"),
        ("full Path A", "patha_full_norm"),
    ]
    summary = _build_summary_rows(detail, methods=methods, patha_col="patha_full_norm")

    error_examples = []
    for label, col in [
        ("OMOP/RxNorm public synonym preserve-term", "public_ontology_preserve_norm"),
        ("OMOP/RxNorm public synonym exact", "public_ontology_synonym_norm"),
        ("char-ngram nearest canonical", "char_ngram_top1"),
        ("SequenceMatcher nearest canonical", "sequence_matcher_top1"),
        ("token-Jaccard nearest canonical", "token_jaccard_top1"),
    ]:
        wrong = detail[detail[col].astype(str).str.strip().str.lower() != detail["gold_norm"]].copy()
        wrong["query_norm"] = detail.loc[wrong.index, "lexical_cleanup_norm"]
        top = (
            wrong.groupby(["query_norm", "gold_norm", col], as_index=False)
            .size()
            .sort_values(["size", "query_norm"], ascending=[False, True])
            .head(20)
            .assign(method=label)
        )
        error_examples.append(top.rename(columns={col: "predicted_norm", "size": "count"}))

    error_examples_df = pd.concat(error_examples, ignore_index=True) if error_examples else pd.DataFrame()

    detail_out = out_dir / "rq1_classical_comparator_detailed.csv"
    summary_out = out_dir / "rq1_classical_comparator_summary.csv"
    error_out = out_dir / "rq1_classical_comparator_top_errors.csv"
    json_out = out_dir / "rq1_classical_comparator_summary.json"

    detail[
        [
            "adjudication_unit_id",
            "raw_mention_text",
            "context_text",
            "adjudicated_canonical_label",
            "gold_norm",
            "surface_exact_norm",
            "lexical_cleanup_norm",
            "curated_alias_norm",
            "public_ontology_preserve_norm",
            "public_ontology_synonym_norm",
            "char_ngram_top1",
            "char_ngram_score",
            "sequence_matcher_top1",
            "sequence_matcher_score",
            "token_jaccard_top1",
            "token_jaccard_score",
            "patha_full_norm",
        ]
    ].to_csv(detail_out, index=False)
    summary.to_csv(summary_out, index=False)
    error_examples_df.to_csv(error_out, index=False)
    json_out.write_text(
        json.dumps(
            {
                "inputs": {
                    "adjudicated_mentions_csv": str(adjudicated_path),
                    "normalization_detailed_csv": str(norm_detail_path),
                    "canonical_vocab_path": str(canonical_vocab_path),
                    "alias_artifact": str(alias_path),
                    "omop_dir": str(omop_dir),
                },
                "metrics": summary.to_dict(orient="records"),
                "outputs": {
                    "detailed_csv": str(detail_out),
                    "summary_csv": str(summary_out),
                    "top_errors_csv": str(error_out),
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    print(f"Saved comparator summary: {summary_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
