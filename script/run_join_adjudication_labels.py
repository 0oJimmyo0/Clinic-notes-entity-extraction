#!/usr/bin/env python3
"""
Join human-reviewed adjudication labels back to seeded extracted mentions.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from rq1_adjudication_utils import (
    COMPARE_VALUES,
    STATUS_VALUES,
    normalize_join_text,
    write_run_summary,
)
from rq1_drug_linking import build_canonical_drug_universe, canonicalize_drug, load_alias_map


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Join adjudication labels to extracted mention packets.")
    p.add_argument(
        "--packets-mentions-csv",
        default="episode_extraction_results/rq1/adjudication_packets/adjudication_packets_mentions.csv",
        help="Seed mention packets CSV from run_build_adjudication_packets.py",
    )
    p.add_argument(
        "--reviewed-adjudication-csv",
        default="",
        help="Human-reviewed adjudication CSV. Defaults to packets CSV if omitted.",
    )
    p.add_argument(
        "--alias-artifact",
        default="lexicons/rq1_drug_aliases.csv",
        help="Path A alias CSV/JSON.",
    )
    p.add_argument(
        "--canonical-vocab-path",
        default="lexicons/rq1_drug_canonical_vocab.csv",
        help="Optional canonical vocabulary used to flag Path A leftovers.",
    )
    p.add_argument(
        "--include-uncertain-downstream",
        action="store_true",
        help="Include compare_to_structured_ehr=uncertain in downstream comparable output.",
    )
    p.add_argument(
        "--output-dir",
        default="episode_extraction_results/rq1/adjudicated",
        help="Output directory.",
    )
    return p.parse_args()


def _normalize_bool_str(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().str.lower()


def _load_review_df(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    required = {
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
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Reviewed adjudication CSV missing columns: {sorted(missing)}")
    return df


def _build_seed_indexes(seed_df: pd.DataFrame) -> Tuple[Dict[str, int], Dict[Tuple[str, str, str], List[int]], Dict[Tuple[str, str], List[int]]]:
    by_unit: Dict[str, int] = {}
    by_span_mention: Dict[Tuple[str, str, str], List[int]] = {}
    by_note_mention: Dict[Tuple[str, str], List[int]] = {}
    for idx, row in seed_df.iterrows():
        by_unit[str(row["adjudication_unit_id"]).strip()] = idx
        span_key = (
            str(row["note_id"]).strip(),
            str(row["span_id_or_local_reference"]).strip(),
            normalize_join_text(row["raw_mention_text"]),
        )
        by_span_mention.setdefault(span_key, []).append(idx)
        note_key = (str(row["note_id"]).strip(), normalize_join_text(row["raw_mention_text"]))
        by_note_mention.setdefault(note_key, []).append(idx)
    return by_unit, by_span_mention, by_note_mention


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    packets_path = (root / args.packets_mentions_csv).resolve()
    reviewed_path = (root / args.reviewed_adjudication_csv).resolve() if args.reviewed_adjudication_csv else packets_path
    alias_path = (root / args.alias_artifact).resolve()
    vocab_path = (root / args.canonical_vocab_path).resolve() if args.canonical_vocab_path else None
    out_dir = (root / args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    if not packets_path.exists():
        raise FileNotFoundError(f"Missing packets mentions CSV: {packets_path}")
    if not reviewed_path.exists():
        raise FileNotFoundError(f"Missing reviewed adjudication CSV: {reviewed_path}")

    seed_df = pd.read_csv(packets_path).fillna("")
    review_df = _load_review_df(reviewed_path).fillna("")

    for df in (seed_df, review_df):
        for col in ["adjudication_unit_id", "person_id", "visit_id", "note_id", "span_id_or_local_reference", "raw_mention_text"]:
            df[col] = df[col].astype(str).str.strip()
        df["raw_mention_norm"] = df["raw_mention_text"].map(normalize_join_text)

    review_df["mention_status"] = _normalize_bool_str(review_df["mention_status"])
    review_df["compare_to_structured_ehr"] = _normalize_bool_str(review_df["compare_to_structured_ehr"])
    review_df["adjudicated_canonical_label"] = review_df["adjudicated_canonical_label"].astype(str).str.strip().str.lower()

    bad_status = sorted(set(review_df["mention_status"]) - {"", *STATUS_VALUES})
    bad_compare = sorted(set(review_df["compare_to_structured_ehr"]) - {"", *COMPARE_VALUES})
    if bad_status:
        raise ValueError(f"Unexpected mention_status values: {bad_status}")
    if bad_compare:
        raise ValueError(f"Unexpected compare_to_structured_ehr values: {bad_compare}")

    by_unit, by_span_mention, by_note_mention = _build_seed_indexes(seed_df)
    matched_seed_idx = set()
    joined_rows: List[Dict] = []
    adjudicated_rows: List[Dict] = []

    for _, truth in review_df.iterrows():
        join_method = ""
        audit_flag = ""
        seed_idx = None
        unit_id = truth["adjudication_unit_id"]
        if unit_id and unit_id in by_unit:
            seed_idx = by_unit[unit_id]
            join_method = "adjudication_unit_id"
        else:
            span_key = (
                truth["note_id"],
                truth["span_id_or_local_reference"],
                truth["raw_mention_norm"],
            )
            idxs = by_span_mention.get(span_key, [])
            if len(idxs) == 1:
                seed_idx = idxs[0]
                join_method = "note_span_mention"
            elif len(idxs) > 1:
                seed_idx = idxs[0]
                join_method = "note_span_mention_multi"
                audit_flag = "multiple_seed_candidates_same_span_mention"
            else:
                note_key = (truth["note_id"], truth["raw_mention_norm"])
                idxs = by_note_mention.get(note_key, [])
                if len(idxs) == 1:
                    seed_idx = idxs[0]
                    join_method = "note_mention"
                elif len(idxs) > 1:
                    seed_idx = idxs[0]
                    join_method = "note_mention_multi"
                    audit_flag = "multiple_seed_candidates_same_note_mention"

        seed_row = seed_df.loc[seed_idx].to_dict() if seed_idx is not None else {}
        if seed_idx is not None:
            matched_seed_idx.add(seed_idx)

        adjudicated_row = {
            "adjudication_unit_id": truth["adjudication_unit_id"] or seed_row.get("adjudication_unit_id", ""),
            "person_id": truth["person_id"] or seed_row.get("person_id", ""),
            "visit_id": truth["visit_id"] or seed_row.get("visit_id", ""),
            "note_id": truth["note_id"] or seed_row.get("note_id", ""),
            "span_id_or_local_reference": truth["span_id_or_local_reference"] or seed_row.get("span_id_or_local_reference", ""),
            "raw_mention_text": truth["raw_mention_text"],
            "context_text": truth["context_text"] or seed_row.get("context_text", ""),
            "adjudicated_canonical_label": truth["adjudicated_canonical_label"],
            "mention_status": truth["mention_status"],
            "compare_to_structured_ehr": truth["compare_to_structured_ehr"],
            "reviewer_notes": truth["reviewer_notes"],
            "join_method": join_method or "unmatched_truth_row",
            "audit_flag": audit_flag,
        }
        adjudicated_rows.append(adjudicated_row)

        if truth["raw_mention_text"] or truth["adjudicated_canonical_label"] or truth["mention_status"]:
            joined_rows.append(
                {
                    "alignment_status": "matched" if seed_idx is not None else "false_negative",
                    "join_method": join_method or "unmatched_truth_row",
                    "audit_flag": audit_flag,
                    "adjudication_unit_id": adjudicated_row["adjudication_unit_id"],
                    "person_id": adjudicated_row["person_id"],
                    "visit_id": adjudicated_row["visit_id"],
                    "note_id": adjudicated_row["note_id"],
                    "span_id_or_local_reference": adjudicated_row["span_id_or_local_reference"],
                    "raw_mention_text_truth": adjudicated_row["raw_mention_text"],
                    "raw_mention_text_extracted": seed_row.get("raw_mention_text", ""),
                    "context_text": adjudicated_row["context_text"],
                    "seed_treatment_action": seed_row.get("seed_treatment_action", ""),
                    "seed_discontinuation_reason": seed_row.get("seed_discontinuation_reason", ""),
                    "seed_certainty": seed_row.get("seed_certainty", ""),
                    "adjudicated_canonical_label": adjudicated_row["adjudicated_canonical_label"],
                    "mention_status": adjudicated_row["mention_status"],
                    "compare_to_structured_ehr": adjudicated_row["compare_to_structured_ehr"],
                    "reviewer_notes": adjudicated_row["reviewer_notes"],
                }
            )

    unmatched_seed = seed_df.loc[~seed_df.index.isin(matched_seed_idx)].copy()
    for _, row in unmatched_seed.iterrows():
        joined_rows.append(
            {
                "alignment_status": "false_positive",
                "join_method": "unmatched_seed_row",
                "audit_flag": "needs_manual_review_if_true_mention_missed_in_adjudication",
                "adjudication_unit_id": row["adjudication_unit_id"],
                "person_id": row["person_id"],
                "visit_id": row["visit_id"],
                "note_id": row["note_id"],
                "span_id_or_local_reference": row["span_id_or_local_reference"],
                "raw_mention_text_truth": "",
                "raw_mention_text_extracted": row["raw_mention_text"],
                "context_text": row.get("context_text", ""),
                "seed_treatment_action": row.get("seed_treatment_action", ""),
                "seed_discontinuation_reason": row.get("seed_discontinuation_reason", ""),
                "seed_certainty": row.get("seed_certainty", ""),
                "adjudicated_canonical_label": "",
                "mention_status": "",
                "compare_to_structured_ehr": "",
                "reviewer_notes": "",
            }
        )

    adjudicated_df = pd.DataFrame(adjudicated_rows).drop_duplicates(subset=["adjudication_unit_id", "raw_mention_text", "adjudicated_canonical_label", "mention_status"])
    join_df = pd.DataFrame(joined_rows)

    # Visit-level adjudicated note-grounded drug labels.
    valid_truth = adjudicated_df[adjudicated_df["adjudicated_canonical_label"].astype(str).str.strip() != ""].copy()
    allowed_compare = {"yes"} | ({"uncertain"} if args.include_uncertain_downstream else set())
    downstream_df = adjudicated_df[adjudicated_df["compare_to_structured_ehr"].isin(allowed_compare)].copy()
    comparable_truth = valid_truth[valid_truth["compare_to_structured_ehr"].isin(allowed_compare)].copy()

    visit_truth = (
        valid_truth.groupby(["person_id", "visit_id"], as_index=False)
        .agg(
            adjudicated_drugs_json=("adjudicated_canonical_label", lambda x: json.dumps(sorted(set(x)))),
            mention_count=("adjudicated_canonical_label", "count"),
        )
        .copy()
    )
    comparable_by_visit = (
        comparable_truth.groupby(["person_id", "visit_id"], as_index=False)
        .agg(
            adjudicated_comparable_drugs_json=(
                "adjudicated_canonical_label",
                lambda x: json.dumps(sorted(set(x))),
            ),
            comparable_mention_count=("adjudicated_canonical_label", "count"),
        )
        .copy()
        if len(comparable_truth)
        else pd.DataFrame(columns=["person_id", "visit_id", "adjudicated_comparable_drugs_json", "comparable_mention_count"])
    )
    visit_truth = visit_truth.merge(comparable_by_visit, on=["person_id", "visit_id"], how="left").fillna(
        {"adjudicated_comparable_drugs_json": "[]", "comparable_mention_count": 0}
    )

    # Path A leftovers for Path B calibration/review.
    alias_map = load_alias_map(alias_path) if alias_path.exists() else {}
    universe = build_canonical_drug_universe(alias_map=alias_map, canonical_vocab_path=vocab_path)
    leftover_rows = []
    for row in valid_truth.itertuples(index=False):
        raw = str(getattr(row, "raw_mention_text", "") or "")
        patha = canonicalize_drug(raw, alias_map)
        exact = universe.synonym_to_canonical.get(patha, "")
        gold = str(getattr(row, "adjudicated_canonical_label", "") or "").strip().lower()
        if exact == gold:
            continue
        leftover_rows.append(
            {
                "adjudication_unit_id": getattr(row, "adjudication_unit_id"),
                "person_id": getattr(row, "person_id"),
                "visit_id": getattr(row, "visit_id"),
                "note_id": getattr(row, "note_id"),
                "span_id_or_local_reference": getattr(row, "span_id_or_local_reference"),
                "raw_mention_text": raw,
                "patha_term": patha,
                "patha_exact_vocab_hit": bool(exact),
                "patha_exact_vocab_label": exact,
                "adjudicated_canonical_label": gold,
                "mention_status": getattr(row, "mention_status"),
                "compare_to_structured_ehr": getattr(row, "compare_to_structured_ehr"),
                "reviewer_notes": getattr(row, "reviewer_notes"),
            }
        )

    adjudicated_path = out_dir / "rq1_adjudicated_mentions.csv"
    join_path = out_dir / "rq1_extraction_vs_truth_mentions.csv"
    visit_path = out_dir / "rq1_visit_adjudicated_drugs.csv"
    leftovers_path = out_dir / "rq1_pathb_leftovers.csv"
    downstream_path = out_dir / "rq1_downstream_comparable_mentions.csv"
    summary_path = out_dir / "rq1_join_adjudication_summary.json"

    adjudicated_df.to_csv(adjudicated_path, index=False)
    join_df.to_csv(join_path, index=False)
    visit_truth.to_csv(visit_path, index=False)
    pd.DataFrame(leftover_rows).to_csv(leftovers_path, index=False)
    downstream_df.to_csv(downstream_path, index=False)

    write_run_summary(
        summary_path,
        {
            "inputs": {
                "packets_mentions_csv": str(packets_path),
                "reviewed_adjudication_csv": str(reviewed_path),
                "alias_artifact": str(alias_path),
                "canonical_vocab_path": str(vocab_path) if vocab_path and vocab_path.exists() else None,
            },
            "counts": {
                "seed_mentions": int(len(seed_df)),
                "review_rows": int(len(review_df)),
                "adjudicated_mentions": int(len(adjudicated_df)),
                "join_rows": int(len(join_df)),
                "false_positive_rows": int((join_df["alignment_status"] == "false_positive").sum()) if len(join_df) else 0,
                "false_negative_rows": int((join_df["alignment_status"] == "false_negative").sum()) if len(join_df) else 0,
                "pathb_leftovers": int(len(leftover_rows)),
                "downstream_comparable_mentions": int(len(downstream_df)),
            },
            "outputs": {
                "rq1_adjudicated_mentions_csv": str(adjudicated_path),
                "rq1_extraction_vs_truth_mentions_csv": str(join_path),
                "rq1_visit_adjudicated_drugs_csv": str(visit_path),
                "rq1_pathb_leftovers_csv": str(leftovers_path),
                "rq1_downstream_comparable_mentions_csv": str(downstream_path),
            },
        },
    )

    print(f"Saved adjudicated mentions: {adjudicated_path}")
    print(f"Saved extraction-vs-truth joins: {join_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
