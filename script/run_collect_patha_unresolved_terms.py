#!/usr/bin/env python3
"""
Collect high-frequency unresolved Path A drug terms.

This script mines the note-entity visit table and reports unresolved terms after
deterministic Path A handling, so manual review remains bounded and targeted.
"""

from __future__ import annotations

import argparse
import ast
import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Sequence, Set, Tuple

import pandas as pd

from rq1_drug_linking import (
    build_canonical_drug_universe,
    canonicalize_drug,
    load_alias_map,
)


def _parse_list_cell(x) -> List[str]:
    if x is None:
        return []
    if isinstance(x, list):
        vals = x
    else:
        s = str(x).strip()
        if s == "" or s.lower() in {"none", "nan"}:
            return []
        try:
            vals = ast.literal_eval(s)
        except Exception:
            try:
                vals = json.loads(s)
            except Exception:
                return []
    if not isinstance(vals, list):
        return []
    out = []
    for v in vals:
        t = str(v).strip()
        if t:
            out.append(t)
    return out


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Collect unresolved Path A drug terms for review.")
    p.add_argument(
        "--note-csv",
        default="episode_extraction_results/rq1/rq1_note_entities_by_visit.csv",
        help="Visit-level note entities CSV (must include person_id, visit_id, drugs).",
    )
    p.add_argument(
        "--drugs-col",
        default="drugs",
        help="Column in note CSV containing JSON/list-like drug terms.",
    )
    p.add_argument(
        "--alias-json",
        default="resources/manual/pathA_alias_map.json",
        help="Path A alias map JSON, either flat or structured.",
    )
    p.add_argument(
        "--canonical-vocab-path",
        default="",
        help="Optional canonical vocab CSV/JSON for deterministic exact-vocab handling.",
    )
    p.add_argument(
        "--adjudicated-labels-csv",
        default="",
        help="Optional adjudicated labels CSV to enrich deterministic vocab.",
    )
    p.add_argument(
        "--top-k",
        type=int,
        default=500,
        help="Maximum unresolved terms to output.",
    )
    p.add_argument(
        "--min-count",
        type=int,
        default=2,
        help="Minimum unresolved mention count to output.",
    )
    p.add_argument(
        "--output-csv",
        default="episode_extraction_results/rq1/diagnostics/rq1_patha_unresolved_terms.csv",
        help="Output CSV path.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[2]

    note_path = (root / args.note_csv).resolve()
    alias_path = (root / args.alias_json).resolve()
    vocab_path = (root / args.canonical_vocab_path).resolve() if args.canonical_vocab_path else None
    adjud_path = (root / args.adjudicated_labels_csv).resolve() if args.adjudicated_labels_csv else None
    out_path = (root / args.output_csv).resolve()

    if not note_path.exists():
        raise FileNotFoundError(f"Missing note CSV: {note_path}")

    alias_map = load_alias_map(alias_path)
    universe = build_canonical_drug_universe(
        alias_map=alias_map,
        canonical_vocab_path=vocab_path,
        adjudicated_labels_path=adjud_path,
    )

    use_cols = ["person_id", "visit_id", args.drugs_col]
    note_df = pd.read_csv(note_path, usecols=lambda c: c in set(use_cols))
    for c in ["person_id", "visit_id", args.drugs_col]:
        if c not in note_df.columns:
            raise ValueError(f"Missing required column: {c}")

    unresolved_mentions = defaultdict(int)
    unresolved_visits: Dict[str, Set[Tuple[str, str]]] = defaultdict(set)
    unresolved_persons: Dict[str, Set[str]] = defaultdict(set)
    unresolved_examples: Dict[str, Set[str]] = defaultdict(set)

    for _, row in note_df.iterrows():
        pid = str(row["person_id"]).strip()
        vid = str(row["visit_id"]).strip()
        raw_terms = _parse_list_cell(row[args.drugs_col])
        for raw in raw_terms:
            c = canonicalize_drug(raw, alias_map)
            if not c:
                continue
            # Path A deterministic handled set in canonical mode:
            # exact match to canonical vocab synonym map.
            if c in universe.synonym_to_canonical:
                continue
            unresolved_mentions[c] += 1
            unresolved_visits[c].add((pid, vid))
            unresolved_persons[c].add(pid)
            if len(unresolved_examples[c]) < 5:
                unresolved_examples[c].add(str(raw))

    rows = []
    for term, n in unresolved_mentions.items():
        if n < max(int(args.min_count), 1):
            continue
        rows.append(
            {
                "term_norm": term,
                "mention_count": int(n),
                "visit_count": int(len(unresolved_visits[term])),
                "person_count": int(len(unresolved_persons[term])),
                "example_raw_terms": "|".join(sorted(unresolved_examples[term])),
            }
        )

    rows.sort(key=lambda x: (x["mention_count"], x["visit_count"]), reverse=True)
    rows = rows[: max(int(args.top_k), 0)]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["term_norm", "mention_count", "visit_count", "person_count", "example_raw_terms"],
        )
        w.writeheader()
        w.writerows(rows)

    print(f"Saved unresolved terms: {out_path} (rows={len(rows):,})")
    print(f"Deterministic vocab size used for Path A: {len(universe.candidates):,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
