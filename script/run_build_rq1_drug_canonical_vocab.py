#!/usr/bin/env python3
"""
Build RQ1 Path B canonical drug vocabulary table.

This script creates a first-pass canonical candidate universe without requiring
hand-crafting the full drug concept set.

Primary inputs (any subset):
- Alias map JSON (clinic-local shorthand / brand->generic)
- Public-source term lexicon (e.g., RxNorm-derived single-column CSV)
- Adjudicated canonical labels CSV (optional)
- Unresolved term frequency CSV for targeted review queue (optional)

Primary outputs:
- Canonical vocabulary CSV for Path B: canonical_label + synonyms
- Unresolved review queue CSV for bounded human review
"""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Set, Tuple

from rq1_drug_linking import load_alias_map, normalize_drug_text


def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        return list(csv.DictReader(f))


def _read_single_term_csv(path: Path, term_col: str = "term") -> List[str]:
    if not path.exists():
        return []
    rows = _read_csv_rows(path)
    if not rows:
        return []

    cols = list(rows[0].keys())
    if term_col in cols:
        use_col = term_col
    else:
        use_col = cols[0]

    out: List[str] = []
    for r in rows:
        v = str(r.get(use_col, "")).strip()
        if v:
            out.append(v)
    return out


def _read_adjudicated_labels(path: Path) -> List[str]:
    if not path.exists():
        return []
    rows = _read_csv_rows(path)
    if not rows:
        return []

    cols = {c.lower(): c for c in rows[0].keys()}
    label_col = (
        cols.get("adjudicated_drug_label")
        or cols.get("canonical_label")
        or cols.get("drug_label")
        or cols.get("label")
        or cols.get("drug")
    )
    if label_col is None:
        return []

    out = []
    for r in rows:
        v = str(r.get(label_col, "")).strip()
        if v:
            out.append(v)
    return out


def _read_unresolved_terms(
    path: Path,
    term_col: str,
    count_col: str,
    min_freq: int,
) -> List[Tuple[str, int]]:
    if not path.exists():
        return []
    rows = _read_csv_rows(path)
    out: List[Tuple[str, int]] = []
    for r in rows:
        t = str(r.get(term_col, "")).strip()
        if not t:
            continue
        try:
            c = int(float(r.get(count_col, "0") or 0))
        except Exception:
            c = 0
        if c >= min_freq:
            out.append((t, c))
    out.sort(key=lambda x: x[1], reverse=True)
    return out


def _add_entry(
    table: Dict[str, Dict[str, Set[str]]],
    canonical_raw: str,
    synonyms_raw: Iterable[str],
    source_tag: str,
) -> None:
    cn = normalize_drug_text(canonical_raw)
    if not cn:
        return
    bucket = table.setdefault(cn, {"synonyms": set(), "sources": set()})
    bucket["sources"].add(source_tag)
    bucket["synonyms"].add(cn)
    for s in synonyms_raw:
        sn = normalize_drug_text(s)
        if sn:
            bucket["synonyms"].add(sn)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build canonical drug vocabulary table for RQ1 Path B.")
    p.add_argument(
        "--alias-json",
        default="resources/lexicons/rq1_drug_aliases.csv",
        help="Path A alias artifact, either CSV or JSON.",
    )
    p.add_argument(
        "--rxnorm-terms-csv",
        default="resources/lexicons/ehr_entities__drugs.csv",
        help="Public-source drug term CSV (single term column), often generated from RxNorm utilities.",
    )
    p.add_argument(
        "--adjudicated-labels-csv",
        default="",
        help="Optional adjudicated labels CSV containing canonical labels.",
    )
    p.add_argument(
        "--unresolved-terms-csv",
        default="",
        help="Optional unresolved-term frequency CSV for review queue generation.",
    )
    p.add_argument(
        "--unresolved-term-col",
        default="term_norm",
        help="Term column in unresolved terms CSV.",
    )
    p.add_argument(
        "--unresolved-count-col",
        default="mention_count",
        help="Count column in unresolved terms CSV.",
    )
    p.add_argument(
        "--min-unresolved-freq",
        type=int,
        default=5,
        help="Minimum unresolved frequency to include in review queue.",
    )
    p.add_argument(
        "--output-vocab-csv",
        default="resources/lexicons/rq1_drug_canonical_vocab.csv",
        help="Output canonical vocabulary CSV.",
    )
    p.add_argument(
        "--output-review-csv",
        default="resources/lexicons/rq1_drug_unresolved_review_queue.csv",
        help="Output unresolved review queue CSV.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[2]

    alias_path = (root / args.alias_json).resolve()
    rxnorm_path = (root / args.rxnorm_terms_csv).resolve()
    adjud_path = (root / args.adjudicated_labels_csv).resolve() if args.adjudicated_labels_csv else None
    unresolved_path = (root / args.unresolved_terms_csv).resolve() if args.unresolved_terms_csv else None
    out_vocab = (root / args.output_vocab_csv).resolve()
    out_review = (root / args.output_review_csv).resolve()

    table: Dict[str, Dict[str, Set[str]]] = {}

    # 1) Alias map (small, high-precision deterministic mappings)
    if alias_path.exists():
        for alias, canonical in load_alias_map(alias_path).items():
            _add_entry(table, canonical_raw=str(canonical), synonyms_raw=[str(alias), str(canonical)], source_tag="alias")

    # 2) Public-source vocabulary terms (e.g. RxNorm-derived)
    if rxnorm_path.exists():
        for t in _read_single_term_csv(rxnorm_path, term_col="term"):
            _add_entry(table, canonical_raw=t, synonyms_raw=[t], source_tag="rxnorm_public")

    # 3) Optional adjudicated canonical labels
    if adjud_path is not None and adjud_path.exists():
        for t in _read_adjudicated_labels(adjud_path):
            _add_entry(table, canonical_raw=t, synonyms_raw=[t], source_tag="adjudicated")

    # Write canonical vocabulary table.
    out_vocab.parent.mkdir(parents=True, exist_ok=True)
    with out_vocab.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["canonical_label", "synonyms", "n_synonyms", "source_tags"])
        for canonical in sorted(table.keys()):
            syns = sorted(table[canonical]["synonyms"])
            srcs = sorted(table[canonical]["sources"])
            w.writerow([canonical, "|".join(syns), len(syns), "|".join(srcs)])

    # Build optional unresolved review queue (bounded human review target).
    review_rows: List[Tuple[str, int, str]] = []
    known_synonyms = set()
    for c in table.values():
        known_synonyms.update(c["synonyms"])

    if unresolved_path is not None and unresolved_path.exists():
        unresolved = _read_unresolved_terms(
            unresolved_path,
            term_col=args.unresolved_term_col,
            count_col=args.unresolved_count_col,
            min_freq=max(int(args.min_unresolved_freq), 1),
        )
        for term_raw, count in unresolved:
            tn = normalize_drug_text(term_raw)
            if not tn:
                continue
            if tn in known_synonyms:
                continue
            review_rows.append((tn, count, "pending_review"))

    out_review.parent.mkdir(parents=True, exist_ok=True)
    with out_review.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["term_norm", "mention_count", "review_status"])
        for term_norm, count, status in review_rows:
            w.writerow([term_norm, count, status])

    n_vocab = len(table)
    n_review = len(review_rows)
    print(f"Saved canonical vocab: {out_vocab} (rows={n_vocab:,})")
    print(f"Saved review queue:  {out_review} (rows={n_review:,})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
