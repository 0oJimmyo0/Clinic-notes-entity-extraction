#!/usr/bin/env python3
"""
Build reviewed adjudication CSV from note-level LLM+human-reviewed medications JSONL.

Purpose:
- Convert note-level reviewed medication entities into the mention-level schema expected by
  run_join_adjudication_labels.py.
- Preserve deterministic matching to existing adjudication packet seeds when possible.

Notes:
- medications.jsonl is note-level truth and does not include mention status/comparability labels.
- This bridge sets conservative defaults (mention_status=unclear, compare_to_structured_ehr=uncertain).
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from rq1_adjudication_utils import stable_id, write_run_summary
from rq1_drug_linking import (
    build_canonical_drug_universe,
    canonicalize_drug,
    load_alias_map,
    normalize_drug_text,
)


def _parse_json_list_cell(x: str) -> List[str]:
    try:
        vals = json.loads(str(x))
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
    p = argparse.ArgumentParser(description="Build reviewed adjudication CSV from medications JSONL.")
    p.add_argument(
        "--packets-mentions-csv",
        default="../episode_extraction_results/clinic_only/rq1/adjudication_packets/adjudication_packets_mentions.csv",
        help="Adjudication packets mention CSV.",
    )
    p.add_argument(
        "--medications-jsonl",
        default="struct_data/medications.jsonl",
        help="LLM+human reviewed note-level medications JSONL.",
    )
    p.add_argument(
        "--alias-artifact",
        default="lexicons/rq1_drug_aliases.csv",
        help="Path A alias artifact used for canonicalization support.",
    )
    p.add_argument(
        "--canonical-vocab-path",
        default="lexicons/rq1_drug_canonical_vocab.csv",
        help="Canonical vocabulary for canonical-label projection.",
    )
    p.add_argument(
        "--default-mention-status",
        default="unclear",
        choices=[
            "active_current",
            "newly_started_or_prescribed",
            "planned_or_considering",
            "discontinued_or_stopped",
            "held_or_paused",
            "historical_prior",
            "reference_only_or_discussion_only",
            "unclear",
        ],
    )
    p.add_argument(
        "--default-compare-to-ehr",
        default="uncertain",
        choices=["yes", "no", "uncertain"],
    )
    p.add_argument(
        "--restrict-to-packet-notes",
        action="store_true",
        default=True,
        help="Restrict reviewed rows to notes present in adjudication packet mentions (recommended).",
    )
    p.add_argument(
        "--allow-notes-outside-packets",
        action="store_true",
        help="Include medications JSONL notes without packet context (creates unmatched truth rows).",
    )
    p.add_argument(
        "--output-csv",
        default="../episode_extraction_results/clinic_only/rq1/adjudicated/reviewed_adjudication_from_medications_jsonl.csv",
        help="Output reviewed adjudication CSV compatible with run_join_adjudication_labels.py.",
    )
    p.add_argument(
        "--summary-json",
        default="../episode_extraction_results/clinic_only/rq1/adjudicated/reviewed_adjudication_from_medications_jsonl_summary.json",
        help="Output summary JSON.",
    )
    return p.parse_args()


def _token_jaccard(a: str, b: str) -> float:
    sa = set(a.split())
    sb = set(b.split())
    if not sa and not sb:
        return 1.0
    if not sa or not sb:
        return 0.0
    return float(len(sa & sb)) / float(len(sa | sb))


def _load_medications_jsonl(path: Path) -> Dict[str, List[Tuple[str, str]]]:
    """
    Returns note_id -> list of tuples (raw_term, canonicalish_term).
    canonicalish_term is initially normalize_drug_text(raw_term).
    """
    out: Dict[str, List[Tuple[str, str]]] = {}
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            note_id = str(obj.get("note_id", "")).strip()
            if not note_id:
                continue
            meds = obj.get("medications", [])
            if not isinstance(meds, list):
                continue

            dedup = {}
            for m in meds:
                raw = str(m).strip()
                if not raw:
                    continue
                norm = normalize_drug_text(raw)
                if not norm:
                    continue
                # Keep first raw rendering for each normalized mention.
                dedup.setdefault(norm, raw)

            out[note_id] = [(raw, norm) for norm, raw in dedup.items()]
    return out


def _build_seed_index(seed_df: pd.DataFrame) -> Dict[str, List[Dict]]:
    by_note: Dict[str, List[Dict]] = {}
    for row in seed_df.itertuples(index=False):
        note_id = str(getattr(row, "note_id", "") or "").strip()
        if not note_id:
            continue
        raw = str(getattr(row, "raw_mention_text", "") or "").strip()
        norm = normalize_drug_text(raw)
        by_note.setdefault(note_id, []).append(
            {
                "adjudication_unit_id": str(getattr(row, "adjudication_unit_id", "") or "").strip(),
                "person_id": str(getattr(row, "person_id", "") or "").strip(),
                "visit_id": str(getattr(row, "visit_id", "") or "").strip(),
                "note_id": note_id,
                "span_id_or_local_reference": str(getattr(row, "span_id_or_local_reference", "") or "").strip(),
                "raw_mention_text": raw,
                "raw_mention_norm": norm,
                "seed_extracted_drugs": [normalize_drug_text(x) for x in _parse_json_list_cell(getattr(row, "seed_extracted_drugs_json", "[]"))],
                "context_text": str(getattr(row, "context_text", "") or ""),
            }
        )
    return by_note


def _pick_best_seed_candidate(
    truth_norm: str,
    truth_canonical: str,
    candidates: List[Dict],
    used_seed_units: set,
    alias_map: Dict[str, str],
) -> Dict | None:
    best = None
    best_score = -1.0

    for c in candidates:
        if c["adjudication_unit_id"] in used_seed_units:
            continue
        m_norm = c["raw_mention_norm"]
        seed_drugs = set(x for x in c.get("seed_extracted_drugs", []) if x)
        if not m_norm:
            m_norm = ""

        score = 0.0
        if m_norm == truth_norm:
            score += 5.0

        m_canon = canonicalize_drug(m_norm, alias_map)
        if m_canon and m_canon == truth_canonical:
            score += 4.0

        if truth_norm in seed_drugs:
            score += 6.0
        if truth_canonical in seed_drugs:
            score += 5.0

        if len(truth_norm) >= 4 and truth_norm in m_norm:
            score += 1.0
        if len(m_norm) >= 4 and m_norm in truth_norm:
            score += 1.0

        if m_norm:
            score += _token_jaccard(m_norm, truth_norm)
        if seed_drugs:
            score += max(_token_jaccard(x, truth_norm) for x in seed_drugs)

        if score > best_score:
            best = c
            best_score = score

    if best_score >= 1.0:
        return best

    # Note-level fallback: if no lexical/seed-drug signal is available, align within-note
    # to an unused seed row so join/evaluation can proceed for note-level reviewed truth.
    for c in candidates:
        if c["adjudication_unit_id"] in used_seed_units:
            continue
        return c
    return None


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]

    packets_path = (root / args.packets_mentions_csv).resolve()
    meds_path = (root / args.medications_jsonl).resolve()
    alias_path = (root / args.alias_artifact).resolve()
    vocab_path = (root / args.canonical_vocab_path).resolve() if args.canonical_vocab_path else None
    out_path = (root / args.output_csv).resolve()
    summary_path = (root / args.summary_json).resolve()

    if not packets_path.exists():
        raise FileNotFoundError(f"Missing packets mentions CSV: {packets_path}")
    if not meds_path.exists():
        raise FileNotFoundError(f"Missing medications JSONL: {meds_path}")

    seed_df = pd.read_csv(packets_path).fillna("")
    for col in ["adjudication_unit_id", "person_id", "visit_id", "note_id", "span_id_or_local_reference", "raw_mention_text", "context_text"]:
        if col not in seed_df.columns:
            seed_df[col] = ""

    meds_by_note = _load_medications_jsonl(meds_path)
    alias_map = load_alias_map(alias_path) if alias_path.exists() else {}
    universe = build_canonical_drug_universe(
        alias_map=alias_map,
        canonical_vocab_path=vocab_path if vocab_path and vocab_path.exists() else None,
    )

    seed_by_note = _build_seed_index(seed_df)
    packet_note_ids = set(seed_by_note.keys())

    if args.restrict_to_packet_notes and not args.allow_notes_outside_packets:
        meds_by_note = {k: v for k, v in meds_by_note.items() if k in packet_note_ids}

    reviewed_rows: List[Dict] = []
    used_seed_units = set()
    matched_truth_rows = 0
    unmatched_truth_rows = 0
    notes_without_seed_context = 0

    for note_id, truth_terms in meds_by_note.items():
        candidates = seed_by_note.get(note_id, [])
        fallback_person = candidates[0]["person_id"] if candidates else ""
        fallback_visit = candidates[0]["visit_id"] if candidates else ""
        if not candidates:
            notes_without_seed_context += 1

        for idx, (truth_raw, truth_norm) in enumerate(truth_terms, start=1):
            if not truth_norm:
                continue
            truth_patha = canonicalize_drug(truth_norm, alias_map)
            truth_canonical = universe.synonym_to_canonical.get(truth_patha, truth_patha)

            best = _pick_best_seed_candidate(
                truth_norm=truth_norm,
                truth_canonical=truth_canonical,
                candidates=candidates,
                used_seed_units=used_seed_units,
                alias_map=alias_map,
            )

            if best is not None:
                used_seed_units.add(best["adjudication_unit_id"])
                reviewed_rows.append(
                    {
                        "adjudication_unit_id": best["adjudication_unit_id"],
                        "person_id": best["person_id"],
                        "visit_id": best["visit_id"],
                        "note_id": best["note_id"],
                        "span_id_or_local_reference": best["span_id_or_local_reference"],
                        "raw_mention_text": best["raw_mention_text"] or truth_raw,
                        "context_text": best["context_text"],
                        "adjudicated_canonical_label": truth_canonical,
                        "mention_status": args.default_mention_status,
                        "compare_to_structured_ehr": args.default_compare_to_ehr,
                        "reviewer_notes": "llm_plus_human_review_medications_jsonl_matched_to_seed",
                    }
                )
                matched_truth_rows += 1
                continue

            synthetic_span = f"jsonl_truth_{idx}"
            synthetic_unit = stable_id(note_id, synthetic_span, truth_canonical, prefix="adj_")
            reviewed_rows.append(
                {
                    "adjudication_unit_id": synthetic_unit,
                    "person_id": fallback_person,
                    "visit_id": fallback_visit,
                    "note_id": note_id,
                    "span_id_or_local_reference": synthetic_span,
                    "raw_mention_text": truth_raw,
                    "context_text": "",
                    "adjudicated_canonical_label": truth_canonical,
                    "mention_status": args.default_mention_status,
                    "compare_to_structured_ehr": args.default_compare_to_ehr,
                    "reviewer_notes": "llm_plus_human_review_medications_jsonl_unmatched_seed_added_as_truth",
                }
            )
            unmatched_truth_rows += 1

    reviewed_df = pd.DataFrame(reviewed_rows)
    if len(reviewed_df):
        reviewed_df = reviewed_df.drop_duplicates(
            subset=[
                "note_id",
                "adjudicated_canonical_label",
                "span_id_or_local_reference",
                "raw_mention_text",
            ],
            keep="first",
        )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    reviewed_df.to_csv(out_path, index=False)

    write_run_summary(
        summary_path,
        {
            "inputs": {
                "packets_mentions_csv": str(packets_path),
                "medications_jsonl": str(meds_path),
                "alias_artifact": str(alias_path) if alias_path.exists() else None,
                "canonical_vocab_path": str(vocab_path) if vocab_path and vocab_path.exists() else None,
            },
            "counts": {
                "seed_packet_rows": int(len(seed_df)),
                "jsonl_notes": int(len(meds_by_note)),
                "reviewed_rows_written": int(len(reviewed_df)),
                "matched_truth_rows": int(matched_truth_rows),
                "unmatched_truth_rows": int(unmatched_truth_rows),
                "notes_without_seed_context": int(notes_without_seed_context),
            },
            "defaults": {
                "mention_status": args.default_mention_status,
                "compare_to_structured_ehr": args.default_compare_to_ehr,
            },
            "outputs": {
                "reviewed_adjudication_csv": str(out_path),
            },
        },
    )

    print(f"Saved reviewed adjudication CSV: {out_path}")
    print(f"Rows: {len(reviewed_df):,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
