#!/usr/bin/env python3
"""
Build reviewer-ready adjudication packets for treatment-context medication mentions.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from rq1_adjudication_utils import (
    build_span_local_id,
    load_note_subset,
    parse_list_cell,
    stable_id,
    write_run_summary,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build adjudication mention packets.")
    p.add_argument(
        "--adjudication-subset-csv",
        default="episode_notes/manifests/adjudication_subset_manifest.csv",
        help="Visit-level adjudication subset manifest from run_select_note_corpus.py",
    )
    p.add_argument(
        "--candidate-csv",
        default="episode_extraction_results/archive_candidates/all_candidates_combined.csv",
        help="Stage 1 candidate spans CSV.",
    )
    p.add_argument(
        "--stage2-csv",
        default="episode_extraction_results/archive_stage2/extracted_treatment_data_episode_cleaned.csv",
        help="Stage 2 extracted span-level CSV used as seed suggestions.",
    )
    p.add_argument(
        "--notes-parquet",
        default="episode_notes/subcohort_patient_complete/notes.parquet",
        help="Optional notes parquet with full note text; if missing, raw chunks will be scanned.",
    )
    p.add_argument("--notes-dir", default="episode_notes", help="Fallback chunk directory for note text lookup.")
    p.add_argument("--notes-glob", default="episode_notes_chunk*.parquet", help="Fallback chunk glob.")
    p.add_argument(
        "--output-dir",
        default="episode_extraction_results/rq1/adjudication_packets",
        help="Output directory.",
    )
    p.add_argument(
        "--write-jsonl",
        action="store_true",
        help="Also write JSONL rows for optional LLM-assisted draft labeling.",
    )
    return p.parse_args()


def _load_subset_keys(path: Path) -> Tuple[set[Tuple[str, str]], pd.DataFrame]:
    subset = pd.read_csv(path)
    required = {"person_id", "visit_occurrence_id"}
    if not required.issubset(subset.columns):
        raise ValueError(f"Subset manifest missing required columns: {sorted(required - set(subset.columns))}")
    subset["person_id"] = subset["person_id"].astype(str).str.strip()
    subset["visit_occurrence_id"] = subset["visit_occurrence_id"].astype(str).str.strip()
    keys = {(r.person_id, r.visit_occurrence_id) for r in subset.itertuples(index=False)}
    return keys, subset


def _stage2_seed_lookup(stage2: pd.DataFrame) -> Dict[Tuple[str, str, str, str, str], Dict]:
    out: Dict[Tuple[str, str, str, str, str], Dict] = {}
    stage2 = stage2.copy()
    for col in ["person_id", "visit_id", "note_id", "category", "span_text"]:
        if col not in stage2.columns:
            raise ValueError(f"Stage 2 CSV missing required column: {col}")
    stage2["person_id"] = stage2["person_id"].astype(str).str.strip()
    stage2["visit_id"] = stage2["visit_id"].astype(str).str.strip()
    stage2["note_id"] = stage2["note_id"].astype(str).str.strip()
    stage2["category"] = stage2["category"].astype(str).str.strip()
    stage2["span_text"] = stage2["span_text"].astype(str).str.strip()
    for row in stage2.itertuples(index=False):
        drugs = parse_list_cell(getattr(row, "drugs", []))
        key = (
            str(getattr(row, "person_id", "")).strip(),
            str(getattr(row, "visit_id", "")).strip(),
            str(getattr(row, "note_id", "")).strip(),
            str(getattr(row, "category", "")).strip(),
            str(getattr(row, "span_text", "")).strip(),
        )
        bucket = out.setdefault(
            key,
            {
                "seed_drugs": [],
                "seed_treatment_action": str(getattr(row, "treatment_action", "") or "").strip().lower(),
                "seed_discontinuation_reason": str(getattr(row, "discontinuation_reason", "") or "").strip().lower(),
                "seed_certainty": str(getattr(row, "certainty", "") or "").strip().lower(),
            },
        )
        for d in drugs:
            if d not in bucket["seed_drugs"]:
                bucket["seed_drugs"].append(d)
    return out


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[2]

    subset_path = (root / args.adjudication_subset_csv).resolve()
    candidate_path = (root / args.candidate_csv).resolve()
    stage2_path = (root / args.stage2_csv).resolve()
    notes_parquet = (root / args.notes_parquet).resolve()
    notes_dir = (root / args.notes_dir).resolve()
    out_dir = (root / args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    if not subset_path.exists():
        raise FileNotFoundError(f"Missing adjudication subset manifest: {subset_path}")
    if not candidate_path.exists():
        raise FileNotFoundError(f"Missing candidate CSV: {candidate_path}")
    if not stage2_path.exists():
        raise FileNotFoundError(f"Missing Stage 2 CSV: {stage2_path}")

    subset_keys, subset_df = _load_subset_keys(subset_path)

    cand = pd.read_csv(candidate_path)
    required_cand = {"person_id", "visit_id", "note_id", "span_text", "category"}
    if not required_cand.issubset(cand.columns):
        raise ValueError(f"Candidate CSV missing required columns: {sorted(required_cand - set(cand.columns))}")
    cand["person_id"] = cand["person_id"].astype(str).str.strip()
    cand["visit_id"] = cand["visit_id"].astype(str).str.strip()
    cand["note_id"] = cand["note_id"].astype(str).str.strip()
    cand = cand[cand.apply(lambda r: (r["person_id"], r["visit_id"]) in subset_keys, axis=1)].copy()
    cand["span_id_or_local_reference"] = cand.apply(lambda r: build_span_local_id(r.to_dict()), axis=1)

    stage2 = pd.read_csv(stage2_path)
    stage2_lookup = _stage2_seed_lookup(stage2)

    note_ids = sorted(cand["note_id"].astype(str).unique().tolist())
    note_df = load_note_subset(
        note_ids=note_ids,
        notes_parquet=notes_parquet if notes_parquet.exists() else None,
        notes_dir=None if notes_parquet.exists() else notes_dir,
        glob_pattern=args.notes_glob,
    )
    note_df["note_id"] = note_df["note_id"].astype(str).str.strip()
    note_lookup = {str(r.note_id): r for r in note_df.itertuples(index=False)}

    mention_rows: List[Dict] = []
    note_rows: List[Dict] = []

    grouped_note = cand.groupby(["person_id", "visit_id", "note_id"], sort=False)
    for (pid, vid, nid), g in grouped_note:
        note_meta = note_lookup.get(str(nid))
        note_text = str(getattr(note_meta, "note_text", "") or "")
        note_rows.append(
            {
                "packet_note_id": stable_id(pid, vid, nid, prefix="notepkt_"),
                "person_id": pid,
                "visit_id": vid,
                "note_id": nid,
                "note_date": str(getattr(note_meta, "note_date", g["note_date"].iloc[0] if "note_date" in g.columns else "")),
                "note_title": str(getattr(note_meta, "note_title", g["note_title"].iloc[0] if "note_title" in g.columns else "")),
                "candidate_span_count": int(len(g)),
                "candidate_categories_json": json.dumps(sorted(set(g["category"].astype(str).tolist()))),
                "candidate_span_examples_json": json.dumps(g["span_text"].astype(str).head(5).tolist()),
                "seed_note_text_excerpt": note_text[:2000],
            }
        )

        for row in g.itertuples(index=False):
            key = (str(pid), str(vid), str(nid), str(getattr(row, "category", "")).strip(), str(getattr(row, "span_text", "")).strip())
            seed = stage2_lookup.get(
                key,
                {
                    "seed_drugs": [],
                    "seed_treatment_action": "",
                    "seed_discontinuation_reason": "",
                    "seed_certainty": "",
                },
            )
            seed_drugs = seed["seed_drugs"] or []
            if not seed_drugs:
                mention_rows.append(
                    {
                        "adjudication_unit_id": stable_id(pid, vid, nid, row.span_id_or_local_reference, "no_seed", prefix="adj_"),
                        "person_id": pid,
                        "visit_id": vid,
                        "note_id": nid,
                        "span_id_or_local_reference": row.span_id_or_local_reference,
                        "raw_mention_text": "",
                        "context_text": str(getattr(row, "span_text", "") or ""),
                        "note_date": str(getattr(row, "note_date", "")),
                        "note_title": str(getattr(row, "note_title", "")),
                        "candidate_category": str(getattr(row, "category", "")),
                        "match_text": str(getattr(row, "match_text", "")),
                        "target_drug": str(getattr(row, "target_drug", "") or ""),
                        "seed_extracted_drugs_json": "[]",
                        "seed_treatment_action": seed["seed_treatment_action"],
                        "seed_discontinuation_reason": seed["seed_discontinuation_reason"],
                        "seed_certainty": seed["seed_certainty"],
                        "adjudicated_canonical_label": "",
                        "mention_status": "",
                        "compare_to_structured_ehr": "",
                        "reviewer_notes": "",
                    }
                )
                continue

            for i, drug in enumerate(seed_drugs):
                mention_rows.append(
                    {
                        "adjudication_unit_id": stable_id(pid, vid, nid, row.span_id_or_local_reference, drug, i, prefix="adj_"),
                        "person_id": pid,
                        "visit_id": vid,
                        "note_id": nid,
                        "span_id_or_local_reference": row.span_id_or_local_reference,
                        "raw_mention_text": drug,
                        "context_text": str(getattr(row, "span_text", "") or ""),
                        "note_date": str(getattr(row, "note_date", "")),
                        "note_title": str(getattr(row, "note_title", "")),
                        "candidate_category": str(getattr(row, "category", "")),
                        "match_text": str(getattr(row, "match_text", "")),
                        "target_drug": str(getattr(row, "target_drug", "") or ""),
                        "seed_extracted_drugs_json": json.dumps(seed_drugs),
                        "seed_treatment_action": seed["seed_treatment_action"],
                        "seed_discontinuation_reason": seed["seed_discontinuation_reason"],
                        "seed_certainty": seed["seed_certainty"],
                        "adjudicated_canonical_label": "",
                        "mention_status": "",
                        "compare_to_structured_ehr": "",
                        "reviewer_notes": "",
                    }
                )

    mention_df = pd.DataFrame(mention_rows)
    note_packet_df = pd.DataFrame(note_rows).drop_duplicates(subset=["packet_note_id"])

    mention_path = out_dir / "adjudication_packets_mentions.csv"
    note_path = out_dir / "adjudication_packets_notes.csv"
    summary_path = out_dir / "adjudication_packets_manifest.json"
    mention_df.to_csv(mention_path, index=False)
    note_packet_df.to_csv(note_path, index=False)

    if args.write_jsonl:
        jsonl_path = out_dir / "adjudication_packets_mentions.jsonl"
        with jsonl_path.open("w", encoding="utf-8") as f:
            for row in mention_df.to_dict(orient="records"):
                f.write(json.dumps(row) + "\n")

    write_run_summary(
        summary_path,
        {
            "inputs": {
                "adjudication_subset_csv": str(subset_path),
                "candidate_csv": str(candidate_path),
                "stage2_csv": str(stage2_path),
                "notes_parquet": str(notes_parquet) if notes_parquet.exists() else None,
                "notes_dir": str(notes_dir),
            },
            "counts": {
                "subset_visits": int(len(subset_df)),
                "candidate_rows_in_subset": int(len(cand)),
                "mention_packets": int(len(mention_df)),
                "note_packets": int(len(note_packet_df)),
                "notes_found": int(note_df["note_id"].nunique()) if len(note_df) else 0,
            },
            "outputs": {
                "adjudication_packets_mentions_csv": str(mention_path),
                "adjudication_packets_notes_csv": str(note_path),
            },
        },
    )

    print(f"Saved adjudication mention packets: {mention_path} (rows={len(mention_df):,})")
    print(f"Saved adjudication note packets: {note_path} (rows={len(note_packet_df):,})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
