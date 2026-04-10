#!/usr/bin/env python3
"""
Build adjudicated drug-label template CSV for mentor/human review.

This script creates a review-ready table from Stage-2 extracted records restricted
to the adjudication subset visits, so adjudication starts from concrete mention
rows rather than building labels from scratch.
"""

from __future__ import annotations

import argparse
import ast
import csv
import json
from pathlib import Path
from typing import Dict, Iterable, List, Set, Tuple

import pandas as pd


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


def _to_status_hint(treatment_action: str, discontinuation_reason: str) -> str:
    ta = str(treatment_action or "").strip().lower()
    dr = str(discontinuation_reason or "").strip().lower()
    if ta in {"start"}:
        return "newly_started"
    if ta in {"hold"}:
        return "held_paused"
    if ta in {"stop"}:
        return "discontinued_stopped"
    if ta in {"dose_change"}:
        return "active_current"
    if dr in {"completion", "toxicity", "progression", "cost", "logistics", "patient_preference"}:
        return "discontinued_stopped"
    return "active_current"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build adjudicated labels template for RQ1 drug mentions.")
    p.add_argument(
        "--adjudication-subset-csv",
        default="episode_notes/manifests/adjudication_subset_manifest.csv",
        help="Adjudication subset manifest (must include person_id and visit_occurrence_id).",
    )
    p.add_argument(
        "--stage2-csv",
        default="episode_extraction_results/archive_stage2/extracted_treatment_data_episode_cleaned.csv",
        help="Stage-2 extracted dataset with drugs list column.",
    )
    p.add_argument(
        "--min-certainty",
        choices=["", "low", "high"],
        default="",
        help="Optional certainty filter for stage2 rows.",
    )
    p.add_argument(
        "--output-csv",
        default="episode_extraction_results/rq1/adjudication/rq1_adjudicated_drug_labels_template.csv",
        help="Output adjudication template CSV.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[2]
    subset_path = (root / args.adjudication_subset_csv).resolve()
    stage2_path = (root / args.stage2_csv).resolve()
    out_path = (root / args.output_csv).resolve()

    if not subset_path.exists():
        raise FileNotFoundError(f"Missing adjudication subset CSV: {subset_path}")
    if not stage2_path.exists():
        raise FileNotFoundError(f"Missing stage2 CSV: {stage2_path}")

    subset = pd.read_csv(subset_path, usecols=lambda c: c in {"person_id", "visit_occurrence_id"})
    if "person_id" not in subset.columns or "visit_occurrence_id" not in subset.columns:
        raise ValueError("adjudication subset CSV must include person_id and visit_occurrence_id")

    target_keys = {
        (str(r.person_id).strip(), str(r.visit_occurrence_id).strip())
        for r in subset.itertuples(index=False)
    }

    stage2 = pd.read_csv(
        stage2_path,
        usecols=lambda c: c
        in {
            "person_id",
            "visit_id",
            "note_id",
            "drugs",
            "certainty",
            "category",
            "treatment_action",
            "discontinuation_reason",
            "span_text",
        },
    )

    need = {"person_id", "visit_id", "note_id", "drugs"}
    if not need.issubset(stage2.columns):
        missing = sorted(list(need - set(stage2.columns)))
        raise ValueError(f"stage2 CSV missing required columns: {missing}")

    if args.min_certainty:
        stage2 = stage2[stage2.get("certainty", "").astype(str).str.lower() == args.min_certainty]

    rows = []
    seen: Set[Tuple[str, str, str, str]] = set()
    for r in stage2.itertuples(index=False):
        pid = str(getattr(r, "person_id", "")).strip()
        vid = str(getattr(r, "visit_id", "")).strip()
        if (pid, vid) not in target_keys:
            continue
        note_id = str(getattr(r, "note_id", "")).strip()
        drugs = _parse_list_cell(getattr(r, "drugs", []))
        if not drugs:
            continue

        span = str(getattr(r, "span_text", "") or "").strip()
        span_excerpt = span[:500]
        certainty = str(getattr(r, "certainty", "") or "").strip().lower()
        category = str(getattr(r, "category", "") or "").strip().lower()
        treatment_action = str(getattr(r, "treatment_action", "") or "").strip().lower()
        discontinuation_reason = str(getattr(r, "discontinuation_reason", "") or "").strip().lower()

        for drug in drugs:
            raw_mention = str(drug).strip()
            if not raw_mention:
                continue
            key = (pid, vid, note_id, raw_mention.lower())
            if key in seen:
                continue
            seen.add(key)

            rows.append(
                {
                    "person_id": pid,
                    "visit_id": vid,
                    "note_id": note_id,
                    "raw_mention_text": raw_mention,
                    "adjudicated_drug_label": "",
                    "mention_status": "",
                    "compare_to_structured_ehr": "",
                    "review_flag": "",
                    "suggested_status_hint": _to_status_hint(treatment_action, discontinuation_reason),
                    "source_certainty": certainty,
                    "source_category": category,
                    "source_treatment_action": treatment_action,
                    "source_discontinuation_reason": discontinuation_reason,
                    "span_text_excerpt": span_excerpt,
                }
            )

    rows.sort(key=lambda x: (x["person_id"], x["visit_id"], x["note_id"], x["raw_mention_text"]))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        fieldnames = [
            "person_id",
            "visit_id",
            "note_id",
            "raw_mention_text",
            "adjudicated_drug_label",
            "mention_status",
            "compare_to_structured_ehr",
            "review_flag",
            "suggested_status_hint",
            "source_certainty",
            "source_category",
            "source_treatment_action",
            "source_discontinuation_reason",
            "span_text_excerpt",
        ]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    print(f"Saved adjudication template: {out_path} (rows={len(rows):,})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
