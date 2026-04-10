#!/usr/bin/env python3
"""
Primary Step 4: evaluate extraction against adjudicated note-grounded truth.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List

import pandas as pd

from rq1_adjudication_utils import candidate_density_bin, grouped_status, note_length_bin, write_run_summary


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Evaluate extraction against adjudicated note truth.")
    p.add_argument(
        "--joined-mentions-csv",
        default="episode_extraction_results/rq1/adjudicated/rq1_extraction_vs_truth_mentions.csv",
        help="Output from run_join_adjudication_labels.py",
    )
    p.add_argument(
        "--visit-manifest-csv",
        default="episode_notes/manifests/adjudication_subset_manifest.csv",
        help="Visit manifest used to enrich slice metrics.",
    )
    p.add_argument(
        "--note-manifest-csv",
        default="episode_notes/manifests/adjudication_note_manifest.csv",
        help="Note manifest used to enrich slice metrics.",
    )
    p.add_argument(
        "--output-dir",
        default="episode_extraction_results/rq1/note_truth_eval",
        help="Output directory.",
    )
    return p.parse_args()


def _safe_div(num: float, den: float) -> float:
    return float(num) / float(den) if den else 0.0


def _slice_metrics(df: pd.DataFrame, slice_col: str) -> pd.DataFrame:
    rows: List[Dict] = []
    for value, sub in df.groupby(slice_col, dropna=False):
        tp = int((sub["alignment_status"] == "matched").sum())
        fp = int((sub["alignment_status"] == "false_positive").sum())
        fn = int((sub["alignment_status"] == "false_negative").sum())
        prec = _safe_div(tp, tp + fp)
        rec = _safe_div(tp, tp + fn)
        f1 = _safe_div(2 * prec * rec, prec + rec)
        rows.append(
            {
                "slice_name": slice_col,
                "slice_value": value,
                "tp": tp,
                "fp": fp,
                "fn": fn,
                "precision": round(prec, 6),
                "recall": round(rec, 6),
                "f1": round(f1, 6),
            }
        )
    return pd.DataFrame(rows)


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    join_path = (root / args.joined_mentions_csv).resolve()
    visit_path = (root / args.visit_manifest_csv).resolve()
    note_path = (root / args.note_manifest_csv).resolve()
    out_dir = (root / args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    if not join_path.exists():
        raise FileNotFoundError(f"Missing joined mentions CSV: {join_path}")

    joined = pd.read_csv(join_path).fillna("")
    if "alignment_status" not in joined.columns:
        raise ValueError("Joined mentions CSV missing alignment_status")

    joined["person_id"] = joined["person_id"].astype(str).str.strip()
    joined["visit_id"] = joined["visit_id"].astype(str).str.strip()
    joined["note_id"] = joined["note_id"].astype(str).str.strip()
    joined["mention_status"] = joined["mention_status"].astype(str).str.strip().str.lower()
    joined["grouped_status"] = joined["mention_status"].map(grouped_status)

    if visit_path.exists():
        visits = pd.read_csv(visit_path).fillna("")
        v_cols = {"person_id", "visit_occurrence_id", "candidate_span_count", "eligible_note_count", "note_type_mode", "service"}
        keep = [c for c in v_cols if c in visits.columns]
        visits = visits[keep].copy()
        visits["person_id"] = visits["person_id"].astype(str).str.strip()
        visits["visit_occurrence_id"] = visits["visit_occurrence_id"].astype(str).str.strip()
        visits = visits.rename(columns={"visit_occurrence_id": "visit_id"})
        joined = joined.merge(visits, on=["person_id", "visit_id"], how="left")

    if note_path.exists():
        notes = pd.read_csv(note_path).fillna("")
        n_cols = {"person_id", "visit_occurrence_id", "note_id", "note_title_norm", "note_len"}
        keep = [c for c in n_cols if c in notes.columns]
        notes = notes[keep].copy()
        notes["person_id"] = notes["person_id"].astype(str).str.strip()
        notes["visit_occurrence_id"] = notes["visit_occurrence_id"].astype(str).str.strip()
        notes["note_id"] = notes["note_id"].astype(str).str.strip()
        notes = notes.rename(columns={"visit_occurrence_id": "visit_id"})
        joined = joined.merge(notes, on=["person_id", "visit_id", "note_id"], how="left")

    joined["note_length_bin"] = joined.get("note_len", "").map(note_length_bin)
    joined["candidate_density_bin"] = joined.get("candidate_span_count", "").map(candidate_density_bin)
    joined["multi_note_visit_indicator"] = joined.get("eligible_note_count", 0).astype(float).gt(1).map({True: "multi_note", False: "single_note"})

    tp = int((joined["alignment_status"] == "matched").sum())
    fp = int((joined["alignment_status"] == "false_positive").sum())
    fn = int((joined["alignment_status"] == "false_negative").sum())
    precision = _safe_div(tp, tp + fp)
    recall = _safe_div(tp, tp + fn)
    f1 = _safe_div(2 * precision * recall, precision + recall)

    matched = joined[joined["alignment_status"] == "matched"].copy()
    matched["seed_status_hint"] = matched["seed_treatment_action"].astype(str).str.strip().str.lower()
    exact_status_accuracy = _safe_div((matched["seed_status_hint"] == matched["mention_status"]).sum(), len(matched))
    grouped_status_accuracy = _safe_div(
        matched["seed_status_hint"].map(grouped_status).eq(matched["grouped_status"]).sum(),
        len(matched),
    )

    slice_frames = []
    for col in [
        "mention_status",
        "note_title_norm",
        "service",
        "note_length_bin",
        "candidate_density_bin",
        "multi_note_visit_indicator",
    ]:
        if col in joined.columns:
            slice_frames.append(_slice_metrics(joined, col))
    slice_df = pd.concat(slice_frames, ignore_index=True) if slice_frames else pd.DataFrame()

    fp_df = joined[joined["alignment_status"] == "false_positive"].copy()
    fn_df = joined[joined["alignment_status"] == "false_negative"].copy()
    status_disagreements = matched[matched["seed_status_hint"] != matched["mention_status"]].copy()
    ambiguous_df = joined[joined["audit_flag"].astype(str).str.strip() != ""].copy()

    confusion = (
        matched.groupby(["mention_status", "seed_status_hint"], as_index=False)
        .size()
        .rename(columns={"size": "count"})
    )

    metrics_path = out_dir / "rq1_step4_note_truth_metrics.json"
    slices_path = out_dir / "rq1_step4_note_truth_slice_metrics.csv"
    fp_path = out_dir / "rq1_step4_false_positives.csv"
    fn_path = out_dir / "rq1_step4_false_negatives.csv"
    status_path = out_dir / "rq1_step4_status_disagreements.csv"
    ambiguous_path = out_dir / "rq1_step4_ambiguous_join_cases.csv"
    confusion_path = out_dir / "rq1_step4_status_confusion.csv"
    summary_path = out_dir / "rq1_step4_note_truth_summary.json"

    metrics = {
        "mention_level": {
            "tp": tp,
            "fp": fp,
            "fn": fn,
            "precision": round(precision, 6),
            "recall": round(recall, 6),
            "f1": round(f1, 6),
        },
        "status_metrics": {
            "n_matched": int(len(matched)),
            "exact_status_accuracy": round(exact_status_accuracy, 6),
            "grouped_status_accuracy": round(grouped_status_accuracy, 6),
        },
    }
    metrics_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    if len(slice_df):
        slice_df.to_csv(slices_path, index=False)
    fp_df.to_csv(fp_path, index=False)
    fn_df.to_csv(fn_path, index=False)
    status_disagreements.to_csv(status_path, index=False)
    ambiguous_df.to_csv(ambiguous_path, index=False)
    confusion.to_csv(confusion_path, index=False)

    write_run_summary(
        summary_path,
        {
            "inputs": {
                "joined_mentions_csv": str(join_path),
                "visit_manifest_csv": str(visit_path) if visit_path.exists() else None,
                "note_manifest_csv": str(note_path) if note_path.exists() else None,
            },
            "metrics": metrics,
            "outputs": {
                "metrics_json": str(metrics_path),
                "slice_metrics_csv": str(slices_path) if len(slice_df) else None,
                "false_positives_csv": str(fp_path),
                "false_negatives_csv": str(fn_path),
                "status_disagreements_csv": str(status_path),
                "ambiguous_join_cases_csv": str(ambiguous_path),
                "status_confusion_csv": str(confusion_path),
            },
        },
    )

    print(f"Saved note-truth metrics: {metrics_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
