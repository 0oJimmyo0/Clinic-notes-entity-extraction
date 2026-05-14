#!/usr/bin/env python3
"""
Quantify credibility of the LLM-bootstrapped, human-audited reference layer.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List

import pandas as pd

from rq1_adjudication_utils import write_run_summary
from rq1_bibm_utils import (
    action_cue,
    broad_drug_category,
    ingredient_level_label,
    load_brand_generic_map,
    safe_div,
    wilson_ci,
)
from rq1_drug_linking import normalize_drug_text


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Analyze audited reference credibility for BIBM-facing paper outputs.")
    p.add_argument(
        "--base-reviewed-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/adjudicated/reviewed_adjudication_from_medications_jsonl.csv",
        help="Original LLM-derived reviewed adjudication CSV before strict human patching.",
    )
    p.add_argument(
        "--final-reviewed-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/adjudicated/reviewed_adjudication_patched.csv",
        help="Final patched reviewed adjudication CSV after human audit.",
    )
    p.add_argument(
        "--audit-review-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/adjudicated/strict_pathb_review_queue_completed_final.csv",
        help="Completed human audit review queue with priority and manual taxonomy fields.",
    )
    p.add_argument(
        "--patch-summary-json",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/adjudicated/reviewed_adjudication_patch_summary_strict_final.json",
    )
    p.add_argument(
        "--packets-mentions-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/adjudication_packets/adjudication_packets_mentions.csv",
    )
    p.add_argument(
        "--note-manifest-csv",
        default="../episode_notes/manifests_clinic_like_20k_30k/adjudication_note_manifest.csv",
    )
    p.add_argument(
        "--alias-artifacts",
        default="../resources/manual/pathA_alias_map.json,../resources/lexicons/rq1_drug_aliases.json",
        help="Comma-separated alias artifacts for ingredient/generic collapsing.",
    )
    p.add_argument(
        "--output-dir",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/reference_audit",
    )
    return p.parse_args()


def _load_json(path: Path) -> Dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _normalize_text_col(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([""] * len(df), index=df.index, dtype="object")
    return df[col].fillna("").astype(str).str.strip()


def _taxonomy_label(row: pd.Series, brand_generic_map: Dict[str, str]) -> str:
    manual = str(row.get("error_taxonomy_manual", "")).strip()
    if manual:
        return manual

    review_action = str(row.get("review_action", "")).strip().lower()
    orig = normalize_drug_text(row.get("base_canonical_label", ""))
    final = normalize_drug_text(row.get("final_canonical_label", ""))
    if review_action == "drop_row":
        return "spurious_or_remove_row"
    if orig == final:
        return "confirmed_correct"
    if ingredient_level_label(orig, brand_generic_map) == ingredient_level_label(final, brand_generic_map):
        return "brand_generic_or_surface_variant"
    if broad_drug_category(orig) == broad_drug_category(final) and broad_drug_category(orig) != "unknown":
        return "same_category_wrong_canonical"
    if not orig and final:
        return "llm_missed_or_blank_label"
    return "canonical_relabel_other"


def _slice_accuracy(df: pd.DataFrame, slice_col: str) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    for value, sub in df.groupby(slice_col, dropna=False):
        correct = int(sub["base_canonical_correct"].sum())
        total = int(len(sub))
        low, high = wilson_ci(correct, total)
        rows.append(
            {
                "slice_name": slice_col,
                "slice_value": value,
                "n": total,
                "correct_n": correct,
                "accuracy": round(safe_div(correct, total), 6),
                "ci_low": round(low, 6),
                "ci_high": round(high, 6),
            }
        )
    return pd.DataFrame(rows).sort_values(["slice_name", "n"], ascending=[True, False])


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]

    base_path = (root / args.base_reviewed_csv).resolve()
    final_path = (root / args.final_reviewed_csv).resolve()
    audit_path = (root / args.audit_review_csv).resolve()
    patch_summary_path = (root / args.patch_summary_json).resolve()
    packets_path = (root / args.packets_mentions_csv).resolve()
    note_manifest_path = (root / args.note_manifest_csv).resolve()
    out_dir = (root / args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    alias_paths = [(root / x.strip()).resolve() for x in str(args.alias_artifacts).split(",") if x.strip()]
    brand_generic_map = load_brand_generic_map(alias_paths)

    base = pd.read_csv(base_path).fillna("")
    final = pd.read_csv(final_path).fillna("")
    audit = pd.read_csv(audit_path).fillna("")
    packets = pd.read_csv(packets_path).fillna("")
    note_manifest = pd.read_csv(note_manifest_path).fillna("") if note_manifest_path.exists() else pd.DataFrame()
    patch_summary = _load_json(patch_summary_path)

    for df in [base, final, audit, packets, note_manifest]:
        if "adjudication_unit_id" in df.columns:
            df["adjudication_unit_id"] = _normalize_text_col(df, "adjudication_unit_id")
    for df in [base, final, packets, note_manifest]:
        for col in ["person_id", "visit_id", "note_id"]:
            if col in df.columns:
                df[col] = _normalize_text_col(df, col)
        if "visit_occurrence_id" in df.columns:
            df["visit_occurrence_id"] = _normalize_text_col(df, "visit_occurrence_id")

    base_small = base[
        [
            "adjudication_unit_id",
            "person_id",
            "visit_id",
            "note_id",
            "raw_mention_text",
            "context_text",
            "adjudicated_canonical_label",
            "mention_status",
            "compare_to_structured_ehr",
            "reviewer_notes",
        ]
    ].rename(
        columns={
            "adjudicated_canonical_label": "base_canonical_label",
            "mention_status": "base_mention_status",
            "compare_to_structured_ehr": "base_compare_to_ehr",
            "reviewer_notes": "base_reviewer_notes",
        }
    )
    final_small = final[
        [
            "adjudication_unit_id",
            "person_id",
            "visit_id",
            "note_id",
            "raw_mention_text",
            "context_text",
            "adjudicated_canonical_label",
            "mention_status",
            "compare_to_structured_ehr",
            "reviewer_notes",
        ]
    ].rename(
        columns={
            "adjudicated_canonical_label": "final_canonical_label",
            "mention_status": "final_mention_status",
            "compare_to_structured_ehr": "final_compare_to_ehr",
            "reviewer_notes": "final_reviewer_notes",
        }
    )

    audited = audit.merge(
        base_small,
        on="adjudication_unit_id",
        how="left",
        suffixes=("", "_base"),
    ).merge(
        final_small,
        on="adjudication_unit_id",
        how="left",
        suffixes=("", "_final"),
    )
    for col in ["person_id", "visit_id", "note_id"]:
        if col in audited.columns:
            audited[col] = audited[col].fillna("").astype(str).str.strip()

    packet_cols = [
        "adjudication_unit_id",
        "note_title",
        "candidate_category",
        "seed_treatment_action",
        "seed_discontinuation_reason",
        "seed_certainty",
    ]
    packets_small = packets[[c for c in packet_cols if c in packets.columns]].copy()
    audited = audited.merge(packets_small, on="adjudication_unit_id", how="left")

    if len(note_manifest):
        note_small = note_manifest.rename(columns={"visit_occurrence_id": "visit_id"})[
            [c for c in ["person_id", "visit_id", "note_id", "note_title_norm", "note_len"] if c in note_manifest.rename(columns={"visit_occurrence_id": "visit_id"}).columns]
        ].copy()
        for col in ["person_id", "visit_id", "note_id"]:
            if col in note_small.columns:
                note_small[col] = note_small[col].fillna("").astype(str).str.strip()
        audited = audited.merge(note_small, on=["person_id", "visit_id", "note_id"], how="left")

    audited["note_title_final"] = audited.get("note_title", "").astype(str)
    audited.loc[audited["note_title_final"].astype(str).str.strip() == "", "note_title_final"] = audited.get("note_title_norm", "")
    audited["action_cue"] = audited.apply(
        lambda r: action_cue(
            seed_treatment_action=str(r.get("seed_treatment_action", "")),
            seed_discontinuation_reason=str(r.get("seed_discontinuation_reason", "")),
            context_text=str(r.get("context_text", "")),
        ),
        axis=1,
    )
    audited["base_canonical_norm"] = audited.get("base_canonical_label", "").map(normalize_drug_text)
    audited["final_canonical_norm"] = audited.get("final_canonical_label", "").map(normalize_drug_text)
    audited["base_canonical_correct"] = audited["base_canonical_norm"].eq(audited["final_canonical_norm"])
    audited["base_ingredient_norm"] = audited["base_canonical_norm"].map(lambda x: ingredient_level_label(x, brand_generic_map))
    audited["final_ingredient_norm"] = audited["final_canonical_norm"].map(lambda x: ingredient_level_label(x, brand_generic_map))
    audited["llm_error_taxonomy"] = audited.apply(lambda r: _taxonomy_label(r, brand_generic_map), axis=1)

    reviewed_kept = audited[audited["review_action"].astype(str).str.lower() == "keep"].copy()
    reviewed_errors = reviewed_kept[~reviewed_kept["base_canonical_correct"]].copy()
    reference_rows = int(patch_summary.get("counts", {}).get("n_base_rows", len(base)))

    overall_correct = int(reviewed_kept["base_canonical_correct"].sum())
    overall_total = int(len(reviewed_kept))
    ci_low, ci_high = wilson_ci(overall_correct, overall_total)

    summary_rows = [
        ("Reference mention rows", reference_rows, "mention"),
        ("Human-audited rows", overall_total, "mention"),
        ("Human-audit percentage", round(100.0 * safe_div(overall_total, max(reference_rows, 1)), 2), "% of reference rows"),
        ("Audit queue rows reviewed", int(len(audited)), "mention"),
        ("Audit queue drops", int((audited["review_action"].astype(str).str.lower() == "drop_row").sum()), "mention"),
        ("LLM canonical correct before human correction", overall_correct, "mention"),
        ("LLM canonical accuracy before human correction", round(safe_div(overall_correct, overall_total), 6), "fraction"),
        ("LLM canonical accuracy CI low", round(ci_low, 6), "wilson_95"),
        ("LLM canonical accuracy CI high", round(ci_high, 6), "wilson_95"),
    ]
    summary_df = pd.DataFrame(summary_rows, columns=["item", "value", "unit"])

    selection_summary = (
        audited.groupby(["review_priority", "current_error_bucket", "error_taxonomy_manual", "review_action"], dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values(["count", "review_priority"], ascending=[False, True])
    )

    slice_frames = []
    for col in ["note_title_final", "action_cue", "candidate_category"]:
        if col in reviewed_kept.columns:
            slice_frames.append(_slice_accuracy(reviewed_kept, col))
    slice_df = pd.concat(slice_frames, ignore_index=True) if slice_frames else pd.DataFrame()

    error_taxonomy = (
        reviewed_kept.groupby("llm_error_taxonomy", dropna=False)
        .agg(
            audited_rows=("adjudication_unit_id", "count"),
            incorrect_rows=("base_canonical_correct", lambda s: int((~s).sum())),
        )
        .reset_index()
        .sort_values(["incorrect_rows", "audited_rows"], ascending=[False, False])
    )
    error_taxonomy["incorrect_share_among_audited"] = error_taxonomy["incorrect_rows"].map(
        lambda x: round(safe_div(x, overall_total), 6)
    )

    examples = reviewed_errors[
        [
            "adjudication_unit_id",
            "person_id",
            "visit_id",
            "note_id",
            "note_title_final",
            "candidate_category",
            "action_cue",
            "raw_mention_text",
            "base_canonical_label",
            "final_canonical_label",
            "llm_error_taxonomy",
            "context_text",
        ]
    ].copy()
    examples["context_text"] = examples["context_text"].astype(str).str.slice(0, 300)

    out_files = {
        "summary_csv": out_dir / "rq1_reference_audit_summary.csv",
        "selection_csv": out_dir / "rq1_reference_audit_selection_summary.csv",
        "slice_csv": out_dir / "rq1_reference_audit_accuracy_by_slice.csv",
        "error_csv": out_dir / "rq1_reference_audit_error_taxonomy.csv",
        "examples_csv": out_dir / "rq1_reference_audit_examples.csv",
        "detailed_csv": out_dir / "rq1_reference_audit_detailed.csv",
        "summary_json": out_dir / "rq1_reference_audit_summary.json",
    }

    summary_df.to_csv(out_files["summary_csv"], index=False)
    selection_summary.to_csv(out_files["selection_csv"], index=False)
    if len(slice_df):
        slice_df.to_csv(out_files["slice_csv"], index=False)
    error_taxonomy.to_csv(out_files["error_csv"], index=False)
    examples.to_csv(out_files["examples_csv"], index=False)
    audited.to_csv(out_files["detailed_csv"], index=False)

    write_run_summary(
        out_files["summary_json"],
        {
            "inputs": {
                "base_reviewed_csv": str(base_path),
                "final_reviewed_csv": str(final_path),
                "audit_review_csv": str(audit_path),
                "packets_mentions_csv": str(packets_path),
            },
            "counts": {
                "reference_rows": int(len(final)),
                "human_audited_rows": overall_total,
                "audit_queue_rows": int(len(audited)),
                "audit_queue_drop_rows": int((audited["review_action"].astype(str).str.lower() == "drop_row").sum()),
                "llm_canonical_correct_before_human_review": overall_correct,
                "llm_canonical_accuracy_before_human_review": round(safe_div(overall_correct, overall_total), 6),
                "llm_canonical_accuracy_ci_low": round(ci_low, 6),
                "llm_canonical_accuracy_ci_high": round(ci_high, 6),
            },
            "outputs": {k: str(v) for k, v in out_files.items()},
        },
    )

    print(f"Saved reference audit outputs to: {out_dir}")
    print(f"Audited kept rows: {overall_total:,}")
    print(f"Pre-correction LLM canonical accuracy: {safe_div(overall_correct, overall_total):.4f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
