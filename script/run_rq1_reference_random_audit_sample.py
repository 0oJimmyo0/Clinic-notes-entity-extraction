#!/usr/bin/env python3
"""
Create a random/stratified sample of currently unaudited reference rows for future manual reliability review.

This script prepares a review packet, annotation template, and guideline. It does not
estimate correctness by itself because manual review labels are not available in the
current repo state.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from rq1_adjudication_utils import write_run_summary
from rq1_bibm_utils import action_cue


GUIDELINE_TEXT = """# Random Audit Annotation Guideline

## Purpose
This audit estimates the reliability of the unaudited portion of the LLM-bootstrapped
reference set. It is separate from the targeted difficult-review audit queue.

## Core review questions
For each row, review the bounded note context and determine:
1. Is the highlighted mention a valid medication mention in context?
2. Is the span acceptable for the medication evidence being captured?
3. Is the proposed canonical label correct?
4. Is the action or treatment-context cue correct?

## Field definitions

### medication mention validity
- `yes`: the mention refers to a medication, regimen, or clearly medication-like treatment item in context
- `no`: the mention is not a medication reference in context
- `uncertain`: context is insufficient or ambiguous

### acceptable span
- `yes`: the span is acceptable for the medication evidence being reviewed
- `no`: the span is clearly too broad, too narrow, or points to the wrong text
- `uncertain`: span quality cannot be judged confidently from bounded context

### canonical-label correctness
- `yes`: the proposed canonical label matches the medication intended by the note
- `no`: the proposed canonical label is incorrect
- `uncertain`: the medication identity cannot be resolved confidently

If `canonical_correct = no`, provide `corrected_canonical_label` when possible.

### treatment-context or action correctness
- `yes`: the proposed action cue is directionally correct in context
- `no`: the action cue is inconsistent with the note context
- `uncertain`: action is not recoverable confidently from the bounded note context

## Acceptable handling rules

### brand and generic names
- Treat brand and generic as correct when they refer to the same underlying medication concept used by the project-specific canonical label space.
- If the proposed label chooses the wrong brand or wrong generic concept, mark incorrect.

### combination drugs
- Mark incorrect when a combination product is reduced to only one ingredient or mapped to the wrong combined concept.
- Use `combination_mismatch` as the error category when appropriate.

### formulation or salt variants
- If the project-specific canonical label intentionally collapses clinically equivalent formulation or salt variants, treat that as correct.
- If the formulation or salt distinction changes the intended mapped concept in the project label space, mark incorrect and use `formulation_or_salt_variant`.

### non-medication, labs, and substances
- If the mention is actually a lab, a biomarker, a non-treatment substance, or another non-medication entity, mark `medication_valid = no`.
- Use `non_medication_or_lab_substance` as the error category.

## Suggested error categories
- `correct`
- `candidate_generation_miss`
- `wrong_canonical_alias`
- `brand_generic_mismatch`
- `combination_mismatch`
- `formulation_or_salt_variant`
- `action_incorrect`
- `non_medication_or_lab_substance`
- `span_problem`
- `uncertain_context`
- `other`

## Uncertain cases
- Use `uncertain` when the bounded context is not sufficient for confident review.
- In the downstream analysis, uncertain canonical judgments are counted as incorrect in the conservative accuracy and excluded in the lenient accuracy.

## Confidence scale
- `high`: reviewer is confident in the judgment
- `medium`: reviewer sees minor ambiguity but can still decide
- `low`: reviewer judgment is tentative
"""


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sample unaudited reference rows for optional manual reliability review.")
    p.add_argument(
        "--final-reviewed-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/adjudicated/reviewed_adjudication_patched.csv",
    )
    p.add_argument(
        "--audit-review-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/adjudicated/strict_pathb_review_queue_completed_final.csv",
    )
    p.add_argument(
        "--packets-mentions-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/adjudication_packets/adjudication_packets_mentions.csv",
    )
    p.add_argument("--sample-size", type=int, default=300)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument(
        "--sampling-mode",
        choices=["random", "stratified"],
        default="stratified",
    )
    p.add_argument(
        "--output-dir",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/reference_random_audit_sample",
    )
    return p.parse_args()


def _normalize(df: pd.DataFrame, col: str) -> None:
    if col in df.columns:
        df[col] = df[col].fillna("").astype(str).str.strip()


def _split_strata(value: str) -> tuple[str, str, str]:
    parts = str(value).split("||")
    while len(parts) < 3:
        parts.append("")
    return parts[0], parts[1], parts[2]


def _build_annotation_template(sample_df: pd.DataFrame) -> pd.DataFrame:
    work = sample_df.copy()
    work["row_id"] = [f"random_audit_{i:03d}" for i in range(1, len(work) + 1)]
    split = work["_strata"].astype(str).map(_split_strata)
    work["stratum_note_type"] = split.map(lambda x: x[0])
    work["stratum_candidate_category"] = split.map(lambda x: x[1])
    work["stratum_action_cue"] = split.map(lambda x: x[2])
    template = pd.DataFrame(
        {
            "row_id": work["row_id"],
            "adjudication_unit_id": work.get("adjudication_unit_id", ""),
            "person_id": work.get("person_id", ""),
            "visit_id": work.get("visit_id", ""),
            "note_id": work.get("note_id", ""),
            "span_id_or_local_reference": work.get("span_id_or_local_reference", ""),
            "bounded_note_context": work.get("context_text", ""),
            "mention_text": work.get("raw_mention_text", ""),
            "proposed_canonical_label": work.get("adjudicated_canonical_label", ""),
            "proposed_mention_status": work.get("mention_status", ""),
            "action_cue": work.get("action_cue", ""),
            "note_type": work.get("note_title", ""),
            "candidate_category": work.get("candidate_category", ""),
            "seed_treatment_action": work.get("seed_treatment_action", ""),
            "seed_discontinuation_reason": work.get("seed_discontinuation_reason", ""),
            "seed_certainty": work.get("seed_certainty", ""),
            "stratum_note_type": work["stratum_note_type"],
            "stratum_candidate_category": work["stratum_candidate_category"],
            "stratum_action_cue": work["stratum_action_cue"],
            "sample_stratum": work.get("_strata", ""),
            "reviewer_1_medication_valid": "",
            "reviewer_1_span_valid": "",
            "reviewer_1_canonical_correct": "",
            "reviewer_1_corrected_canonical_label": "",
            "reviewer_1_action_correct": "",
            "reviewer_1_error_category": "",
            "reviewer_1_confidence": "",
            "reviewer_1_notes": "",
            "reviewer_2_medication_valid": "",
            "reviewer_2_span_valid": "",
            "reviewer_2_canonical_correct": "",
            "reviewer_2_corrected_canonical_label": "",
            "reviewer_2_action_correct": "",
            "reviewer_2_error_category": "",
            "reviewer_2_confidence": "",
            "reviewer_2_notes": "",
            "adjudicated_medication_valid": "",
            "adjudicated_span_valid": "",
            "adjudicated_canonical_correct": "",
            "adjudicated_corrected_canonical_label": "",
            "adjudicated_action_correct": "",
            "adjudicated_error_category": "",
            "adjudicated_confidence": "",
        }
    )
    return template


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    final_path = (root / args.final_reviewed_csv).resolve()
    audit_path = (root / args.audit_review_csv).resolve()
    packets_path = (root / args.packets_mentions_csv).resolve()
    out_dir = (root / args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    final = pd.read_csv(final_path).fillna("")
    audit = pd.read_csv(audit_path).fillna("")
    packets = pd.read_csv(packets_path).fillna("")
    for df in [final, audit, packets]:
        for col in ["adjudication_unit_id", "note_id", "person_id", "visit_id"]:
            _normalize(df, col)

    audited_ids = set(audit["adjudication_unit_id"].astype(str))
    sample_pool = final[~final["adjudication_unit_id"].astype(str).isin(audited_ids)].copy()

    packets_small = packets[
        [
            c
            for c in [
                "adjudication_unit_id",
                "note_title",
                "candidate_category",
                "seed_treatment_action",
                "seed_discontinuation_reason",
                "seed_certainty",
            ]
            if c in packets.columns
        ]
    ].copy()
    sample_pool = sample_pool.merge(packets_small, on="adjudication_unit_id", how="left")
    sample_pool["action_cue"] = sample_pool.apply(
        lambda r: action_cue(
            str(r.get("seed_treatment_action", "")),
            str(r.get("seed_discontinuation_reason", "")),
            str(r.get("context_text", "")),
        ),
        axis=1,
    )

    if args.sampling_mode == "random" or len(sample_pool) <= args.sample_size:
        sample_df = sample_pool.sample(n=min(args.sample_size, len(sample_pool)), random_state=args.seed)
    else:
        sample_pool["_strata"] = (
            sample_pool.get("note_title", "").astype(str).str.slice(0, 40)
            + "||"
            + sample_pool.get("candidate_category", "").astype(str)
            + "||"
            + sample_pool["action_cue"].astype(str)
        )
        counts = sample_pool["_strata"].value_counts(dropna=False)
        alloc = ((counts / counts.sum()) * args.sample_size).round().astype(int)
        alloc[alloc == 0] = 1
        parts = []
        for key, n_take in alloc.items():
            part = sample_pool[sample_pool["_strata"] == key]
            parts.append(part.sample(n=min(len(part), n_take), random_state=args.seed))
        sample_df = pd.concat(parts, ignore_index=True).drop_duplicates("adjudication_unit_id")
        if len(sample_df) > args.sample_size:
            sample_df = sample_df.sample(n=args.sample_size, random_state=args.seed)

    out_csv = out_dir / "rq1_reference_random_audit_sample.csv"
    sample_df.to_csv(out_csv, index=False)
    template_csv = out_dir / "rq1_reference_random_audit_annotation_template.csv"
    _build_annotation_template(sample_df).to_csv(template_csv, index=False)
    guideline_md = out_dir / "rq1_reference_random_audit_annotation_guideline.md"
    guideline_md.write_text(GUIDELINE_TEXT, encoding="utf-8")
    write_run_summary(
        out_dir / "rq1_reference_random_audit_sample_summary.json",
        {
            "inputs": {
                "final_reviewed_csv": str(final_path),
                "audit_review_csv": str(audit_path),
                "packets_mentions_csv": str(packets_path),
                "sampling_mode": args.sampling_mode,
                "sample_size_requested": int(args.sample_size),
                "seed": int(args.seed),
            },
            "counts": {
                "unaudited_pool_rows": int(len(sample_pool)),
                "sample_rows": int(len(sample_df)),
            },
            "outputs": {
                "sample_csv": str(out_csv),
                "annotation_template_csv": str(template_csv),
                "annotation_guideline_md": str(guideline_md),
            },
            "note": "Manual review required before label correctness / CI can be reported.",
        },
    )
    print(f"Saved random audit sample to: {out_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
