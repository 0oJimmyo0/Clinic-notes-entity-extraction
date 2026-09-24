#!/usr/bin/env python3
"""Audit candidate patients for traceable review/development exposure.

Default output is aggregate JSON only. An optional patient-level CSV may be written
exclusively to an existing restricted episode_extraction_results directory. Artifact
absence does not certify independence; investigator attestations are still required.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path


PROJECT = Path(__file__).resolve().parents[2]

# Automatic generation or selection is not evidence of human inspection.
AUTOMATED = {
    "in_selection_manifest": "episode_notes/manifests_clinic_only/adjudication_note_manifest.csv",
    "in_global_packet": "episode_extraction_results/rq1/adjudication_packets/adjudication_packets_notes.csv",
    "in_clinic_only_packet": "episode_extraction_results/clinic_only/rq1/adjudication_packets/adjudication_packets_notes.csv",
}

# These are completed/review-linked artifacts, not generated templates or queues.
# A positive exact-ID join is conservative evidence of prior exposure; the identity
# and independence of the human reviewer still need investigator confirmation.
REVIEW_LINKED = {
    "in_clinic_only_reviewed_bridge": "episode_extraction_results/clinic_only/rq1/adjudicated/reviewed_adjudication_from_medications_jsonl.csv",
    "in_bibm_completed_300_audit": "episode_extraction_results/clinic_like_20k_30k/rq1/reference_random_audit_sample_bibm_test/completed_manual_annotation_random_audit_reviewer2_completed.csv",
    "in_bibm_completed_1000_audit": "episode_extraction_results/clinic_like_20k_30k/rq1/reference_random_audit_consensus_prep_n1000/rq1_reference_random_audit_final_adjudicated.csv",
    "in_bibm_no_overlap_manual_review": "episode_extraction_results/clinic_like_20k_30k/rq1/temporal_mismatch_ladder_omop_sensitivity/manual_review_mismatch_bucket_annotated (1).csv",
    "in_bibm_pathb_completed_review": "episode_extraction_results/clinic_like_20k_30k/rq1/adjudicated/strict_pathb_review_queue_completed_final.csv",
    "in_bibm_leftover_completed_review": "episode_extraction_results/clinic_like_20k_30k/rq1/adjudicated/pathb_leftover_review_completed.csv",
}

# Explicit patient-linked source rows used in alias refinement. Manual alias maps
# themselves lack patient IDs and cannot be cleared by an exact-ID scan.
DEVELOPMENT_LINKED = {
    "in_dev_alias_source_rows": "episode_extraction_results/clinic_like_20k_30k/rq1/heldout_split/dev_alias_supplement/dev_reviewed_failure_rows_used.csv",
    "in_patha_v2_reviewed_failure_rows": "episode_extraction_results/clinic_like_20k_30k/rq1/patha_v1_v2_sensitivity/rq1_patha_v1_v2_reviewed_failure_rows.csv",
}


def read_ids(relative_path: str, field: str = "person_id") -> set[str]:
    with (PROJECT / relative_path).open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if field not in (reader.fieldnames or []):
            raise ValueError(f"Missing {field} in {relative_path}")
        return {row[field] for row in reader if row[field]}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--restricted-output",
        type=Path,
        help="Optional absolute CSV path under an existing episode_extraction_results directory; never commit it.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    old = read_ids("episode_notes/subcohort_clinic_like_20k_30k/patient_manifest.csv")
    all_manifest = read_ids("episode_notes/manifests_clinic_only/evaluation_note_manifest.csv")
    candidates = all_manifest - old

    sources = {**AUTOMATED, **REVIEW_LINKED, **DEVELOPMENT_LINKED}
    source_ids = {flag: read_ids(path) & candidates for flag, path in sources.items()}
    automatic_union = set().union(*(source_ids[flag] for flag in AUTOMATED))
    review_union = set().union(*(source_ids[flag] for flag in REVIEW_LINKED))
    development_union = set().union(*(source_ids[flag] for flag in DEVELOPMENT_LINKED))

    records = []
    for patient_id in sorted(candidates):
        flags = {flag: int(patient_id in ids) for flag, ids in source_ids.items()}
        if patient_id in development_union:
            provisional = "D_linked_artifact"
        elif patient_id in review_union:
            provisional = "C_linked_artifact"
        elif patient_id in automatic_union:
            provisional = "B_candidate_automated_only"
        else:
            provisional = "A_candidate_no_traced_exposure"
        records.append(
            {
                "person_id": patient_id,
                "in_bibm": 0,
                **flags,
                "used_for_prompt_or_rule_revision": "unknown",
                "alias_map_patient_provenance": "unresolved",
                "provisional_exposure_class": provisional,
                "exposure_class_confirmed": "unknown",
                "eligibility_reason": "pending_governance_and_investigator_attestation",
            }
        )

    if args.restricted_output is not None:
        output = args.restricted_output.expanduser().resolve()
        restricted_root = (PROJECT / "episode_extraction_results").resolve()
        if not output.is_relative_to(restricted_root) or output.suffix.lower() != ".csv":
            raise ValueError("Patient-level CSV must be under episode_extraction_results and end in .csv")
        if not output.parent.is_dir():
            raise ValueError("Restricted output directory must already exist")
        # Exclusive creation prevents silently replacing a reviewed register.
        with output.open("x", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(records[0]))
            writer.writeheader()
            writer.writerows(records)

    summary = {
        "candidate_patients": len(candidates),
        "artifact_overlap_patients": {flag: len(ids) for flag, ids in source_ids.items()},
        "automated_artifact_union": len(automatic_union),
        "review_linked_artifact_union": len(review_union),
        "development_linked_artifact_union": len(development_union),
        "provisional_classes": dict(Counter(row["provisional_exposure_class"] for row in records)),
        "certified_eligible_patients": 0,
        "certification_status": "NOT_CERTIFIED: manual alias/provenance, investigator exposure, and governance unresolved",
        "patient_level_csv_written": args.restricted_output is not None,
    }
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
