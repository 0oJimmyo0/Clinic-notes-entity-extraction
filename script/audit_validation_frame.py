#!/usr/bin/env python3
"""Print aggregate, ID-only feasibility checks for the proposed raw-note validation frame.

No note text, patient IDs, note IDs, or medication strings are printed or written.
Artifact membership flags exposure for investigation; it does not prove manual review.
"""

from __future__ import annotations

import csv
import json
from collections import Counter
from pathlib import Path

import pyarrow.parquet as pq


PROJECT = Path(__file__).resolve().parents[2]


def rows(relative_path: str):
    with (PROJECT / relative_path).open(newline="", encoding="utf-8") as handle:
        yield from csv.DictReader(handle)


def patient_ids(relative_path: str, eligible: set[str]) -> set[str]:
    return {row["person_id"] for row in rows(relative_path) if row["person_id"] in eligible}


def main() -> None:
    prior_cohort = {
        row["person_id"]
        for row in rows("episode_notes/subcohort_clinic_like_20k_30k/patient_manifest.csv")
    }
    evaluation = list(rows("episode_notes/manifests_clinic_only/evaluation_note_manifest.csv"))
    all_patients = {row["person_id"] for row in evaluation}
    outside = all_patients - prior_cohort
    outside_rows = [row for row in evaluation if row["person_id"] in outside]
    note_counts = Counter(row["person_id"] for row in outside_rows)

    artifacts = {
        "automated_selection_manifest": "episode_notes/manifests_clinic_only/adjudication_note_manifest.csv",
        "clinic_only_packet_notes": "episode_extraction_results/clinic_only/rq1/adjudication_packets/adjudication_packets_notes.csv",
        "global_packet_notes": "episode_extraction_results/rq1/adjudication_packets/adjudication_packets_notes.csv",
        "clinic_only_reviewed_bridge": "episode_extraction_results/clinic_only/rq1/adjudicated/reviewed_adjudication_from_medications_jsonl.csv",
    }
    exposed = {name: patient_ids(path, outside) for name, path in artifacts.items()}
    note_to_patient = {row["note_id"]: row["person_id"] for row in outside_rows}
    medications_path = PROJECT / "resources/struct_data/medications.jsonl"
    with medications_path.open(encoding="utf-8") as handle:
        medication_note_ids = {
            str(json.loads(line).get("note_id", "")) for line in handle if line.strip()
        }
    exposed["medications_jsonl"] = {
        note_to_patient[note_id]
        for note_id in medication_note_ids
        if note_id in note_to_patient
    }

    full_text_index = pq.read_table(
        PROJECT / "episode_notes/clinic_only_fulltext_chunks/full_visit_eligible_notes.parquet",
        columns=["person_id", "note_id"],
    ).to_pydict()
    full_text_keys = {
        (str(person_id), str(note_id))
        for person_id, note_id in zip(
            full_text_index["person_id"], full_text_index["note_id"], strict=True
        )
    }

    packet_or_bridge = set().union(
        exposed["clinic_only_packet_notes"],
        exposed["global_packet_notes"],
        exposed["clinic_only_reviewed_bridge"],
        exposed["medications_jsonl"],
    )
    ordered_counts = sorted(note_counts.values())
    summary = {
        "evaluation_manifest_patients": len(all_patients),
        "bibm_cohort_patients": len(prior_cohort),
        "outside_bibm_patients": len(outside),
        "outside_bibm_note_rows": len(outside_rows),
        "outside_bibm_unique_note_ids": len({row["note_id"] for row in outside_rows}),
        "outside_bibm_duplicate_patient_note_rows": len(outside_rows)
        - len({(row["person_id"], row["note_id"]) for row in outside_rows}),
        "patients_with_at_least_3_notes": sum(count >= 3 for count in note_counts.values()),
        "patients_with_at_least_6_notes": sum(count >= 6 for count in note_counts.values()),
        "median_notes_per_patient": ordered_counts[len(ordered_counts) // 2],
        "rows_with_key_in_fulltext_parquet": sum(
            (row["person_id"], row["note_id"]) in full_text_keys
            for row in outside_rows
        ),
        "artifact_membership_patients": {name: len(ids) for name, ids in exposed.items()},
        "patients_in_any_packet_or_bridge": len(packet_or_bridge),
        "patients_not_in_any_packet_or_bridge": len(outside - packet_or_bridge),
        "patients_not_in_automated_selection_manifest": len(
            outside - exposed["automated_selection_manifest"]
        ),
    }
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
