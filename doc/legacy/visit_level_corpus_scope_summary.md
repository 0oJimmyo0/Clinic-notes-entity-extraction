# Visit-Level Corpus Scope Summary

Date: 2026-04-08

## What Changed
The corpus definition was moved from note-level random capping to visit-level cohort construction.

Script updated:
- `resources/script/run_select_note_corpus.py`

Run mode used:
- `--require-candidates-for-adjudication`
- `--require-structured-drugs-for-downstream`
- `--sampling-mode stratified`
- `--stratify-by note_type,visit_count_bin,candidate_count_bin`
- `--max-visits-per-patient-for-adjudication 3`
- `--max-adjudication-visits 5000`

## Inclusion Logic Implemented
Visit-level base eligibility:
- valid `person_id` and `visit_occurrence_id`
- note text non-empty
- note length >= 20 chars
- deduplicate by `note_id`
- deduplicate exact normalized note text within the same visit

Adjudication subset:
- requires at least one Stage-1 treatment-context candidate span
- per-patient visit cap applied for manual-review fairness
- stratified sampling by note type + patient visit-count bin + candidate-count bin

Downstream evaluation cohort:
- full eligible visits filtered to those with structured drug data present

## Cohort Sizes (Current Run)
- Notes input: 400,283
- Notes after cleaning/dedup: 276,260
- Full eligible visits: 95,078
- Full eligible patients: 2,106
- Visits with candidate spans: 21,693
- Visits with structured drugs: 10,828
- Downstream evaluation visits: 10,828
- Downstream evaluation patients: 2,041
- Adjudication subset visits: 5,000
- Adjudication subset patients: 2,051

## Critical Distribution Signals
- Full cohort patients with >1 visit: 2,079 / 2,106
- Evaluation cohort patients with >1 visit: 1,790 / 2,041
- Adjudication subset patients with >1 visit: 1,817 / 2,051
- Candidate-span intensity (adjudication subset):
  - p50 = 2
  - p90 = 11
  - p99 = 50

## Output Files
- `episode_notes/manifests/full_visit_eligible_manifest.csv`
- `episode_notes/manifests/evaluation_visit_manifest.csv`
- `episode_notes/manifests/adjudication_subset_manifest.csv`
- `episode_notes/manifests/evaluation_note_manifest.csv`
- `episode_notes/manifests/adjudication_note_manifest.csv`
- `episode_notes/manifests/cohort_justification_summary.json`

## Why This Is Better Than the Previous 50k Note Cap
- Aligns unit of analysis with downstream concordance (visit-level).
- Keeps full downstream cohort without dropping high-utilization patients by default.
- Uses patient-capped, stratified adjudication subset to avoid reviewer burden and dominance effects.
- Enforces treatment-context relevance for adjudication via candidate-span requirement.
