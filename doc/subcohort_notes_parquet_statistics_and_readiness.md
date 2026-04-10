# Clinic-Like 20-30k Subcohort: Detailed Statistics and Readiness Assessment

Date: 2026-04-10

## Direct Answer

Yes. The active cohort has been switched to the clinic-like 20-30k version.

- notes: 22,483
- clinic-like notes: 22,483 / 22,483 (100.00%)
- non-clinic-like notes: 0 / 22,483 (0.00%)

## Data Source (Active 20-30k Cohort)

- Primary notes file:
  - `episode_notes/subcohort_clinic_like_20k_30k/notes.parquet`
- Related manifests:
  - `episode_notes/subcohort_clinic_like_20k_30k/note_manifest.csv`
  - `episode_notes/subcohort_clinic_like_20k_30k/visit_manifest.csv`
  - `episode_notes/subcohort_clinic_like_20k_30k/patient_manifest.csv`
  - `episode_notes/subcohort_clinic_like_20k_30k/summary.json`

Current extraction run outputs for this cohort:
- `episode_extraction_results/clinic_like_20k_30k/candidates/all_candidates_combined.csv`
- `episode_extraction_results/clinic_like_20k_30k/stage2/extracted_treatment_data_episode_cleaned.csv`
- `episode_extraction_results/clinic_like_20k_30k/rq1/rq1_note_entities_by_visit.csv`
- `episode_extraction_results/clinic_like_20k_30k/rq1/pre_adjudication_dryrun/rq1_preadj_dryrun_summary.json`

## Structural Profile

- Rows: 22,483
- Columns: 12
- Column set:
  - `TaskIDNumber`
  - `person_id`
  - `visit_occurrence_id`
  - `note_id`
  - `note_date`
  - `note_datetime`
  - `note_title`
  - `note_title_norm`
  - `note_len`
  - `note_text`
  - `source_note_text_col`
  - `is_clinic_like_note`

## Data Quality Checks

Completeness:
- 0 missing values in all 12 columns.

Uniqueness and duplicate checks:
- Duplicate `note_id` rows: 0
- Duplicate `person_id + visit_occurrence_id + note_id` rows: 0
- Duplicate `person_id + visit_occurrence_id + note_text` rows: 0

Manifest consistency:
- notes.parquet rows minus note_manifest rows: 0
- notes note_id not in note_manifest: 0
- note_manifest note_id not in notes.parquet: 0
- notes visit_id not in visit_manifest: 0
- notes person_id not in patient_manifest: 0

## Cohort Cardinality

- Unique patients: 761
- Unique visits: 11,812
- Unique notes: 22,483

Notes per patient:
- mean: 29.54
- p50: 25
- p95: 68

Notes per visit:
- mean: 1.90
- p50: 1
- p95: 4

## Text-Length and Content Coverage

`note_len` and `note_text` length are exactly aligned:
- mean absolute difference: 0
- exact matches: 22,483 / 22,483

Text length distribution:
- Empty text rows: 0
- <50 chars: 816
- <100 chars: 2,011
- 100-299 chars: 2,787
- 300-999 chars: 2,897
- >=1000 chars: 14,788
- mean: 1,373.52
- p50: 2,000
- p95: 2,000

Important cap signal:
- `note_len` max = 2,000
- rows with `note_len == 2,000`: 13,067 (58.12%)

Interpretation: the 2,000-character hard-cap signature remains and should be treated as an upstream context-limitation risk.

## Clinic-Like Composition

- `is_clinic_like_note = true`: 22,483 (100.00%)
- `is_clinic_like_note = false`: 0 (0.00%)

Top note titles:
- Progress Notes: 16,981
- Assessment & Plan Note: 3,060
- H&P: 740
- Patient Instructions: 625
- Consults: 542
- Discharge Instructions: 239
- H&P (View-Only): 163
- Research Coordinator Notes: 133

## Temporal Coverage

- Date range: 2017-11-02 to 2023-04-10
- Missing dates: 0
- Distinct years: 7
- Largest years by note count:
  - 2018: 6,647
  - 2019: 5,142
  - 2021: 4,319
  - 2020: 3,932
  - 2022: 1,557
  - 2017: 884

## Extraction Engineering Readiness (Current 20-30k Run)

Current pre-adjudication pipeline outputs on this cohort:
- Candidate rows: 21,530
- Stage-2 rows: 21,530
- Stage-2 rows with non-empty drugs: 8,001
- Stage-2 rows with any non-empty entity column: 11,430
- Visit-level note-entity rows (Step 2): 6,948
- Visit-level rows with any non-empty entity column: 4,664
- Pre-adjudication mentions entering normalization dry-run: 11,195

Status:
- Stage-1 candidate extraction: completed
- Stage-2 extraction: completed with non-empty entity output
- Step-2 note aggregation: completed with non-empty visit-level coverage
- Pre-adjudication dry run: completed

## Interpretation for Mentor Handoff and Next Step

- This is the cohort to share for the clinic-like 20-30k plan.
- It is clinic-like-only by construction and within the intended note budget.
- Engineering prerequisites for LLM entity extraction and human review handoff are in place.
- Final adjudicated truth performance is not yet claimed here; these are pre-adjudication readiness statistics.
