# Subcohort notes.parquet: Detailed Statistics and Readiness Assessment

Date: 2026-04-09

## Data Source

- File: episode_notes/subcohort_patient_complete/notes.parquet
- Related manifests:
  - episode_notes/subcohort_patient_complete/note_manifest.csv
  - episode_notes/subcohort_patient_complete/visit_manifest.csv
  - episode_notes/subcohort_patient_complete/patient_manifest.csv
  - episode_notes/subcohort_patient_complete/summary.json

## Structural Profile

- Rows: 21,786
- Columns: 12
- Column set:
  - TaskIDNumber
  - person_id
  - visit_occurrence_id
  - note_id
  - note_date
  - note_datetime
  - note_title
  - note_title_norm
  - note_len
  - note_text
  - source_note_text_col
  - is_clinic_like_note

## Data Quality Checks

Completeness:
- 0 missing values in all 12 columns.

Uniqueness and duplicate checks:
- Duplicate note_id rows: 0
- Duplicate person_id + visit_occurrence_id + note_id rows: 0
- Duplicate person_id + visit_occurrence_id + note_text rows: 0

Manifest consistency:
- notes.parquet rows minus note_manifest rows: 0
- notes note_id not in note_manifest: 0
- note_manifest note_id not in notes.parquet: 0
- notes visit_id not in visit_manifest: 0
- notes person_id not in patient_manifest: 0

## Cohort Cardinality

- Unique patients: 479
- Unique visits: 7,499
- Unique notes: 21,786
- Notes per patient (mean): 45.48
- Notes per patient (p50): 38
- Notes per patient (p95): 104.3
- Notes per visit (mean): 2.91
- Notes per visit (p50): 2
- Notes per visit (p95): 7

## Text-Length and Content Coverage

note_len and note_text length are exactly aligned:
- mean absolute difference: 0
- exact matches: 21,786/21,786

Text length distribution:
- Empty text rows: 0
- <50 chars: 950
- <100 chars: 3,223
- 100-299 chars: 3,445
- 300-999 chars: 3,571
- >=1000 chars: 11,547
- mean: 1,134.43
- p50: 1,193
- p95: 2,000

Important cap signal:
- note_len max = 2,000
- rows with note_len == 2,000: 9,063 (41.6%)

Interpretation: there is likely truncation/capping at 2,000 characters for a large fraction of notes.

## Clinic-Like vs Non-Clinic Mix

- is_clinic_like_note true: 10,171 (46.69%)
- is_clinic_like_note false: 11,615 (53.31%)

Top clinic-like titles:
- Progress Notes: 7,604
- Assessment & Plan Note: 1,390
- H&P: 355
- Consults: 312
- Patient Instructions: 281

Top non-clinic titles:
- IMAGING: 2,487
- SOCIAL HISTORY: 2,330
- PATHOLOGY AND CYTOLOGY: 787
- PROCEDURES: 511
- Anesthesia Procedure Notes: 467

## Temporal Coverage

- Date range: 2017-11-02 to 2025-05-12
- Missing dates: 0
- Distinct years: 9
- Largest years by note count:
  - 2018: 6,545
  - 2019: 4,701
  - 2021: 4,654
  - 2020: 3,602

## Selection Bias / Enrichment Relative to Full Eligible Visits

Comparison against episode_notes/manifests/full_visit_eligible_manifest.csv:
- Full eligible visits: 95,078
- Subcohort visits: 7,499 (7.89% of full eligible)
- Full eligible patients: 2,106
- Subcohort patients: 479 (22.74% of full eligible)

Enrichment indicators (visit level):
- has_candidate_span:
  - full: 22.82%
  - subcohort: 52.02%
  - delta: +29.20 percentage points
- has_structured_drug_data:
  - full: 11.39%
  - subcohort: 27.46%
  - delta: +16.07 percentage points

Interpretation: this is an intentionally enriched analytic cohort, not a population-representative sample.

## LLM Extraction Reliability Assessment

Can this cohort be used for LLM entity extraction?
- Yes, for development and engineering validation.
- Conditions:
  - Use a compliant environment for protected health text.
  - Treat outputs as candidate extraction signals, not final truth.
  - Account for note truncation and non-clinic-note mix.

Reliability strengths:
- No missing core fields.
- No duplicate key collisions.
- Strong manifest alignment.
- Reasonable longitudinal depth per patient.

Reliability risks:
- 41.6% of notes are at the 2,000-char cap, so context may be incomplete.
- Non-clinic note proportion is high (53.31%), adding off-target text for treatment-context extraction.
- Cohort is enriched and selective relative to full eligible population.

## Academic Publishability Assessment

Can this cohort be used in a published paper?
- Yes, as a well-documented development/evaluation cohort for treatment-context extraction, if framed correctly.
- Not sufficient alone for broad generalization claims.

Required framing in paper:
- Explicitly describe subcohort selection constraints and enrichment.
- State that this is not a random sample of all eligible visits.
- Report note truncation/capping behavior and potential effect on recall.
- Keep adjudicated note-grounded truth as primary evaluation source.
- Keep structured EHR concordance as downstream/secondary analysis.

Recommended minimum additions before final claims:
- Sensitivity analysis by note type (clinic-like vs non-clinic).
- Sensitivity analysis for capped (2,000-char) vs uncapped notes.
- Coverage table showing candidate-bearing vs non-candidate visits.
- Clear external-validity limitations section.

## Decision Summary

- Engineering reliability for LLM extraction: acceptable with caveats.
- Academic suitability: acceptable for publication when positioned as an adjudication-first, enriched cohort with explicit limitations.
- Do not position this cohort as representative of all clinic notes or all eligible visits.
