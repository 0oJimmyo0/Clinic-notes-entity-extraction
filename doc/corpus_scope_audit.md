# Corpus Scope Audit (Chunks 000-019)

Date: 2026-04-08

## Source
- Input files: `episode_notes/episode_notes_chunk000.parquet` ... `episode_notes/episode_notes_chunk019.parquet`
- Counted over all 20 chunks.

## Observed Schema
- `TaskIDNumber`
- `person_id`
- `visit_occurrence_id`
- `note_id`
- `note_date`
- `note_datetime`
- `note_title`
- `note_text`

## Scope Summary
- Total rows (all notes): 400,283
- Non-empty note rows: 399,099
- Unique patients: 2,106
- Unique visits: 95,436
- Unique note IDs: 291,621
- Duplicate rows by `note_id`: 108,662
- Missing `note_id`: 0
- Missing `person_id`: 0
- Missing `visit_occurrence_id`: 0

Interpretation:
- The corpus is much larger than the target cap of 50,000 notes.
- Downsampling/selection is required before LLM extraction and baseline/PathA/PathB evaluation.

## 50k Selection Output
A patient-aware selected corpus was generated with:
- max notes total: 50,000
- max notes per patient (soft cap before fill): 50
- random seed: 42
- selection pool: non-empty notes, deduplicated by `note_id`

Outputs:
- `episode_notes/selected_corpus_50k_manifest.csv`
- `episode_notes/selected_corpus_50k.parquet`

Selected corpus coverage:
- Selected notes: 50,000
- Selected unique patients: 2,102
- Selected unique visits: 30,831

## Recommended Next Step
Use `selected_corpus_50k.parquet` as the common input for:
1. LLM entity extraction + adjudication subset creation
2. Existing pipeline runs (baseline, Path A, Path B)

This keeps both methods aligned to the same corpus and supports fair downstream comparison.
