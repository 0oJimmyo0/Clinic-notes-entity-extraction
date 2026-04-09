# Full-Note Pipeline Upgrade (No 2000-char Dependency)

## Goal
Move from truncated-note processing to a defensible architecture:
1. Preserve full note text.
2. Scan full note text for treatment-context candidates.
3. Keep candidate spans + local context for downstream extraction.

## What Changed
- `resources/script/run_select_note_corpus.py`
  - Added flexible note text column resolution via:
    - `--note-text-col-candidates` (default: `note_text_full,full_note_text,note_text,text`)
  - Added truncation diagnostics in summary JSON:
    - `max_note_len_after_dedup`
    - `pct_note_len_eq_max_after_dedup`
    - `pct_note_len_eq_2000_after_dedup`
    - `pct_note_len_ge_1800_after_dedup`
    - `suspected_hard_cap_2000`
  - Writes cohort note parquet outputs by default (unless `--no-write-cohort-note-parquet`):
    - `full_visit_eligible_notes.parquet`
    - `evaluation_cohort_notes.parquet`
    - `adjudication_subset_notes.parquet`

- `resources/script/run_candidates_overnight.py`
  - Added `--note-text-col-candidates` with same default order.
  - Stage-1 scanner now reports note-length distribution per chunk and warns on strong 2000-char hard-cap signature.

- `resources/script/run_attach_full_note_text.py` (new)
  - Joins external full-text source by `note_id` and writes upgraded chunk files with `note_text_full`.
  - This is the bridge when current chunks are truncated upstream.

## Recommended Run Order
1. If full-text source exists, build upgraded chunks:

```bash
python resources/script/run_attach_full_note_text.py \
  --notes-dir episode_notes \
  --glob 'episode_notes_chunk*.parquet' \
  --full-text-source /path/to/full_notes.parquet \
  --full-source-note-id-col note_id \
  --full-source-text-col note_text \
  --output-dir episode_notes_fulltext
```

2. Build visit-level cohorts using full text first:

```bash
python resources/script/run_select_note_corpus.py \
  --notes-dir episode_notes_fulltext \
  --glob 'episode_notes_chunk*.parquet' \
  --note-text-col-candidates note_text_full,note_text \
  --candidate-csv episode_extraction_results/archive_candidates/all_candidates_combined.csv \
  --structured-ehr-csv episode_extraction_results/rq1/rq1_ehr_entities_by_visit.csv \
  --output-dir episode_notes/manifests \
  --require-candidates-for-adjudication \
  --require-structured-drugs-for-downstream \
  --sampling-mode stratified \
  --stratify-by note_type,visit_count_bin,candidate_count_bin \
  --max-visits-per-patient-for-adjudication 3 \
  --max-adjudication-visits 5000
```

3. Run Stage-1 candidate extraction on full-text chunks:

```bash
python resources/script/run_candidates_overnight.py \
  --chunk-dir episode_notes_fulltext \
  --output-dir episode_extraction_results/archive_candidates_fulltext \
  --state-file episode_extraction_results/archive_candidates_fulltext/state.json \
  --note-text-col-candidates note_text_full,note_text \
  --combine
```

## Current Limitation in Existing Chunks
Latest summary indicates `used_note_text_cols=["note_text"]` with `max_note_len_after_dedup=2000` and high boundary mass, so current chunk source appears upstream-truncated until a full-text source is attached.
