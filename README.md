# RQ1 Resources Scripts Repo

This repository packages the RQ1 script stack and planning docs for treatment-context drug concordance work.

## Included
- `script/`: pipeline and evaluation scripts.
- `doc/`: paper planning notes.
- `lexicons/`: lightweight lexicon/config artifacts used by scripts.

## Excluded (large data)
- `raw/`
- `struct_data/`
- any chunked outputs/results directories

## Main Pipeline Scripts
- `script/run_candidates_overnight.py`
- `script/run_stage2_overnight.py`
- `script/run_rq1_step2_aggregate.py`
- `script/run_rq1_step3_build_ehr_by_visit.py`
- `script/run_rq1_step3b_build_timeline.py`
- `script/run_rq1_step4_similarity.py`
- `script/run_rq1_step4b_calibrate_drug_linker.py`
- `script/run_rq1_step5_make_outputs.py`
- `script/rq1_drug_linking.py`

## Lexicon Build Utilities
- `script/process_raw_vocabularies.py`
- `script/build_public_lexicons.py`
- `script/discover_terms_from_corpus.py`

## Notes
Script defaults may reference paths outside this repository root (for example `episode_extraction_results/`). Pass explicit CLI args when running in a different environment.
