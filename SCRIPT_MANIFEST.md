# Script Manifest

This file identifies the scripts included in this lightweight repository.

## Core RQ1 Pipeline
- `script/run_candidates_overnight.py` - candidate generation over note chunks.
- `script/run_stage2_overnight.py` - medspaCy/entity extraction over candidate spans.
- `script/run_rq1_step2_aggregate.py` - visit-level aggregation of note entities.
- `script/run_rq1_step3_build_ehr_by_visit.py` - visit-level aggregation of structured EHR entities.
- `script/run_rq1_step3b_build_timeline.py` - timeline construction for windowed comparisons.
- `script/run_rq1_step4_similarity.py` - baseline/Path A/Path B similarity evaluation.
- `script/run_rq1_step4b_calibrate_drug_linker.py` - Path B calibration diagnostics and sweeps.
- `script/run_rq1_step5_make_outputs.py` - table-ready/report outputs.

## Drug Linking and Matching Logic
- `script/rq1_drug_linking.py` - normalization/linking utilities used by Step 4.
- `script/run_rq1_step0_freeze_baseline.py` - snapshot freeze and baseline diagnostics helper.

## Lexicon Build and Term Discovery Utilities
- `script/process_raw_vocabularies.py` - process local raw vocabularies into lexicons.
- `script/build_public_lexicons.py` - download/build lexicons (where possible).
- `script/discover_terms_from_corpus.py` - discover candidate terms from corpus text.

## Planning/Documentation
- `doc/rq1_full_paper_plan.md` - current narrowed manuscript plan.
- `script/README.md` - lexicon build usage details.
- `README.md` - repository scope and include/exclude rules.
