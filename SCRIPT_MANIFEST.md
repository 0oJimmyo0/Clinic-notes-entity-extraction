# Script Manifest

This file identifies the scripts included in this lightweight repository.

## Paper Workflow

1. corpus selection
2. candidate/context extraction
3. adjudication packet build
4. adjudication join
5. note-truth extraction evaluation
6. note-truth normalization evaluation
7. downstream EHR concordance

## Core RQ1 Pipeline
- `script/run_candidates_overnight.py` - candidate generation over note chunks.
- `script/run_stage2_overnight.py` - medspaCy/entity extraction over candidate spans.
- `script/run_rq1_step2_aggregate.py` - visit-level aggregation of note entities.
- `script/run_rq1_step3_build_ehr_by_visit.py` - visit-level aggregation of structured EHR entities.
- `script/run_rq1_step3b_build_timeline.py` - timeline construction for windowed comparisons.
- `script/run_build_adjudication_packets.py` - reviewer-ready mention and note packet build.
- `script/run_join_adjudication_labels.py` - join reviewed adjudication labels back to seeded extracted mentions.
- `script/run_rq1_step4_note_truth_eval.py` - primary extraction evaluation against adjudicated note truth.
- `script/run_rq1_step5_normalization_eval.py` - baseline vs Path A vs Path B against adjudicated canonical labels.
- `script/run_rq1_step4b_calibrate_pathb.py` - Path B calibration using adjudicated leftovers.
- `script/run_rq1_step6_downstream_concordance.py` - downstream note-to-EHR concordance on adjudicated comparable mentions.
- `script/run_rq1_step4_similarity.py` - legacy downstream concordance/overlap utility.
- `script/run_rq1_step4b_calibrate_drug_linker.py` - legacy overlap-era linker calibration helper.
- `script/run_rq1_step5_make_outputs.py` - paper-output builder for the layered workflow.

## Drug Linking and Matching Logic
- `script/rq1_drug_linking.py` - Path A + canonical-vocabulary Path B normalization/linking utilities.
- `script/run_rq1_step0_freeze_baseline.py` - snapshot freeze and baseline diagnostics helper.

## Lexicon Build and Term Discovery Utilities
- `script/process_raw_vocabularies.py` - process local raw vocabularies into lexicons.
- `script/build_public_lexicons.py` - download/build lexicons (where possible).
- `script/discover_terms_from_corpus.py` - discover candidate terms from corpus text.

## Planning/Documentation
- `doc/rq1_full_paper_plan.md` - current narrowed manuscript plan.
- `script/README.md` - lexicon build usage details.
- `README.md` - repository scope and include/exclude rules.
