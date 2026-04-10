# Script Manifest

## Active Pipeline
1. `script/run_select_note_corpus.py` - build eligible note cohort and adjudication/downstream manifests.
2. `script/run_attach_full_note_text.py` - attach full-note payload for context recovery.
3. `script/run_candidates_overnight.py` - treatment-context candidate generation.
4. `script/run_stage2_overnight.py` - stage-2 medication/context extraction.
5. `script/run_rq1_step2_aggregate.py` - visit-level note entity aggregation.
6. `script/run_rq1_step3_build_ehr_by_visit.py` - visit-level structured EHR entity aggregation.
7. `script/run_rq1_step3b_build_timeline.py` - optional timeline build for windowed downstream concordance.
8. `script/run_build_adjudication_packets.py` - adjudication packet construction.
9. `script/run_join_adjudication_labels.py` - adjudication label join and truth table outputs.
10. `script/run_rq1_step4_note_truth_eval.py` - primary extraction evaluation against adjudicated note truth.
11. `script/run_rq1_step4b_calibrate_pathb.py` - Path B confidence calibration on adjudicated leftovers.
12. `script/run_rq1_step5_normalization_eval.py` - baseline vs Path A vs Path B canonical-label evaluation.
13. `script/run_rq1_step6_downstream_concordance.py` - secondary note-to-EHR concordance on adjudicated comparable mentions.
14. `script/run_rq1_step5_make_outputs.py` - paper-ready tables and figures for the layered workflow.

## Active Helper Modules and Utilities
- `script/rq1_drug_linking.py` - deterministic Path A and transparent Path B linking primitives.
- `script/rq1_adjudication_utils.py` - adjudication labels/status parsing and run-summary helpers.
- `script/rq1_concordance_utils.py` - reusable concordance metrics and window helpers used by Step 6.
- `script/run_rq1_step0_freeze_baseline.py` - baseline snapshot helper for controlled comparisons.
- `script/run_build_rq1_drug_canonical_vocab.py` - canonical vocabulary build utility.
- `script/run_collect_patha_unresolved_terms.py` - unresolved Path A term triage utility.
- `script/run_rq1_pre_adjudication_dryrun.py` - pre-adjudication baseline/Path A/Path B engineering diagnostics.
- `script/process_raw_vocabularies.py` - raw-to-lexicon parser.
- `script/build_public_lexicons.py` - lexicon download/build utility.
- `script/discover_terms_from_corpus.py` - corpus term discovery utility.

## Evaluation Contract and Runbooks
- `doc/evaluation_contract.md` - frozen scoring and matching contract before final adjudication labels.
- `doc/pre_adjudication_validation_runbook.md` - pre-adjudication diagnostic run commands and post-adjudication command sequence.

## Archived/Legacy
- `script/legacy/run_rq1_step4_similarity.py` - superseded overlap-era similarity runner.
- `script/legacy/run_rq1_step4b_calibrate_drug_linker.py` - superseded overlap-era linker calibration.
- `script/legacy/run_select_patient_complete_subcohort.py` - off-scope patient-complete subcohort selector.
- `script/legacy/run_build_adjudicated_labels_template.py` - superseded by adjudication packet workflow.
