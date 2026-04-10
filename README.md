# Clinic Notes Entity Extraction

This repository contains an adjudication-first, treatment-context medication extraction workflow for clinic notes.

Primary study objective:
- evaluate extraction and normalization against an adjudicated, note-grounded reference set

Secondary objective:
- evaluate downstream note-to-structured-EHR concordance only after adjudication filtering

## Repository Surface
- `script/`: active pipeline and helper modules
- `lexicons/`: alias and canonical vocabulary artifacts
- `doc/`: active scope and workflow documentation

Historical scripts and plans are preserved under:
- `script/legacy/`
- `doc/legacy/`

## Paper Cohorts
1. Full Eligible Cohort
2. Downstream Evaluation Cohort
3. Adjudication Subset

## Active Workflow Map
1. `script/run_select_note_corpus.py`
2. `script/run_attach_full_note_text.py`
3. `script/run_candidates_overnight.py`
4. `script/run_stage2_overnight.py`
5. `script/run_rq1_step2_aggregate.py`
6. `script/run_rq1_step3_build_ehr_by_visit.py`
7. `script/run_rq1_step3b_build_timeline.py` (optional, only for windowed downstream concordance)
8. `script/run_build_adjudication_packets.py`
9. `script/run_join_adjudication_labels.py`
10. `script/run_rq1_step4_note_truth_eval.py`
11. `script/run_rq1_step4b_calibrate_pathb.py`
12. `script/run_rq1_step5_normalization_eval.py`
13. `script/run_rq1_step6_downstream_concordance.py`
14. `script/run_rq1_step5_make_outputs.py`

## Scope Guardrails
- The primary truth source is adjudicated note-grounded mention labeling.
- Baseline vs Path A vs Path B normalization is evaluated against adjudicated canonical labels.
- Downstream EHR concordance is secondary and should not be interpreted as extraction truth.

## Pre-Adjudication Validation
- Use `script/run_rq1_pre_adjudication_dryrun.py` for engineering diagnostics before adjudication is finalized.
- Pre-adjudication outputs are operational checks (coverage, abstention, rejection reasons), not final extraction performance claims.
- See `doc/pre_adjudication_validation_runbook.md` for runnable commands.
- See `doc/evaluation_contract.md` for frozen scoring/matching rules.
