# Pre-Adjudication Validation Runbook

This runbook is for engineering validation only.
Do not treat outputs here as final extraction performance claims.

## Current Runnable Pipeline Map

1. script/run_select_note_corpus.py
2. script/run_attach_full_note_text.py
3. script/run_candidates_overnight.py
4. script/run_stage2_overnight.py
5. script/run_rq1_step2_aggregate.py
6. script/run_rq1_step3_build_ehr_by_visit.py
7. script/run_rq1_step3b_build_timeline.py (optional)
8. script/run_build_adjudication_packets.py
9. script/run_join_adjudication_labels.py
10. script/run_rq1_step4_note_truth_eval.py
11. script/run_rq1_step4b_calibrate_pathb.py
12. script/run_rq1_step5_normalization_eval.py
13. script/run_rq1_step6_downstream_concordance.py
14. script/run_rq1_step5_make_outputs.py

## Pre-Adjudication Engineering Diagnostics (Baseline / Path A / Path B)

Run from repository root (resources):

python3 script/run_rq1_pre_adjudication_dryrun.py \
  --note-csv ../episode_extraction_results/rq1/rq1_note_entities_by_visit.csv \
  --subset-csv ../episode_notes/manifests/adjudication_subset_manifest.csv \
  --subset-person-col person_id \
  --subset-visit-col visit_occurrence_id \
  --alias-artifact lexicons/rq1_drug_aliases.csv \
  --canonical-vocab-path lexicons/rq1_drug_canonical_vocab.csv \
  --pathb-calibration-json ../episode_extraction_results/rq1/diagnostics/rq1_drug_linker_calibration.json \
  --output-dir ../episode_extraction_results/rq1/pre_adjudication_dryrun

Key outputs:
- rq1_preadj_dryrun_summary.json
- rq1_preadj_method_diagnostics_detailed.csv
- rq1_preadj_transition_counts.csv
- rq1_preadj_pathb_rejection_reasons.csv
- rq1_preadj_top_unresolved_raw_mentions.csv
- rq1_preadj_patha_alias_hits.csv
- rq1_preadj_patha_suspicious_alias_mappings.csv

## Adjudication Packet Quality Checks

python3 script/run_build_adjudication_packets.py \
  --adjudication-subset-csv ../episode_notes/manifests/adjudication_subset_manifest.csv \
  --candidate-csv ../episode_extraction_results/archive_candidates/all_candidates_combined.csv \
  --stage2-csv ../episode_extraction_results/archive_stage2/extracted_treatment_data_episode_cleaned.csv \
  --notes-parquet ../episode_notes/subcohort_patient_complete/notes.parquet \
  --output-dir ../episode_extraction_results/rq1/adjudication_packets \
  --min-context-chars 40

Key outputs:
- adjudication_packets_diagnostics.json
- adjudication_packets_mentions_per_note.csv
- adjudication_packets_mentions_per_visit.csv

## Post-Adjudication Command Sequence (When Reviewed Labels Are Ready)

1) Join reviewed adjudication labels
python3 script/run_join_adjudication_labels.py \
  --packets-mentions-csv ../episode_extraction_results/rq1/adjudication_packets/adjudication_packets_mentions.csv \
  --reviewed-adjudication-csv <REVIEWED_LABELS_CSV> \
  --alias-artifact lexicons/rq1_drug_aliases.csv \
  --canonical-vocab-path lexicons/rq1_drug_canonical_vocab.csv \
  --output-dir ../episode_extraction_results/rq1/adjudicated

2) Note-truth extraction evaluation
python3 script/run_rq1_step4_note_truth_eval.py \
  --joined-mentions-csv ../episode_extraction_results/rq1/adjudicated/rq1_extraction_vs_truth_mentions.csv \
  --visit-manifest-csv ../episode_notes/manifests/adjudication_subset_manifest.csv \
  --note-manifest-csv ../episode_notes/manifests/adjudication_note_manifest.csv \
  --output-dir ../episode_extraction_results/rq1/note_truth_eval

3) Normalization evaluation (baseline vs Path A vs Path B)
python3 script/run_rq1_step5_normalization_eval.py \
  --adjudicated-mentions-csv ../episode_extraction_results/rq1/adjudicated/rq1_adjudicated_mentions.csv \
  --alias-artifact lexicons/rq1_drug_aliases.csv \
  --canonical-vocab-path lexicons/rq1_drug_canonical_vocab.csv \
  --output-dir ../episode_extraction_results/rq1/normalization_eval

4) Refit/validate Path B calibration on leftovers
python3 script/run_rq1_step4b_calibrate_pathb.py \
  --normalization-detailed-csv ../episode_extraction_results/rq1/normalization_eval/rq1_normalization_eval_detailed.csv \
  --output-dir ../episode_extraction_results/rq1/pathb_calibration

5) Downstream concordance (secondary)
python3 script/run_rq1_step6_downstream_concordance.py \
  --downstream-comparable-mentions-csv ../episode_extraction_results/rq1/adjudicated/rq1_downstream_comparable_mentions.csv \
  --ehr-csv ../episode_extraction_results/rq1/rq1_ehr_entities_by_visit.csv \
  --timeline-csv ../episode_extraction_results/rq1/rq1_visit_timeline.csv \
  --windows 0 \
  --output-dir ../episode_extraction_results/rq1/downstream_concordance

6) Paper-ready outputs
python3 script/run_rq1_step5_make_outputs.py \
  --cohort-summary-json ../episode_notes/manifests/cohort_justification_summary.json \
  --note-truth-summary-json ../episode_extraction_results/rq1/note_truth_eval/rq1_step4_note_truth_summary.json \
  --adjudicated-mentions-csv ../episode_extraction_results/rq1/adjudicated/rq1_adjudicated_mentions.csv \
  --normalization-summary-json ../episode_extraction_results/rq1/normalization_eval/rq1_normalization_eval_summary.json \
  --normalization-detailed-csv ../episode_extraction_results/rq1/normalization_eval/rq1_normalization_eval_detailed.csv \
  --pathb-calibration-summary-json ../episode_extraction_results/rq1/pathb_calibration/rq1_pathb_calibration_summary.json \
  --pathb-confidence-bands-csv ../episode_extraction_results/rq1/pathb_calibration/rq1_pathb_confidence_bands.csv \
  --downstream-summary-csv ../episode_extraction_results/rq1/downstream_concordance/rq1_similarity_summary.csv \
  --pathb-leftovers-csv ../episode_extraction_results/rq1/adjudicated/rq1_pathb_leftovers.csv \
  --status-confusion-csv ../episode_extraction_results/rq1/note_truth_eval/rq1_step4_status_confusion.csv \
  --output-dir ../episode_extraction_results/rq1/paper_outputs
