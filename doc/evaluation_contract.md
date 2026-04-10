# Evaluation Contract (Frozen Before Final Adjudication)

This contract freezes scoring and linkage behavior before final adjudicated labels are completed.

## 1) Mention Match Rule

Primary key for adjudication join:
- adjudication_unit_id exact match

Fallback keys (in order):
1. note_id + span_id_or_local_reference + normalized raw_mention_text
2. note_id + normalized raw_mention_text (only when unique)

If multiple seed candidates match a fallback key:
- keep deterministic first row
- mark audit_flag for manual review

If no extracted seed row matches a reviewed row:
- alignment_status = false_negative

If extracted seed row is unmatched by reviewed rows:
- alignment_status = false_positive

Reference implementation:
- script/run_join_adjudication_labels.py

## 2) Canonical Label Match Rule

Canonical label correctness is strict normalized-string equality:
- normalize prediction and gold with normalize_drug_text
- correct iff normalized prediction == normalized adjudicated canonical label

Reference implementation:
- script/run_rq1_step5_normalization_eval.py
- script/rq1_drug_linking.py (normalize_drug_text)

## 3) Status Scoring Rule

Mention status values are constrained to:
- active_current
- newly_started_or_prescribed
- planned_or_considering
- discontinued_or_stopped
- held_or_paused
- historical_prior
- reference_only_or_discussion_only
- unclear

Primary status scoring:
- strict exact match on the above status labels

Optional sensitivity view:
- grouped status map from rq1_adjudication_utils.GROUPED_STATUS_MAP

Reference implementation:
- script/rq1_adjudication_utils.py
- script/run_rq1_step4_note_truth_eval.py

## 4) Potentially Comparable-to-EHR Statuses

Comparable-to-EHR eligibility is explicitly adjudicator-labeled using compare_to_structured_ehr:
- yes
- no
- uncertain

Status-aware guidance for likely comparability:
- likely comparable: active_current, newly_started_or_prescribed, planned_or_considering
- usually not comparable: historical_prior, reference_only_or_discussion_only, unclear
- context-dependent: held_or_paused, discontinued_or_stopped

Contract for downstream concordance inclusion:
- default include only compare_to_structured_ehr = yes
- optional sensitivity include uncertain via explicit flag

Reference implementation:
- script/run_join_adjudication_labels.py
- script/run_rq1_step6_downstream_concordance.py

## 5) Path B Acceptance Rule

Path B is an abstaining canonical-vocabulary linker.

Acceptance is allowed only when all checks pass:
- score >= pathb_min_score
- top1-top2 margin >= pathb_min_margin
- calibrated_confidence >= pathb_min_calibrated_confidence
- short-mention safeguards pass
- mention is not out-of-scope by status metadata

If any check fails:
- prediction is blank
- reason_codes are emitted

Reference implementation:
- script/rq1_drug_linking.py

## 6) Unmatched Mention Handling

For extraction-vs-truth alignment:
- unmatched reviewed mention -> false_negative
- unmatched extracted mention -> false_positive

For normalization:
- Path B abstention is a first-class outcome, not auto-failure conversion to a forced label
- unresolved terms are preserved for diagnostics and targeted vocabulary/alias curation

Reference implementation:
- script/run_join_adjudication_labels.py
- script/run_rq1_step5_normalization_eval.py
- script/run_rq1_pre_adjudication_dryrun.py
