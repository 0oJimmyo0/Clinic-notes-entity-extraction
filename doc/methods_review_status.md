# Provisional Methods-Review Status and Decision Log

Date recorded: 2026-09-24  
Source: user-supplied methods-review text, authorship/credentials not independently verified in this repository  
Status: **PROVISIONAL — no real notes reviewed; no clinician sign-off; no independent annotation or inter-rater assessment**

The supplied response explicitly describes itself as biomedical-informatics/clinical-methods advice and states that it cannot serve as the human clinician of record, certify institutional access, or act as an authorized annotator. It may guide protocol drafting, but **must not** be called a completed clinician review, patient-note audit, independent human gold standard, or IRB/consent determination. If the text was generated or materially drafted by AI, the team should record and disclose that use according to the target journal's current policy. The response itself contains only synthetic examples; no patient-level data are copied here.

## Recommendations carried into draft documents

| Recommendation | Draft location | Still open before freeze |
|---|---|---|
| Treat medication identity as primary; action secondary | `revision_plan.md`, `gold_evaluation_contract.md` | Clinician confirms clinical target and pilot reliability |
| Define patient-linked evidence and separate historical, planned, negated, class-only, allergy-only, and general discussion | `raw_note_annotation_guide_draft.md` | Clinician/pilot chooses primary versus secondary denominators, checked against legacy pipeline scope |
| Split legacy `mention_status` into identity, temporality, assertion, and action | `raw_note_annotation_guide_draft.md` | Confirm annotator workload/agreement and final schema |
| Use complete ingredient set for fixed combinations and unambiguous brand/generic mapping; no forced RxNorm code | `raw_note_annotation_guide_draft.md`, `gold_evaluation_contract.md` | Freeze terminology release and salt/form equivalence |
| Use minimal identity-bearing spans; evaluate strict boundaries separately if relaxed scoring is chosen | `gold_evaluation_contract.md` | Freeze allowable extra text, duplicate/tie rules, and ambiguous prediction handling |
| Require more than category similarity or date proximity to call matches the same clinical medication event | `revision_plan.md`; downstream match-review guide still needed | Clinician-reviewed RQ3 guide and blinded match-validation pilot |
| Annotate broadly but use a treatment-relevant named primary target and broader named-mention sensitivity, reflecting the action/context-triggered legacy pipeline | `legacy_task_scope_audit.md`, `raw_note_annotation_guide_draft.md`, `gold_evaluation_contract.md` | Qualified clinician and pilot settle exact boundaries and prediction FP treatment before gold scoring; broad misses remain visible |
| Resolve previous patient exposure with investigator artifact/batch attestations and restricted patient-linked exceptions | `investigator_exposure_attestation_template.md`, `validation_sampling_frame.md` | Signed/factually verified coverage from relevant investigators; governance and patient eligibility still unconfirmed |

## Unresolved decisions (do not mark as complete based on this review)

1. A qualified human clinician reviews/signs the final clinical definitions; a second independent, authorized annotator is designated for double coding.
2. The PI/data owner confirms the factual study authorization, full-note access, and secure annotation workflow. Original investigators resolve patient human/development exposure.
3. The team chooses the primary target and exact denominators **before** final gold scores: whether historical/planned/negated mentions enter headline detection; affirmative/current subgroup; class-only and unresolvable identity handling; allergy-only boundary; prediction FP treatment on those spans.
4. The team freezes one-to-one span/identity matching, strict-boundary diagnostic, terminology release, RxNorm policy, and the sampling estimator. The provisional proposal that a relaxed prediction cover all identity-bearing tokens still needs a maximum-extra-text rule.
5. An authorized pilot measures annotation time, mention density, inter-reviewer disagreement, and copy-forward/repeated-mention burden. The methods draft's 8--12 minute typical-note estimate is **unverified**.
6. The separate human validation of broad semantic/temporal matches needs its own examples, adjudication rules, and reliability check; the present review does not supply case-level evidence.

## Decision rule

Until items 1--4 are resolved and the pilot is completed, the correct description is **“provisional protocol informed by a methods-review draft.”** Do not cite this review as a human-validated result in the manuscript. No gold sample or performance claim follows from it.
