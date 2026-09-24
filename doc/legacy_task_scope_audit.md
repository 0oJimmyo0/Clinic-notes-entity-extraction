# Legacy BIBM Pipeline Task-Scope Audit

Status: **code-derived, non-PHI scope audit; not a new evaluation or final gold-task freeze** (2026-09-24)

Purpose: show the clinician what the existing Stage-1/Stage-2 system was actually built to process before choosing the new primary gold denominator. This audit uses source code and the existing [legacy evaluation contract](evaluation_contract.md), not test notes or new gold labels. It does not prove the system's recall for any subgroup.

## Evidence from the current code

| Component | Observed behavior | Consequence for the new evaluation |
|---|---|---|
| Stage-1 patterns — [`run_candidates_overnight.py`](../script/run_candidates_overnight.py) | Loads treatment-action terms (start/stop/hold/dose change), discontinuation-reason terms, and treatment-context terms from lexicons; has an optional patient target-drug trigger. | Candidate selection is **context/action-triggered**, not unconditional medication-name detection throughout every note. |
| Stage-1 window and limits | Selects the sentence around each trigger (or a bounded fallback window), deduplicates by text/category, skips very short input, and can cap candidates per patient (`max_candidates_per_patient`, default 40). | A valid named medication can be missed if its sentence has no trigger, the input is truncated, or the cap/deduplication removes evidence. Measure these misses; do not explain them away. |
| Stage-1 text input | Uses the selected note-text column; the runner warns when a 2,000-character hard-cap signature is detected. | Full raw notes are a proposed **new primary input view** for independent validation, while the legacy 2,000-character representation remains a paired sensitivity. Record the exact column and version. |
| Stage-2 input — [`run_stage2_overnight.py`](../script/run_stage2_overnight.py) | Runs medspaCy on **candidate-span text**, not all raw-note text. Adds drug, action, and other entity lexicons; extracts drug terms and start/stop/hold/dose-change signals. | Stage 2 cannot rescue medications never sent by Stage 1. End-to-end RQ1 must separately report candidate recall. |
| Stage-2 context | Includes negation/uncertainty ConText rules and a coarse certainty output. The fallback action mapping groups some words such as `continue`/`resume` with `start`. | Do not assume negation or treatment action is reliable merely because a rule exists. Annotate assertion/action separately and evaluate action only as a secondary endpoint. |
| Legacy reference/evaluation — [`evaluation_contract.md`](evaluation_contract.md) | Scores candidate-packet/mention alignment using IDs or fallback joins and a constructed note-grounded reference; structured EHR is a secondary comparator. | This is not exhaustive independent raw-note medication NER truth. The new blinded gold evaluator must support candidate-negative notes and mentions never in packets. |

## Intended task versus broader annotation

The most defensible **proposed primary RQ1 target** is patient-linked, explicitly named **treatment-relevant medication evidence**: current use/continuation, a stated start/stop/hold/same-drug regimen change, a firm patient-specific treatment plan, or an explicit statement that the patient is or is not taking a named drug. The clinician must decide whether tentative consideration and context-poor current medication-list entries belong in this primary target. Do **not** automatically exclude negated use statements; they may be central to note–structured disagreement.

Annotators should nevertheless mark a **broader patient-linked mention universe** in full raw notes, including remote historical named drugs, so recall outside the primary target can be measured as a prespecified **secondary sensitivity**. Keep planned/considered, negated, historical, class-only, and unresolvable identity strata visible. Allergy-only references, family-member medications, and general pharmacology are proposed outside the treatment target (with boundary examples in the annotation guide). The final primary/secondary/excluded assignment must be made **before** new gold scoring and after clinical review; it cannot be inferred from system predictions.

An endpoint of “every medication mention anywhere in the note” would be broader than the current code's candidate-selection design. It is valid as a sensitivity analysis but should not silently replace the primary treatment-relevant task. Conversely, the task boundary must not be narrowed *after* inspecting performance merely to protect F1. The paper should report both the primary task and broader sensitivity transparently, including the effect of the candidate-stage ceiling.

## Questions the clinician and analysis team must settle before freeze

1. What textual evidence ties a named drug to this patient's **current or intended treatment** rather than remote history, a copied list, hypothetical discussion, allergy, or another person?
2. Are explicit “not taking X” statements primary treatment-relevant evidence? The proposed answer is yes; confirm clinically.
3. Are tentative “may consider X” statements primary, secondary, or excluded? Distinguish them from a committed plan.
4. Should a named medication in a current med list count when there is no local action cue? If so, evaluate the expected Stage-1 misses honestly.
5. For each endpoint, how are class-only and unresolvable mentions and system predictions on non-primary mentions counted? Freeze this in `gold_evaluation_contract.md`.
6. What rule distinguishes **same medication event** from merely related drugs or nearby dates in RQ3 human review?

## Scope and freeze caveats

This document describes the checked runner code, not a claim that every historical output was produced by precisely these files or a formal model freeze. Before RQ1 scoring, record the exact invoked scripts, lexicon files, alias artifacts, candidate cap, target-drug option, note-text column, Git commit/hashes, and any wrapper/configuration differences in the resource chronology. If those differ from this audit, revise the *pre-gold* scope statement and record why. No new patient data or performance results were used here.
