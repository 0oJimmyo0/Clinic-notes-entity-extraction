# Clinician Review Handoff: JAMIA Open Medication-Evidence Study

Status: **methods review only; no clinical-note access or pilot annotation authorized by this document**  
Prepared: 2026-09-24

## What we need from you now

Please help us make the *clinical definitions* of the independent raw-note evaluation usable and defensible. The study asks whether a frozen pipeline can identify and normalize medication evidence in clinic notes, then how that evidence relates to structured medication history. Medication identity is the primary NLP endpoint; treatment action is secondary. Your review should focus on what counts as a medication mention, how specific its identity must be, and whether the planned comparisons have clinical meaning. We are **not** asking you to certify IRB status or infer which patients earlier investigators inspected.

Please return one completed [clinician review form](clinician_review_response_template.md) with tracked comments or specific section references. A short discussion after the written review is useful, but the decisions must be recorded in the form before we freeze the protocol.

An earlier [methods-review draft](methods_review_status.md) has been summarized as **provisional recommendations only**. Its author/credentials are not verified here; it reviewed no patient notes and does not replace your own clinical judgment or sign-off. Please assess the proposed rules independently.

## Send now: small, PHI-free review packet

| Priority and file | How to review it | What we expect back |
|---|---|---|
| **1. This handoff** | Confirm the clinical-review scope, your proposed role, and whether you can later participate in a secure pilot. Do not begin patient-level review. | Role/availability and any scope concern in the review form. |
| **2. [Provisional raw-note annotation guide](raw_note_annotation_guide_draft.md)** | Review every proposed inclusion/exclusion rule and orthogonal field. Confirm or revise patient-linked scope, allergy-only, class-only, historical/planned/negated, combinations, and action-versus-status decisions. Synthetic examples are starting points, not signed-off rules. | A marked-up guide or section-specific accept/change/keep-open decisions, plus missing synthetic examples. |
| **3. [Gold evaluation contract](gold_evaluation_contract.md)** | Check the primary target and **denominators** (especially class-only/unresolvable mentions), ingredient-set identity, and span matching. Mark what must remain open until the pilot; do not fill `OPEN` fields by guessing. | Proposed primary/secondary endpoint boundaries and unresolved scoring decisions in the review form. |
| **4. [Sampling-frame audit](validation_sampling_frame.md)** | Read the aggregate findings and A/B/C/D/Unknown definitions. Distinguish automated packet generation from actual human review. If you personally saw these patients' notes/outputs or used them for development, tell the PI via a restricted channel rather than listing IDs in the review form. | Whether the distinction is clear; any first-hand exposure to report securely; questions for the PI/data owner. No patient IDs in the form. |
| **5. [Revision plan](revision_plan.md), “Decision,” “Priority 1,” and “Priority 3” only** | Check the three RQs, primary-versus-secondary outcomes, and whether semantic/category/time overlap could be misconstrued as the same treatment event. You need not review every implementation or journal-formatting section. | Top 3 scientific changes, if any; whether the proposed human relaxed-match categories are clinically usable. |
| **6. [Legacy adjudication schema](adjudication_schema.md), for comparison only** | Identify labels worth reusing. It starts from candidate packets and is **not** the new exhaustive raw-note gold guide. Note missing or overly broad definitions. | “Keep/change/drop” comments on its mention-status and canonical-label rules. |

The [review form](clinician_review_response_template.md) is the only file you need to edit for this first pass. You may add comments to the other Markdown files, but please return a consolidated decision record in the form. Do **not** send the restricted `validation_exposure_matrix.csv`, packet CSVs, full note text, audit sheets with contexts, or alias source files as ordinary email attachments. The PI/original developers—not a new clinician reviewer—must trace alias provenance and prior development use.

## After this review, only if institutional access is confirmed

1. The PI/data owner confirms the actual study authorization, complete-note access, and which investigators can review notes. Original reviewers/developers classify prior human/development exposure in the restricted patient matrix. At least 75–100 eligible patients must be documented before any gold draw.
2. We create a **separate 20–30-note pilot reserve** from eligible patients who will not enter the final gold or untouched replication sets. The pilot notes and annotation sheet remain in an approved secure environment. Do not email note text or upload it to GitHub/Overleaf/arXiv.
3. As a pilot annotator, review every raw note exhaustively, including notes with **zero** in-scope medications. Mark mention spans and canonical identity first; action and temporality are secondary. Record unresolvable cases instead of guessing. Track minutes per note and ambiguities. A second independent annotator must review a predeclared subset; a single clinician cannot supply inter-annotator agreement alone.
4. Return a **pilot-only summary**, not test performance: medication mentions per note, time per note, inter-reviewer disagreements, rule changes needed, and recommendation for the final sample size and per-patient note cap. The team then finalizes the guide/evaluator and freezes resources **before** the independent gold sample is drawn.

## Division of responsibility

- **Clinician reviewer:** clinical task boundary, identity/action definitions, pilot annotation, adjudication when appropriate, clinical interpretation of relaxed semantic/temporal matches.
- **PI/data owner:** IRB/data-use/consent facts, access authorization, secure workspace, determination of whether a new sample is allowed.
- **Original investigators/developers:** identify who viewed old packets/notes, link alias resources to source patients where possible, and attest whether candidate patients informed prompts, aliases, rules, thresholds, or model choices.
- **Analysis team:** apply the restricted exposure register, build the blinded sampling frame, lock code and matching rules, calculate design-aware metrics; no test-driven changes to the primary pipeline.

The immediate deliverable is the completed review form plus PI/developer answers through their appropriate channels. **Do not start pilot annotation while governance and exposure classification remain open.**
