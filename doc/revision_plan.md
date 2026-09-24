# JAMIA Open Revision Plan

Status: pre-freeze study protocol for a new JAMIA Open submission after BIBM 2026 rejection; no new experiments completed by this document

Last reviewed: 2026-09-24

## Decision

Preserve the BIBM study's core question and most of its methods/results, but change the evidence hierarchy. This is a scientific validation and calibration revision, **not** a new medication-event extraction project, a medication-NER leaderboard, or merely a longer conference paper. Reuse is a planning aim, not a promised percentage of unchanged text.

**Cohort decision rule:** Do not expand or pool the 761-patient BIBM characterization cohort merely to increase N. First establish end-to-end validity on an independently annotated raw-note sample. If that succeeds, use the remaining eligible, untouched patients for a **separate within-institution replication cohort** when feasible. Pool cohorts only for a predeclared question that a separate comparison cannot answer, and retain cohort-specific estimates. More rows alone cannot remove candidate-selection bias or reference circularity.

The revised paper should answer three research questions, in this order. Every planned experiment must answer one or be dropped:

1. **RQ1 — Independent validity:** How accurately does the frozen note-side pipeline identify and normalize patient-linked, treatment-relevant named medication evidence in previously unused raw clinic notes? Primary evidence: candidate recall; end-to-end normalized precision, recall, and F1 with patient-clustered intervals; same-gold comparators. Report broader patient-linked named-mention detection on the same notes as a prespecified sensitivity.
2. **RQ2 — Mechanism:** Which components account for normalization performance? Evidence: surface exact -> lexical cleanup -> public terminology -> development-only aliases, including the historical conditional 0.7226 -> 0.7749 -> 0.8429 ablation after denominator verification. Historical conditional accuracy is not RQ1 performance.
3. **RQ3 — Cross-source characterization:** After note-side validity is established, how does note-derived medication evidence relate to structured medication history across semantic specificity and time? Evidence: semantic x temporal matrix, human review of relaxed matches, permuted-history null, and optional separate same-institution replication.

Proposed central message, contingent on the new validation results:

> Independent raw-note validation separates note-side extraction and normalization error from semantic and temporal differences in how clinic notes and structured medication history represent medication evidence.

Medication **identity/normalization** is the primary NLP task. Treatment action is annotated where feasible but remains a secondary, exploratory endpoint until independently shown reliable. Prefer “note-derived medication evidence” over “treatment-context medication evidence” in the title and central claims. The central message must be weakened if the gold set, relaxed-match review, or negative control does not support it.

Working title direction: *Characterizing Medication Evidence Across Clinical Notes and Structured EHR Data with Independent Note-Level Validation*. Final title follows the results; do not imply an action-extraction capability that the gold evaluation does not establish.

### Governing response matrix

| BIBM vulnerability | JAMIA Open response | Existing work retained |
|---|---|---|
| Constructed reference and candidate ceiling | Blinded, broad raw-note annotation; measure primary and broader-mention candidate recall, including candidate misses | 27,752-row resource for large-cohort characterization |
| Conditional 84.29% presented like system accuracy | Primary end-to-end normalized P/R/F1; conditional ablation labeled separately | 0.7226 -> 0.7749 -> 0.8429 ablation, after ledger verification |
| Limited independent adjudication and patient dependence | Double-code 20--30%; patient-clustered intervals | 1,000-row audit as resource calibration |
| Alias/reference circularity | Dated resource chronology and freeze before gold scoring | Existing deterministic pipeline and development-only alias machinery |
| MedXN denominator/adaptation asymmetry | Same raw notes, gold denominator, and task definitions | Existing MedXN implementation; old comparison as historical only |
| Weak action attribution | Secondary/exploratory per-class analysis; narrow claims if weak | Existing action labels for descriptive context |
| Broad/late overlap may be incidental | Semantic x temporal matrix, human review, and permuted-history null | Existing cross-source ladder and ontology work |
| Innovation framed as another normalizer | Present an evidence-calibration framework | Existing cohort, motivation, methods, and limitations |

## Repository and data audit

The arXiv/Overleaf repository is only the presentation layer. The analysis repository is the adjacent project workspace:

- manuscript: `6a6b9d18cee075ba27501107/`
- analysis resources and scripts: `resources/`
- active cohort: `episode_notes/subcohort_clinic_like_20k_30k/`
- active RQ1 outputs: `episode_extraction_results/clinic_like_20k_30k/rq1/`

The active clinic-like cohort currently contains:

- 761 patients;
- 11,812 visits;
- 22,483 notes;
- 27,752 constructed reference medication mention rows (not an independent human gold standard);
- 3,188 targeted human-audited rows;
- 150 previously held-out test patients, with 1,015 reference-supported visits, 1,422 reference-supported notes, and 5,403 reference mention rows. The full raw-note universe for these patients is larger; these figures must not be used as its sampling frame.

The active note corpus has an important context limitation: 13,067 of 22,483 notes (58.12%) have the 2,000-character maximum-length signature. Full-note material is available locally at `episode_notes/clinic_only_fulltext_chunks/`, and the pipeline already supports a `note_text_full` column. This must be measured and reported rather than silently ignored.

**Independent-sample feasibility check (updated 2026-09-24):** `episode_notes/manifests_clinic_only/evaluation_note_manifest.csv` lists 2,029 patients. Of these, 1,276 are outside the 761-patient BIBM cohort, with 23,211 clinic-like note rows. The [sampling-frame audit](validation_sampling_frame.md) finds that 1,261 appear in prior packet/review-bridge artifacts; adding the separate automated selection manifest gives **1,275 with some checked automated exposure** and one with no exact-ID trace in those sources. Crucially, no out-of-BIBM patient has an exact-ID match in the checked completed human-review or patient-linked alias-source files. This does **not** certify all 1,275 as automated-only: manual alias resources lack patient IDs, other files may exist, and investigators may have inspected notes outside these artifacts. Packet presence alone is not a disqualifier. Full-text parquet keys exist for all 23,211 notes, which does not prove usable/full/authorized text. Governance and human/development-exposure classification must precede any pilot or gold draw. Conversely, only five BIBM-cohort patients are outside the prior 606/150 patient split, so the existing BIBM test patients cannot furnish 75--100 genuinely untouched patients. Treat prior BIBM test results as retrospective characterization, not pristine independent validation.

Use separate, non-pooled roles: the **BIBM characterization cohort** (761 patients, large constructed reference, prior held-out analysis); a **pilot reserve** (~20--30 notes from patients excluded from final validation and replication); an **independent gold validation subset** (target ~75--100 eligible unused patients, 300--500 raw notes); and, only if justified, a **remaining-patient replication cohort**. The old held-out split remains useful for historical reproducibility and directional comparison. The validation and replication groups are both from the same institution; do not call either external, prospective, or independently untouched without supporting provenance.

Relevant existing assets include:

- patient-level development/test split: `rq1/heldout_split/`;
- development-only alias supplement machinery: `run_rq1_build_dev_alias_supplement.py` and `run_rq1_holdout_alias_supplement_eval.py`;
- adjudication schema and packet workflow: `doc/adjudication_schema.md`, `run_build_adjudication_packets.py`, and `run_join_adjudication_labels.py`;
- extraction and normalization evaluation: `run_rq1_step4_note_truth_eval.py` and `run_rq1_step5_normalization_eval.py`;
- semantic/temporal matching: `run_rq1_temporal_mismatch_ladder.py` and `run_rq1_step6_downstream_concordance.py`;
- OMOP/RxNorm mapping artifacts: `rq1/omop_rxnorm_mapping/` and `rq1/ontology_gap_audit/`;
- existing action- and drug-class-stratified outputs: `rq1/temporal_mismatch_ladder/` and `rq1/note_only_evidence_bibm_test/`;
- existing MedXN and classical comparator outputs, which should remain secondary.

Raw clinical notes and structured extracts are restricted project data. Do not place raw note text, patient identifiers, annotation packets, or unrestricted extracts in the manuscript repository, Overleaf, arXiv, or a public supplement.

## Gate 0: audit the baseline and freeze the primary evaluation before gold testing

This is mandatory. Existing artifacts require an explicit denominator and provenance ledger before new interpretation; different populations must not be mislabeled as conflicting estimates.

Examples requiring reconciliation:

- the current manuscript reports held-out normalization values of 0.7226 to 0.8429, while the enriched output bundle reports 0.721353 to 0.845777 on the full 27,752-row reference set; these are different populations, so label both explicitly;
- `reference_audit_bibm_test` reports 27,552 reference rows, while the current manuscript and other outputs use 27,752;
- the 1,000-row random audit reports canonical agreement 0.92096 and action agreement 0.53265, while the separate 300-row BIBM-completed artifact is a different sample and should not be treated as the primary audit;
- the temporal ladder is generated by code with fixed same-visit, +/-30-day, +/-90-day, and any-history checks, not the proposed 7/14/30/60/90-day sensitivity curve.

Before interpreting any new result:

1. Select the canonical input snapshot and record its generation dates, Git commits, seeds, cohort manifests, alias artifacts, ontology version, and structured-data version. Preserve the original BIBM results as immutable historical artifacts.
2. Preserve and audit the current BIBM pipeline outputs. Re-run into a new revision output root only where needed to resolve provenance or regenerate manuscript numbers; do not overwrite BIBM outputs and do not put a full legacy rerun ahead of the new gold evaluation without a specific reason.
3. Create a metric ledger containing every manuscript number, its exact source file, denominator, unit of analysis, patient/visit split, and formula.
4. Trace the 27,552 versus 27,752 row difference; document, rather than conflate, the held-out 0.8429 and full-cohort 0.845777 estimates.
5. Use the 1,000-row audit as the current supporting reliability audit only if its sampling and adjudication provenance are confirmed. Retain the 300-row artifact as historical/secondary, not as the headline result.
6. Verify that every reported result can be regenerated without absolute `/Users/...` paths. Use the [code-derived legacy task-scope audit](legacy_task_scope_audit.md) to document the action/context-triggered candidate design, then freeze candidate rules, lexical cleanup, base aliases, development-only aliases, action rules, terminology versions, and evaluator **before gold-test scoring**. Record each resource's creation date, source patients, allowed use, hash, and code commit. No gold-test inspection may feed back into this locked primary run.
7. Verify whether full note text exists for the proposed validation frame. Predefine whether the primary system receives complete raw notes or the legacy 2,000-character view; keep the other as a separately labeled sensitivity analysis. Report truncation by sample and error stratum.

**Gate 0 acceptance criterion:** every reused baseline number has a traceable input, denominator, and code/resource version; any required rerun agrees with the manuscript ledger. The locked code/resource manifest precedes gold-test scoring. A legacy rerun must not delay the gold study once its inputs and denominators are reliably documented.

## Priority 1: independent human gold evaluation

This is the highest-value revision and is not equivalent to the existing random audit. The existing 1,000-row audit samples rows from the unaudited LLM-bootstrapped reference set; it estimates reference reliability but cannot adequately measure system false negatives or define an independent gold standard.

### Recommended design

After the 1,276-patient candidate frame is audited, reserve **20--30 raw notes from separate patients** for annotation feasibility only. Use the reserve to estimate medication-mention density, time per note, and whether action labels are interpretable. It may inform the guide and whether the final target should be closer to 300 or 500 notes, but supplies **no final performance estimate**; its patients are excluded from the final validation and any untouched replication cohort. Pilot on old development notes instead if patient provenance or governance makes a new-pool reserve inappropriate.

Then freeze the annotation guide, system resources, sampling plan, matching rules, and analysis code. Draw **300--500 raw clinic notes from approximately 75--100 eligible, genuinely uninspected patients** in the broader clinic-only manifest, subject to the provenance and access check above. Use a patient-level, prespecified probability sample from the raw-note frame, not candidate rows, packet notes, or the previously analyzed BIBM test split. Retain all selected notes, including those with no pipeline candidate. If the broader pool is not truly unused or accessible, acquire an approved new temporal/patient slice; otherwise label the exercise retrospective and do not claim independent validation. Record inclusion probabilities and exclusions.

Predefine a **two-stage probability design**: select eligible patients first, then a bounded number of notes per selected patient. Choose the per-patient note cap after examining frame distributions and pilot workload (planning range 3--6); freeze it before the gold draw so one prolific patient cannot dominate. Record inclusion probabilities at both stages and state whether the headline estimand is note-weighted or patient-weighted. Patient-clustered intervals must also respect the sample design. Keep 75--100 patients as the cluster-diversity target, but let pilot medication-mention density and projected F1 uncertainty inform whether the sample needs nearer 300 or 500 notes. Do not claim a particular CI width without those pilot estimates.

For the **primary** sample, stratify only on prespecified source metadata available before model output (for example date, note type, length, and patient visit burden), and preserve sampling weights or report design-specific estimates. Separately build an **action/difficulty challenge set** if rare classes need more cases; never pool it into the headline accuracy. Ensure coverage of:

- notes later found to have candidates and those found to be candidate-negative;
- different note types and lengths, including potentially truncated notes;
- patients with varying note and visit burden.

**Primary input is the full raw clinic note**, if source-text access and governance permit it; annotators and all comparators see that same input. Evaluate the legacy 2,000-character view on the **same gold notes** as sensitivity analysis, counting valid gold mentions beyond the truncation boundary as misses, not removing them from the denominator. If full text is unavailable, document why and amend the target input/population before gold sampling. Never call a truncated-input study “full-note” extraction.

Annotators see the raw note only: no Qwen output, medspaCy/pipeline predictions, candidate list, projected labels, project alias maps, or structured medication history. Create immutable note IDs and a blinded annotation export. The [raw-note guide](raw_note_annotation_guide_draft.md) contains **provisional** methods-review recommendations; it is not a clinician-approved or pilot-tested protocol. Use separate, orthogonal fields:

- **Required primary labels:** patient-linked mention validity, minimal drug-identity span offsets, identity resolvability, canonical ingredient or complete unordered ingredient set when resolvable, and an explicit unresolvable/uncertain identity code. RxNorm is recorded when a frozen release gives an unambiguous link, not forced for every valid mention.
- **Proposed lightweight context labels for each mention, pending clinician/pilot confirmation:** temporality (current, planned, historical, unclear) and assertion (affirmed, negated, uncertain). Keep these separate from identity.
- **Secondary action label:** start, stop, hold, continue, explicit same-drug regimen change, discussed, or unclear. Do not infer a new action from list presence; a drug switch is provisionally stop old drug plus start new drug.
- **Optional/contextual labels:** certainty for conditional plans and short rationale for ambiguous cases; do not require free-text rationales for every routine mention.

The clinician and pilot must settle how historical, negated, planned, discussed, allergy-only, and class-only mentions enter **each** endpoint. The [legacy task-scope audit](legacy_task_scope_audit.md) shows that candidate selection is action/context-triggered, not exhaustive drug-name scanning. Therefore, **provisionally annotate broadly but score hierarchically**: primary patient-linked, treatment-relevant **named** medication evidence (current use/continuation, explicit treatment action, firm patient-specific plan, or explicit taking/not-taking statement); broader explicit patient-linked named mentions, including remote history, as a prespecified secondary sensitivity; and separate clinical/identity strata. Do not automatically exclude negated use statements. The clinician must decide tentative consideration and context-poor copied/current medication lists; allergy-only/general discussion are provisionally outside the treatment target, while class-only/unresolvable mentions receive no ingredient-level identity credit but are counted and reported. **Check compatibility with the legacy pipeline's intended target on development data before choosing the primary denominator.** Freeze the final boundary and FP treatment in `doc/gold_evaluation_contract.md` before gold scoring; do not narrow it after seeing performance. Clinical correspondence with structured EHR is reviewed later, separately.

Double-code 20--30% independently, including examples from every predeclared stratum, then adjudicate disagreements. A methods-review draft—however useful—does not count as a second annotator or clinical sign-off. Designate two authorized independent reviewers; use ~15--20 synthetic/development examples for training, then revise the guide using the **separate pilot reserve or development notes** and freeze it before gold-test annotation. Report span agreement/F1, canonical-label agreement, and action-label kappa or an appropriate prevalence-aware alternative. Correspondence with structured history is a **separate later review**, not part of the blinded note-gold annotation. If action or temporality agreement is low, qualify or remove those claims rather than silently recoding the gold set after scoring.

### Gold-set metrics

Report a sequential error cascade:

1. candidate-generation recall among **all human gold mentions in the prespecified primary treatment-relevant named target**, explicitly separating misses in candidate-negative and candidate-positive notes; report recall among **all broadly annotated patient-linked named mentions** as a separate sensitivity with its own denominator;
2. span validity;
3. canonical medication identity conditional on valid evidence;
4. primary end-to-end normalized medication mention precision, recall, and F1 on all sampled raw notes;
5. secondary note-level medication-set performance and exploratory action attribution, including prevalence, per-class metrics, macro-F1, and majority baseline where support permits.

Freeze mention matching, duplicate handling, drug-identity semantics, uncertainty handling, note-level set construction, and sampling estimand in `doc/gold_evaluation_contract.md` **before anyone sees gold-test performance**. The primary confidence intervals must resample patients, not mentions, while respecting the two-stage design/weights. Keep the legacy 1,000-row audit supporting only: 582/1,000 valid/evaluable, 536/582 correct canonical labels conditional on validity, and 536/1,000 both valid and correct. The 1.25% targeted queue is failure-enriched and not a population estimate.

This separates medication-identity reliability from treatment-action interpretation, rather than treating the current 0.92096 canonical audit result and 0.53265 action result as interchangeable. Poor action performance narrows the claim; it does not invalidate a well-supported identity and cross-source study.

### Reuse and required script work

Reuse `run_rq1_reference_random_audit_sample.py`, `run_rq1_reference_random_audit_results.py`, and the existing annotation conventions, but create a new independent annotation template. Extend the schema rather than silently overloading `mention_status` or `seed_treatment_action`. Add a gold-set evaluator that supports missing system candidates and false-negative review.

## Priority 2: leakage-safe normalization evaluation

The current patient split is useful and should remain frozen: seed `20260622`, 606 development patients and 150 historical test patients. The **primary new validation system** must use public terminology, pre-gold general rules, and aliases demonstrably derived only from the 606-patient BIBM development side. This gives a clean development-resource -> frozen-model -> entirely new-patient test chronology. Do not quietly promote aliases learned from the old 150-patient test or new validation pool into the primary system.

Report the following normalization stages on the same **new independent raw-note gold sample**, with the original patient split used only for resource development and legacy comparison:

1. surface-exact matching;
2. generic lexical normalization;
3. public OMOP/RxNorm terminology only;
4. development-derived aliases, built only from development patients and frozen before test scoring;
5. the full project-curated alias map as an explicitly labeled upper-bound/sensitivity analysis.

The development-only machinery already exists. It currently produces 39 effective alias pairs from 47 development review rows and resolves 11 test review rows. The existing 29-entry base map and 83-entry v2 map must be given explicit provenance; they must not be described as leakage-safe merely because a separate supplement is safe.

Add a provenance audit for every alias pair:

- source artifact and date;
- public terminology versus project review;
- development/test patient eligibility;
- whether it was present before the split;
- conflict status and one-to-one mapping status.

The main text should lead with the development-only result. The full historical project map belongs in sensitivity analysis **only if no new-test information entered it**; explicitly label any old-test-derived aliases. Also run the locked pipeline, MedXN, public-terminology baseline, and one generic lexical baseline on the **same complete raw-note set and human-gold denominator**. Predeclare how each output maps to the common span/identity/action schema and report unsupported outputs as such; retain the old MedXN-output-conditioned comparison only as historical/supplementary. Do not restart Path B or add several generic fuzzy baselines unless the gold results show a clear need. Current Path B results show high abstention and accepted-link precision below the intended conservative target.

## Priority 3: temporal sensitivity and semantic-match validation

### 3.1 Temporal-window sensitivity

Replace the current discrete reporting with cumulative windows:

`same visit -> +/-7 days -> +/-14 days -> +/-30 days -> +/-90 days` (optional +/-60 days and any-history sensitivity)

For each window, report a **semantic x temporal matrix** of cumulative correspondence at exact drug, ingredient, and therapeutic-category levels; no single broad-window percentage is the lead result. Also report the nearest matched structured-record lag distribution. Keep the mutually exclusive ladder as a secondary display so readers can distinguish first-match buckets from cumulative coverage. State whether this characterization uses the newly adjudicated gold notes or the legacy large reference cohort; do not transfer performance claims between them.

The current `run_rq1_temporal_mismatch_ladder.py` hard-codes 30 and 90 days. Extend it with a parameterized window list and a deterministic matching contract. Do not change the meaning of same-visit matching while adding date windows. Ensure structured exposure start/end dates and visit anchors are documented.

### 3.2 Human validation of the reconciliation ladder

The observed ladder currently contains 4,563 exact-label overlaps, 16 same-visit ingredient-only cases, 10,732 same-visit category-only cases, 9,761 first matches within +/-30 days, 671 within +/-90 days, 907 any-history matches, and 1,102 residual no-overlap rows. These numbers establish the descriptive phenomenon but do not establish clinical correspondence.

Review a feasible stratified sample focused on **same-visit category-only and temporal-only matches** (target roughly 100 each, with exact targets finalized from available counts), plus selected exact/ingredient and no-overlap controls. The legacy ladder contains only 16 same-visit ingredient-only cases, so do not specify an impossible quota of 50. Report the sampling frame, bucket counts, and any review weighting.

Reviewers should classify **same clinical medication event / clinically related but different event / unrelated / uncertain**, with temporal plausibility and reason. Blind them to the pipeline's proposed explanation where feasible. Report human-confirmed fractions and uncertainty by bucket. Do not call the 90%+ broadened overlap “reconciliation” without this validation.

### 3.3 Negative control

Permute structured histories across different patients while preserving the note event, structured-record burden, approximate visit timing, and temporal-window procedure. Use a preregistered fixed-seed count (target 1,000, at least 100 if computationally constrained), excluding the true patient and preferably matching on visit/medication burden strata.

Compare observed versus permuted overlap for exact, ingredient, category, and each temporal window. If the observed-minus-null separation is small for broad categories or long windows, narrow the claim accordingly. This is a key safeguard against incidental overlap from common medications and permissive matching.

### 3.4 Conditional within-institution replication (not cohort expansion)

Only **after** the independent gold analysis has quantified candidate recall and end-to-end normalized extraction, decide whether the remaining eligible untouched patients can support replication. If major classes of note medication evidence are systematically missed, first revise or narrow the characterization claim; running the same biased pipeline on more patients does not repair it. Any post-gold pipeline repair creates a new version that needs a new untouched evaluation set or must be labeled exploratory.

If the gate passes, run the **locked** pipeline and matching rules on the remaining eligible patients as a separate cohort. Exclude pilot-reserve and gold-validation patients from this analysis. Preserve the original 761-patient BIBM cohort as-is. Predefine one common estimand and denominator for each comparison (for example note mentions versus visit-level sets); harmonize note length/full-text policy, structured-data availability, calendar coverage, and missingness before comparing. Report separately for both cohorts:

- same-visit exact, ingredient, and category overlap;
- cumulative +/-7/14/30/90-day overlap at each semantic level;
- strict no-overlap proportion and ontology-mapping coverage;
- patient-clustered uncertainty and cohort differences, with coverage/missingness counts.

This is **replication of model-derived descriptive patterns at the same institution**, not independent annotation of every replication note and not external-site validation. A qualitative pattern can replicate even when percentages differ; predefine what direction/order of effects matters. Do not simply pool the original 761 patients with the new patients to announce a larger N. If replication is too costly or the new pool is not genuinely unused, omit it and state that limitation.

## Secondary/exploratory: action-stratified and residual analyses

### 4.1 Action-stratified analysis

Action-stratified outputs already exist, but the current labels include `start`, `stop`, `hold`, `changed`, `continue`, `discussed`, and `other_or_unclear`. Do not relabel `changed` as dose change without annotation support, and do not force `discussed` or `other_or_unclear` into a treatment-action category.

After gold-set action validation, report class prevalence, majority-class baseline, per-class precision/recall/F1, macro-F1, and confusion matrix. Then report semantic-temporal correspondence separately for supported action classes:

- start;
- stop;
- hold;
- continue;
- dose change, only where the annotation establishes that `changed` means dose/regimen change.

If a category has insufficient validated cases, mark it underpowered or predeclare a clinically sensible combination; never merge labels after seeing test scores merely to improve F1. If action performance remains weak, narrow the title and claims to medication normalization and cross-source documentation differences. The clinical question is whether narrative notes preferentially encode holds, discontinuations, or dose/regimen changes that structured medication history misses at the same visit.

### 4.2 Residual disagreement review

The existing 100-row no-overlap export and targeted Path B review queue are not yet a validated residual-disagreement study. First count available residual cases in the **independent gold sample** and legacy characterization cohort separately. Review all eligible independent residuals if few; optionally add a stratified legacy review (up to ~250) labeled as exploratory. Sample only after the matching procedure is frozen, and do not call legacy cases independent held-out cases.

Use this taxonomy:

- note normalization failure;
- structured representation or ontology mismatch;
- temporal displacement;
- structured undercapture;
- note extraction/reference error;
- genuine note-only treatment event;
- uncertain.

Only elevate a new residual distribution into the main paper if its sampling and adjudication support inference. Otherwise retain the existing 100-case residual review as clearly labeled exploratory evidence, and place detailed subtypes in the supplement. The previously reported 80% note-side mapping-failure share is **within that reviewed sample**, not a population prevalence estimate. Reviewers must be blinded to the proposed explanation during first-pass coding when feasible.

## Priority 5: patient-level uncertainty

Implement patient-clustered bootstrap confidence intervals, not row-level intervals, for:

- extraction precision, recall, and F1;
- normalization accuracy and paired stage deltas;
- exploratory action metrics where class counts support estimation;
- temporal-window overlap;
- match-validation PPV by bucket;
- observed-versus-permuted differences;
- action-stratified estimates;
- residual taxonomy proportions only if a new probability-sampled review is completed.

Use a fixed bootstrap seed and a documented replicate count, such as 2,000. Report percentage-point deltas with 95% confidence intervals. Avoid filling the manuscript with p-values; paired patient-clustered intervals are more interpretable for this study.

## Optional analyses, only after the core gates pass

1. **Temporal holdout:** develop aliases/rules on earlier dates and evaluate on later dates. The active corpus spans 2017-11-02 to 2023-04-10, so this is feasible, but it should not replace the patient-level test split.
2. **Full-note sensitivity:** after Gate 0 verifies full-text access and defines the primary text view, rerun the alternate view to quantify the effect of the 2,000-character cap. This is more important than adding another generic NLP baseline.
3. **Medication-class analysis:** retain the existing drug-class output as descriptive subgroup analysis; promote it only if the gold set supports clinically interpretable classes and adequate sample sizes.
4. **External cohort:** MIMIC-IV or another institution is not currently present in the workspace and is not a submission gate. Add it only if data access, harmonization, and ethics approvals are already in place without delaying the core validation.
5. **Bounded LLM-bootstrap robustness:** if approved and computationally feasible, run the original Qwen version and one or two alternatives with identical schema on 100--200 independently annotated notes. Compare candidate recall, false positives, identity, and action. This is sensitivity analysis, not a new gold standard or model leaderboard.

## Analyses explicitly not adopted as primary work

- Do not add five more near-duplicate string baselines.
- Do not restart extensive Path B tuning before validating the independent gold set and current Path B precision/abstention behavior.
- Do not use structured medication history as extraction truth.
- Do not treat broader semantic or longer temporal windows as automatically clinically correct.
- Do not claim full-note medication extraction while the active cohort retains a 2,000-character cap.
- Do not report the current BIBM numbers until Gate 0 resolves the denominator and artifact-version discrepancies.

## Target manuscript package

### Main figures

1. Study design and information barriers: BIBM development/resource construction -> pipeline freeze; a separate independent raw-note validation branch -> blinded human gold -> end-to-end evaluation; and the existing cohort -> cross-source characterization. Structured history must not enter note-gold annotation.
2. Reliability cascade: raw notes -> candidate detected -> valid span -> normalized medication identity; action/temporality only as secondary branches.
3. Conditional normalization ablation, explicitly separate from end-to-end performance.
4. Semantic x temporal heatmap, with exact/ingredient/category by same visit and +/-7/14/30/90 days.
5. Observed versus permuted overlap; human-confirmed correspondence can be an overlay or table.

### Main tables

1. Cohort, split, annotation, and inter-rater characteristics.
2. Independent end-to-end extraction and **symmetric comparator** results with patient-clustered confidence intervals.
3. Component ablations and development-only alias contribution.
4. Human validation of relaxed matches; exploratory action-stratified results only if sufficiently supported.

Supplementary material should contain detailed temporal-window values, alias provenance, ontology coverage, annotation schema, prompt/schema details, de-identified residual examples only if release is approved, medication-class breakdowns, MedXN implementation details, and additional sensitivity analyses.

## Manuscript rewrite map

- **Introduction:** retain the BIBM motivation; sharpen the gap as confounding among candidate misses, normalization errors, and genuine cross-source differences.
- **Methods:** reuse cohort, deterministic pipeline, alias, and semantic-ladder descriptions; add the independent gold design, resource chronology, matching contract, temporal windows, null control, review sampling, and clustered bootstrap.
- **Results:** independent annotation -> end-to-end performance -> error cascade -> conditional normalization ablation and symmetric comparators -> large-cohort semantic-temporal characterization -> calibrated relaxed matches. Action and residual analysis follow only if supported.
- **Discussion:** interpret what each EHR component captures, distinguish terminology/timing from undercapture, and state exactly where the method remains uncertain.
- **Limitations:** include LLM-assisted reference construction, annotation scope, action-label difficulty, institution-shaped terminology, structured-record incompleteness, 2,000-character context risk, and lack of cross-institution external validation even if same-institution replication is completed.
- **Conclusion:** avoid claiming that every note/structured mismatch is explained; state the validated fraction and the remaining uncertainty. Retain prior BIBM findings as mechanistic/descriptive results, not substitute gold-test performance.

## JAMIA Open submission requirements and factual gates

Target **Research and Applications** as a new journal submission, not a formal revision of a JAMIA Open manuscript. The [current author instructions](https://academic.oup.com/jamiaopen/pages/general_instructions) specify up to 4,000 main-text words, a structured abstract of up to 250 words (Objectives; Materials and Methods; Results; Discussion; Conclusion), up to four tables and six figures. Prepare a title page with authors, affiliations, corresponding-author information, keywords, and word count; figure alt text; data availability; funding/conflict declarations; and a concise lay summary (prepare one even if the submission portal labels it optional until revision). Recheck the live instructions at submission.

The study uses clinical records. Obtain the **actual** Vanderbilt/VUMC IRB or non-human-subjects determination and verify whether consent was obtained, waived, or not applicable before writing the ethics statement. This is a submission gate, not a wording exercise. Describe the restricted data and access conditions accurately; publish code or aggregate artifacts only after PHI/security review. Disclose the LLM's role in reference construction and any manuscript/analysis AI use per journal policy. In the cover letter, identify the BIBM predecessor/preprint and explain substantive new validation; prior reviews may be supplied to editors only where appropriate. Do not imply BIBM acceptance or that the journal submission is a transfer.

## Immediate next steps (start here)

| Order | Action | Concrete output | Pass criterion / decision |
|---|---|---|---|
| 1 | Check governance and source-note access with the data owner/PI; confirm IRB/waiver/non-human-subjects facts from records. | Documented authority and text-access decision kept in restricted project records. | No new clinical-note annotation or public release before authorization. |
| 2 | **In progress:** Stage 1 artifact and Stage 2 exact-ID exposure audits are complete. Investigators use the [PHI-free batch attestation template](investigator_exposure_attestation_template.md) to resolve manual alias provenance, packet viewing, off-file human inspection, and development use; the analysis team maps verified batches and restricted-ID exceptions to A/B/C/D/Unknown. Governance and full-note access require separate PI/data-owner confirmation. | `resources/doc/validation_sampling_frame.md` has aggregate results; a provisional ID-level matrix is in restricted results, not the public repo. | At least 75--100 confirmed A/B (uninspected and development-independent) patients with 300--500 usable raw notes; otherwise request a new approved slice. Current gate: **not passed**. |
| 3 | Audit existing numbers/resources and prepare `resources/doc/metric_ledger.md` and `resources/doc/resource_chronology.md`; reconcile the [legacy task-scope audit](legacy_task_scope_audit.md) with the exact historical run configuration. | Source/denominator ledger, dated provenance, and versioned task scope. | Every reused BIBM number traceable; no new-test patient contributed to a primary alias/rule. |
| 4 | Use 20--30 notes from a separate, excluded pilot reserve (or old development patients) to test annotation time, mention density, label clarity, and approximate gold-mention yield/primary F1 uncertainty. Choose the two-stage patient-first design and per-patient cap; **then freeze** code, vocabulary hashes, annotation guide, sampling strategy, and `doc/gold_evaluation_contract.md`. | Pilot-only feasibility summary, final guide, note/patient sampling seed and probabilities, signed-off evaluator/matching contract, locked run ID. | Pilot patients excluded from final gold and replication; no pilot estimate reported as test performance; no OPEN contract fields at test draw. |
| 5 | Draw the probability sample with a prespecified seed; save the ID-level sample manifest and its hash in restricted storage **before annotation**. Assign notes to annotators and adjudicate the 20--30% double-coded subset; keep an optional action challenge set separate. | Restricted immutable sample manifest, annotated gold dataset, agreement report, versioned adjudication log; public-facing aggregate flow counts only. | Primary sample comes from the raw-note universe, including no-candidate notes, with no model/structured-EHR cues shown to annotators. |
| 6 | Run all locked systems on exactly those notes; calculate candidate recall, primary end-to-end metrics, symmetric comparator results, and clustered CIs. | Reproducible evaluation bundle, error cascade, and go/no-go interpretation decision. | Substantial candidate misses trigger claim revision or a new independently tested pipeline, not immediate cohort expansion. |
| 7 | Complete semantic-temporal sensitivity, null control, and relaxed-match review on the BIBM characterization cohort. If the gold analysis supports inference and sufficient untouched patients remain, run the **same frozen procedure separately** for within-institution replication. | Side-by-side cohort-specific estimates with common denominators and uncertainty; or a documented reason replication was not feasible. | No pooled headline N; note-source accuracy in the replication cohort is not assumed from an unannotated rerun. |

The first practical task is **step 2 after step 1 is confirmed**. Do not schedule annotators, rewrite results around the old held-out set, or process the remaining pool for replication until the new-patient frame and governance are verified.

For the prospective clinician collaborator, use the PHI-free [clinician review handoff](clinician_review_handoff.md), [fillable response template](clinician_review_response_template.md), [provisional raw-note guide](raw_note_annotation_guide_draft.md), and [legacy task-scope audit](legacy_task_scope_audit.md) now. The [received methods-review status note](methods_review_status.md) is a decision aid, **not a claim that a clinician or a human annotator reviewed notes**. Obtain an identifiable qualified clinician's review/sign-off of the guide and a second authorized annotator before protocol freeze. Real-note pilot materials are prepared only after institutional access and exposure classification pass.

### Experiment tiers and scope control

- **Tier 1 (submission-critical validity / RQ1):** approved genuinely unused-patient raw-note sample; blinded and partly double-coded annotation; frozen pipeline, sample manifest, and evaluation contract; candidate recall; primary end-to-end normalized P/R/F1 with design-aware patient-clustered 95% CIs; fair same-gold comparators; annotation agreement. If the preferred comparator cannot produce medication spans/identities on this corpus, document the incompatibility and compare only the shared supported task rather than inventing a denominator.
- **Tier 2 (RQ2/RQ3 mechanism and cross-source calibration):** reuse the BIBM cohort and existing normalization ablation; semantic x temporal matrix; selected category-only/temporal-match human review; permuted-history negative control; explicit resource chronology. Action metrics are reported transparently but remain secondary. The full new residual taxonomy review is optional unless residual mechanisms become a main claim. Separate remaining-patient replication is valuable **but not submission-critical**, even if the pool is eligible; do it only if it tests stability of cross-source distributions at acceptable cost after RQ1.
- **Tier 3 (only if time permits):** extra LLMs, external institutions/datasets, more NLP systems, sophisticated action modeling, or expanded medication attributes. None is a prerequisite for this paper's central contribution.

## Work sequence and deliverables

### Phase A: reproducibility freeze

- canonical run manifest and metric ledger;
- reconciled denominator and manuscript-number table;
- BIBM rerun into a revision output root only if needed to resolve a specific ledger gap;
- provenance report for aliases, OMOP/RxNorm, structured data, and text versions.

### Phase B: annotation and schema

- independent gold-set sampling frame with patient-first draw, capped notes per patient, and inclusion probabilities;
- 20--30-note feasibility pilot from excluded reserve patients or old development patients;
- clinician-reviewed raw-note annotation guide and template, with orthogonal identity/temporality/action/assertion fields;
- pilot-informed guide, full-note primary input decision, `doc/gold_evaluation_contract.md`, and code/resource freeze before gold sampling/scoring;
- adjudicated 300--500-note primary gold set from ~75--100 eligible unused patients, plus a separate challenge set only if needed;
- agreement and error-cascade evaluator.

### Phase C: computational validation

- dev-only alias comparison;
- 7/14/30/90-day semantic x temporal sensitivity;
- fixed-seed patient-mismatched negative control (target 1,000 permutations);
- patient-clustered bootstrap module;
- full-note sensitivity diagnostic.

### Phase D: reconciliation review

- approximately 200+ targeted category-only and temporal-only match reviews, plus control buckets;
- optional new residual review sized after available gold and legacy pools are counted (250 is aspirational, not an automatic quota);
- exploratory action-stratified analysis with an explicit reliability decision;
- final tables and figures.

### Conditional Phase D2: remaining-patient replication

- after the gold validity decision, identify untouched patients remaining after pilot and validation exclusions;
- rerun the frozen pipeline and semantic-temporal procedure on this separate same-institution cohort, if affordable and authorized;
- compare cohort-specific estimates using matched denominators, timing rules, note-context definitions, and patient-clustered intervals;
- do not merge the cohorts merely to increase N, and label any pipeline revisions after gold inspection exploratory until independently revalidated.

### Phase E: manuscript and submission package

- rewrite around RQ1--RQ3;
- move detailed artifacts to supplement;
- verify all claims against the metric ledger;
- complete JAMIA Open reporting, data governance, AI-use, IRB/consent, and reproducibility statements based on the factual study record; **do not invent an IRB determination, exemption, or consent waiver**;
- coauthor review and final submission audit.

## Proposed schedule

Use dependency gates rather than a fixed submission date:

- **Week 1:** governance/text-access check, new-patient-frame audit, metric ledger, and resource chronology; no automatic BIBM cohort expansion.
- **Weeks 2--3:** excluded-reserve/development-note annotation pilot, protocol and pipeline freeze, draw primary sample, begin independent coding.
- **Weeks 3--5:** adjudication, end-to-end and symmetric comparator evaluation, clustered intervals.
- **Weeks 5--6:** semantic x temporal matrix, permutation control, manual relaxed-match review; secondary action analysis only where labels support it.
- **Weeks 6--7, conditional:** separate within-institution replication on remaining eligible patients if gold validity and resources support it.
- **Weeks 7--9:** results-driven manuscript rewrite, supplement, coauthor review, and journal-policy audit.

These are planning estimates; annotation capacity, data access, and IRB facts control the actual dates. No submission date is promised until the primary gold analysis passes its gates.

## Stop rule

Stop adding experiments after the following core components are complete and internally consistent:

1. independent gold set;
2. leakage-safe independent-sample normalization and symmetric comparators;
3. temporal-window sensitivity;
4. human validation of semantic-temporal matches;
5. patient-level confidence intervals and negative control;
6. transparent action performance/uncertainty assessment, without requiring high action F1 or a new action model.

At that point, decide whether the untouched-patient replication adds a distinct scientific check at acceptable cost. If not, improve clarity, limitations, and reproducibility rather than adding more patients, models, or baselines.
