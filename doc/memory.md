# Project Memory: Clinic-Note Medication Evidence / BIBM 2026

## Current Paper Identity

This project is no longer being framed as a medication NER benchmark or a SOTA extraction paper.

The current target paper identity is:

- a note-grounded clinical informatics / evidence-characterization study
- focused on treatment-context medication evidence in clinic notes
- using structured EHR medication fields as secondary semantic concordance comparators, not ground truth

The central claim we are working toward is:

Clinic notes contain heterogeneous treatment-context medication evidence that can be normalized, audited, and characterized relative to structured medication history. A note-grounded, auditable framework helps distinguish apparent note-vs-EHR mismatch caused by normalization limitations from residual mismatch that may reflect real documentation differences.

## What Has Been Completed

### 1. Note-grounded reference layer

We already have a clinic-note-only, note-grounded reference layer built from note content.

Core artifacts:
- `episode_extraction_results/clinic_like_20k_30k/rq1/adjudicated/...`
- `episode_extraction_results/clinic_like_20k_30k/rq1/adjudication_packets/...`

This is the reference target for the project. It should not be described as an exhaustive manual gold standard.

### 2. Human reliability calibration

Two audit layers now exist:

#### Targeted difficult-review audit

This audit is explicitly error-enriched and should not be interpreted as global bootstrap accuracy.

Key numbers:
- audited rows: `3,188 / 27,752 = 11.49%`
- targeted queue size: `3,388`
- dropped rows: `200`
- pre-correction LLM canonical accuracy on the targeted slice: `40 / 3,188 = 0.0125`
- Wilson 95% CI: `0.0092 to 0.0170`

Interpretation:
- this slice is enriched for difficult Path B / abstention / ambiguous cases
- it is useful for failure analysis, not global quality estimation

#### Random audit of previously unaudited rows

This is the defensible reliability estimate for the unaudited reference pool.

Key numbers:
- reviewed `n = 300`
- canonical correctness: `182 / 300 = 0.6067`
- Wilson 95% CI: `0.5504 to 0.6603`
- weighted conservative accuracy: `0.6447`
- weighted lenient accuracy: `0.6506`

Main error categories:
- `non_medication_or_lab_substance = 102`
- `action_incorrect = 57`
- `wrong_canonical_alias = 16`
- `span_problem = 1`

Important caution:
- perfect agreement / kappa are currently reported in the completed audit file, but we should only emphasize them if that coding was truly independent rather than consensus-style

### 3. Deterministic normalization ladder

This remains the main technical core of the paper.

Current Path A v1 ladder:
- surface-exact baseline accuracy = `0.7214`
- lexical cleanup accuracy = `0.7811`
- curated alias map accuracy = `0.8458`
- full Path A accuracy = `0.8458`

Interpretation:
- lexical cleanup and curated aliasing drive the measurable gains
- safe decomposition does not add measurable gain in the frozen cohort

Residual Path A errors:
- unresolved rows after full Path A: `4,280`
- residual failures are dominated by missing aliases
- unresolved terms are concentrated rather than diffuse

This should be framed as an internal, auditable component ablation over a fixed note-grounded denominator, not as a leaderboard.

### 4. Same-visit semantic mismatch ladder

We implemented a same-visit semantic mismatch ladder and then extended it to a semantic + temporal ladder.

Earlier same-visit-only 5-bucket interpretation:
- exact label overlap
- exact mismatch but ingredient overlap
- ingredient mismatch but category overlap
- category overlap only
- no category overlap

This was useful, but it is no longer the main novelty result.

### 5. Semantic + temporal mismatch ladder

We implemented a stricter internal semantic-temporal hierarchy and a 7-bucket paper-facing collapsed ladder.

Internal order:
1. same-visit exact
2. same-visit ingredient
3. same-visit category
4. +/-30d exact
5. +/-30d ingredient
6. +/-30d category
7. +/-90d exact
8. +/-90d ingredient
9. +/-90d category
10. any-history exact
11. any-history ingredient
12. any-history category
13. no structured overlap

Paper-facing collapsed ladder:
1. same-visit exact
2. same-visit ingredient only
3. same-visit category only
4. any +/-30d overlap after failing same visit
5. any +/-90d overlap after failing +/-30
6. any-history overlap after failing +/-90
7. no structured overlap anywhere

The broad project-specific temporal ladder showed:
- same-visit exact: `4,563 / 27,752 = 16.44%`
- same-visit ingredient only: `16 / 27,752 = 0.06%`
- same-visit category only: `10,732 / 27,752 = 38.67%`
- +/-30d overlap: `9,761 / 27,752 = 35.17%`
- +/-90d overlap: `671 / 27,752 = 2.42%`
- any-history overlap: `907 / 27,752 = 3.27%`
- no structured overlap: `1,102 / 27,752 = 3.97%`

Interpretation:
- exact overlap is low
- broader semantic and near-term temporal relationships are much more common
- this supports “related but non-interchangeable” better than a simple mismatch story

### 6. OMOP / RxNorm-backed mapping layer

We added OMOP vocabulary support from `resources/raw/OMOP_related`.

Structured side:
- maps well into OMOP/RxNorm
- unique `drug_concept_id`s: `5,172`
- mapped to standard concepts: `5,149` (`99.56%`)
- mapped structured rows: `92.52%`
- ingredient-covered structured rows: `92.41%`
- category-covered structured rows: `91.33%`

Note side:
- much harder to standardize
- unique note canonical labels: `2,258`
- mapped note labels: `677` (`29.98%`)
- ingredient-covered note labels: `664` (`29.41%`)
- category-covered note labels: `656` (`29.05%`)

Interpretation:
- OMOP/RxNorm is strong enough for the structured side
- note-side heterogeneity is the major standardization bottleneck
- this means the OMOP-backed layer should be treated as a high-confidence sensitivity analysis, not as the only main analysis

### 7. OMOP-backed temporal sensitivity

Using OMOP-backed note/structured mappings:

All-row collapsed result:
- same-visit exact: `299` (`1.08%`)
- same-visit ingredient only: `5,668` (`20.42%`)
- same-visit category only: `1,058` (`3.81%`)
- +/-30d overlap: `5,727` (`20.64%`)
- +/-90d overlap: `861` (`3.10%`)
- any-history overlap: `1,507` (`5.43%`)
- no structured overlap: `12,632` (`45.52%`)

Mapped-note sensitivity:
- denominator: `17,219`
- no structured overlap: `12.19%`

Ingredient-or-category-covered sensitivity:
- denominator: `16,988`
- no structured overlap: `11.54%`

Interpretation:
- the raw OMOP all-row result overstates residual mismatch because many note labels are unmapped
- after restricting to note rows with semantic coverage, the final no-overlap bucket drops a lot
- this makes the mismatch story more credible but also highlights note-side mapping limitations

### 8. Manual review of the strict OMOP no-overlap bucket

We manually reviewed 100 examples from the final OMOP-backed `no_structured_overlap` bucket.

Key result:
- `80%` note-side mapping failure
- `7%` real note-only evidence
- `6%` structured-side undercapture
- `7%` extraction/reference noise

Interpretation:
- the final strict no-overlap bucket is mostly not pure note-only evidence
- the dominant issue is note-side mapping failure
- this does not kill the paper; it changes the safest claim

The strongest current interpretation is:
- the framework is useful for decomposing mismatch into normalization failure vs plausible residual signal
- not for claiming that notes prove the EHR is broadly missing medications

### 9. Path A v1 vs v2 post-hoc normalization sensitivity

We implemented a bounded post-hoc `Path A v1 vs v2` note-side normalization sensitivity.

Important framing:
- this is not a new benchmark
- this is not a new end-to-end model evaluation
- this is a transparent note-side normalization refinement sensitivity

Artifacts:
- `resources/manual/pathA_v2_alias_supplement.csv`
- `resources/script/run_rq1_patha_v1_v2_sensitivity.py`
- `episode_extraction_results/clinic_like_20k_30k/rq1/patha_v1_v2_sensitivity/...`

Main results:
- Path A v1 changed rows at this note-label / OMOP mapping layer: `0`
- Path A v2 added `54` bounded aliases to the v1 alias set
- Path A v2 changed `2,344` rows across `51` reviewed label types

Coverage improvements:
- mapped row rate: `0.6205 -> 0.7029`
- ingredient-covered row rate: `0.6184 -> 0.7009`
- category-covered row rate: `0.6134 -> 0.6959`

Temporal ladder improvement:
- no structured overlap rows: `12,632 -> 10,529`
- no structured overlap rate: `0.4552 -> 0.3794`
- same-visit exact rate: `0.0108 -> 0.0175`

Manual-review calibration of v2:
- among reviewed `note-side mapping failure` rows, `58 / 80 = 72.5%` were resolved by v2

What remains unresolved after v2:
- regimens like `folfox`, `folfiri`
- combination products
- product/prep labels
- some generics still absent from the current note-side OMOP mapping layer

Interpretation:
- this strongly supports that note-side normalization coverage is a major driver of apparent mismatch
- it also shows that deterministic, transparent alias refinement materially changes the mismatch interpretation
- this is a good supporting sensitivity result for the paper

## What The Paper Can Safely Claim Now

Safe claims:
- clinic notes and structured EHR medication fields are related but non-interchangeable
- exact same-visit overlap is low, but semantic and temporal relatedness is much higher
- a large share of strict residual mismatch is driven by note-side normalization coverage failure
- deterministic normalization plus targeted audit helps separate normalization artifact from residual candidate signal
- the project is an evidence-characterization framework, not a benchmark

Unsafe or overstated claims:
- “structured EHR is wrong” or “missing many medications”
- “no-overlap rows are true undocumented medication use”
- “the reference is a full manual gold standard”
- “RxNorm fully standardized the note side”
- “this is a SOTA medication extraction result”

## Current Best BIBM Story

The strongest BIBM framing now is:

1. note-grounded reference and audit framework
2. calibrated human reliability, including random audit
3. auditable deterministic normalization ladder
4. semantic + temporal note-to-EHR mismatch characterization
5. manual mismatch decomposition showing that much residual mismatch is due to note-side normalization limits
6. bounded Path A refinement sensitivity showing that deterministic alias expansion can materially reduce apparent mismatch

This is stronger than a plain extraction paper because it contributes:
- methodology for note-grounded evaluation
- calibration of reference reliability
- transparent normalization analysis
- cross-source evidence characterization
- decomposition of mismatch into normalization artifact vs residual candidate signal

## What Should Be the Next Step

### Highest-value next manuscript step

Update the BIBM manuscript to explicitly integrate:
- OMOP/RxNorm coverage table
- manual review of the final no-overlap bucket
- Path A v1 vs v2 bounded sensitivity

The manuscript should say clearly:
- the no-overlap bucket is mostly note-side mapping failure in the current pipeline
- Path A refinement reduces that mismatch materially
- therefore the paper’s contribution is evidence characterization plus mismatch decomposition, not definitive clinical discordance discovery

### Highest-value next analysis step if we want more clinical meaning

Manually review or stratify the residual post-v2 no-overlap subset.

The best target is:
- the post-v2 residual `no_structured_overlap` rows
- ideally a focused review of clinically important classes or action cues

Reason:
- this will tell us whether the residual signal after deterministic refinement is actually clinically interesting
- this is the best route to a stronger claim about note-only treatment-context evidence

### Highest-value next normalization step

Build a conservative `Path A v3` idea bank, but do not make it the center of the current paper.

Most promising areas:
- oncology regimen normalization
- combination product normalization
- common product/prep labels
- supplements / OTC forms where clinically relevant

Important caution:
- any v3 should be post-hoc or future work unless evaluated carefully on a held-out subset

## What Should Probably Appear in the 8-Page BIBM Paper

Best main-paper tables:
- cohort / reference / audit summary
- random audit reliability table
- deterministic normalization ladder
- semantic + temporal mismatch ladder
- OMOP/RxNorm mapping coverage table
- compact Path A v1 vs v2 sensitivity table

Best main-paper figures:
- workflow figure
- normalization ladder figure
- semantic + temporal mismatch ladder figure

Best compact supporting paragraph:
- manual review of final no-overlap bucket
- Path A v1 vs v2 sensitivity

If space is too tight:
- keep Path A v1 vs v2 as one compact table or one dense paragraph
- keep the mismatch manual review as a compact result paragraph rather than a large table

## Bottom Line

We are in a much stronger place than before:

- the project is no longer just “low overlap”
- we now know why the strongest residual mismatch bucket looks the way it does
- we have evidence that note-side normalization is the main bottleneck
- we have shown that transparent deterministic refinement can reduce apparent mismatch substantially

The paper should now be positioned as:

A note-grounded clinical informatics framework for auditing, normalizing, and characterizing treatment-context medication evidence in clinic notes, with structured medication history used as a secondary comparator. The main contribution is not proving large medication discordance, but making the sources, limits, and meaning of apparent mismatch measurable and clinically interpretable.

## 2026-06-15 Feedback Triage and To-Do

New feedback to evaluate before the next manuscript revision:

### 1. Too many figures

Assessment:
- valid concern
- the current BIBM manuscript likely has more figures than necessary for an 8-page evidence-characterization paper
- highest-retention figures are:
  - workflow figure
  - normalization ladder figure
  - semantic + temporal mismatch ladder figure
- lower-priority figures are:
  - OMOP no-overlap sensitivity bar
  - manual strict no-overlap review bar
  - Path A v2 dumbbell
  - residual failure figure

Recommended action:
- collapse some figure content into compact tables or prose
- likely remove at least 1 to 2 of the smaller support figures and keep their main numbers in tables/text
- preserve only visuals that directly carry the main paper claim

### 2. Remove Path B language and replace `clinic_like_20k_30k`

Assessment:
- should adapt
- current manuscript still contains Path B wording in Methods and audit description
- current manuscript still contains the internal run name `clinic_like_20k_30k` in Results

Recommended action:
- delete Path B discussion from the main paper unless absolutely required for historical explanation
- replace with neutral wording:
  - `frozen cohort run`
  - `error-enriched difficult-review queue`
  - `abstention/ambiguity review queue` only if needed
- remove literal `clinic_like_20k_30k` from text, tables, captions, and file-facing prose

### 3. Clarify what “accuracy” means

Assessment:
- the current use of `accuracy` is defensible for mention-level closed-set canonical label assignment
- but it is easy for readers to confuse with extraction precision/recall
- replacing everything with precision/recall would be misleading unless we fully pivot back to a detection benchmark

Recommended action:
- do not replace the main Path A metric with precision/recall
- instead rename more explicitly in prose and captions:
  - `mention-level canonical mapping accuracy`
  - or `exact canonical label agreement rate`
- keep the equation defining the metric
- if needed, add one sentence:
  - this is a closed-set label-assignment endpoint over reference mention rows, not a span-detection precision/recall metric

### 4. Meaning of `0.6067`

Assessment:
- the number is meaningful, but only if framed correctly
- `0.6067 = 182 / 300` refers to random-audit canonical correctness on previously unaudited reference rows
- it is not end-to-end pipeline accuracy and not Path A normalization accuracy
- it is best understood as a conservative estimate of note-grounded reference reliability in the unaudited pool

Recommended action:
- replace bare `accuracy 0.6067` wording with:
  - `random-audit canonical correctness rate`
  - or `reference-label correctness rate in the random unaudited audit sample`
- explicitly state denominator and object of judgment:
  - 300 previously unaudited reference rows
  - judged against adjudicated canonical correctness
- keep the Wilson CI

### 5. Reframe ablation as baseline + component rather than incremental ladder

Assessment:
- worth considering, but not obviously better as the main table
- the current sequential ladder matches the operational pipeline
- a baseline + one-component-at-a-time table would be more classic as an ablation, but it answers a slightly different question

Recommended action:
- keep the sequential ladder as the main operational result
- optionally add a secondary component-isolation sensitivity if easy to compute:
  - baseline only
  - baseline + lexical cleanup
  - baseline + alias map
  - baseline + safe decomposition
- if implemented, present it as a supplemental interpretability view, not a replacement for the staged ladder

### 6. Public replication on MIMIC-IV

Assessment:
- possible only as a partial external replication
- not a direct match to the current paper goal
- MIMIC-IV-Note is hospital-note oriented rather than clinic-note oriented, with public note coverage centered on hospital-wide notes such as discharge summaries and radiology reports
- MIMIC-IV structured medication tables do provide medication timing fields, so note-to-structured comparison is feasible
- there is also a public PhysioNet medication-label subset for MIMIC-IV discharge summaries, which could support a smaller reproducibility experiment

Recommended action:
- do not make MIMIC-IV replication a prerequisite for the current BIBM paper unless time is ample
- if pursued, frame it as:
  - external portability / reproducibility sensitivity
  - on hospital-note medication normalization and note-to-structured comparison
- do not claim it is the same clinic-note treatment-context task

### 7. `kappa = 1` problem

Assessment:
- this is a real issue
- current completed random-audit CSV shows reviewer 1 and reviewer 2 fields are identical across the 300 rows for the main judgment fields
- therefore the current `kappa = 1.00` should not be presented as independent inter-rater reliability unless we can document that two truly independent coders produced those identical labels

Relevant files:
- annotation CSV:
  - `episode_extraction_results/clinic_like_20k_30k/rq1/reference_random_audit_sample_bibm_test/completed_manual_annotation_random_audit_reviewer2_completed.csv`
- analysis script:
  - `resources/script/run_rq1_reference_random_audit_results.py`
- current outputs:
  - `episode_extraction_results/clinic_like_20k_30k/rq1/reference_random_audit_results_bibm_test/...`

Recommended action:
- remove or downplay `kappa = 1.00` from the paper unless independence can be proven
- safer manuscript wording:
  - `double-coded fields were identical in the completed file, but independent coding provenance should be interpreted cautiously`
  - or omit kappa entirely and report adjudicated audit only
- best fix:
  - run an actually independent second human review on a subset
- do not replace human inter-rater reliability with two external LLM judges
  - that would not measure human reliability
  - it would introduce privacy / governance / comparability concerns
  - it could be a separate AI-adjudication sensitivity study, but not a substitute for kappa

## Immediate To-Do List

1. Revise the BIBM manuscript to remove Path B wording and internal run-name wording.
2. Rename `accuracy` more explicitly in the paper where needed:
   - `mention-level canonical mapping accuracy`
   - `random-audit canonical correctness rate`
3. Remove or neutralize the `kappa = 1.00` claim unless independence of reviewer 2 can be documented.
4. Reduce figure count by converting at least one support figure into a compact table or prose summary.
5. Decide whether to add a secondary baseline-plus-component ablation table; do not replace the sequential ladder unless the alternate view is clearly better.
6. Treat MIMIC-IV replication as optional future portability work unless a narrowly scoped public sensitivity can be run quickly and honestly.
